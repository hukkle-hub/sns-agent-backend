"""Supabase 접근 레이어.

`supabase` 는 지연 초기화 프록시다. 모듈 import 시점에 create_client 를 부르면
환경변수가 하나라도 비었을 때 앱 전체가 뜨지 않으므로, 실제로 쓸 때 연결한다.
사용법은 평범한 클라이언트와 같다: supabase.table("...").select("*").execute()
"""

import os
from typing import Optional

from supabase import Client, create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
TABLE = os.environ.get("SUPABASE_TABLE", "content_generations")
VOICE_TABLE = "brand_voice"

_client: Optional[Client] = None


def is_configured() -> bool:
    return bool(SUPABASE_URL and SUPABASE_KEY)


def get_client() -> Client:
    global _client
    if not is_configured():
        raise RuntimeError(
            "SUPABASE_URL / SUPABASE_KEY 환경변수가 없습니다. "
            "Render 대시보드 → Environment 를 확인하세요."
        )
    if _client is None:
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _client


class _LazyClient:
    """supabase.table(...) 형태를 그대로 쓰되 연결만 늦춘다."""

    def __getattr__(self, name):
        return getattr(get_client(), name)


supabase = _LazyClient()


# ------------------------------------------------------------------ 헬퍼

def save_generated_content(
    topic: str,
    draft: str,
    score: Optional[float] = None,
    revision_count: int = 0,
    approval_status: str = "pending",
) -> str:
    """생성 결과를 저장하고 generation_id 를 돌려준다."""
    res = (
        get_client()
        .table(TABLE)
        .insert(
            {
                "topic": topic,
                "ai_raw_output": draft,
                "ai_self_score": score,
                "revision_count": revision_count,
                "approval_status": approval_status,
            }
        )
        .execute()
    )
    if not res.data:
        raise RuntimeError("insert 응답이 비었습니다. RLS 정책 또는 키 종류를 확인하세요.")
    return res.data[0]["generation_id"]


def save_channel_outputs(generation_id: str, channels: dict, cards: list) -> None:
    get_client().table(TABLE).update(
        {"channel_outputs": channels or None, "cards": cards or None}
    ).eq("generation_id", generation_id).execute()


def schedule_publish(generation_id: str, when_iso: str, channel: str = "") -> None:
    """발행 예약. 실제 발사는 항상 깨어 있는 Node 백엔드가 담당한다."""
    get_client().table(TABLE).update(
        {
            "scheduled_at": when_iso,
            "publish_status": "scheduled",
            "target_channel": channel or None,
        }
    ).eq("generation_id", generation_id).execute()


def list_scheduled(limit: int = 50) -> list[dict]:
    if not is_configured():
        return []
    try:
        res = (
            get_client()
            .table(TABLE)
            .select("generation_id, topic, scheduled_at, publish_status, target_channel, published_at, publish_error")
            .neq("publish_status", "none")
            .order("scheduled_at", desc=False)
            .limit(limit)
            .execute()
        )
        return res.data or []
    except Exception:
        return []


def cancel_schedule(generation_id: str) -> None:
    get_client().table(TABLE).update(
        {"publish_status": "none", "scheduled_at": None}
    ).eq("generation_id", generation_id).execute()


def update_human_approval(
    generation_id: str,
    final_draft: str,
    status: str = "approved",
    original: str = "",
) -> None:
    """수정본 저장 + 수정률(edit_distance) 계산.

    original 을 넘기면 사람이 얼마나 고쳤는지 함께 기록한다. 이 값이 학습의 핵심 신호다.
    """
    patch = {"human_modified_output": final_draft, "approval_status": status}
    if status == "approved":
        patch["approved_at"] = "now()"
        if original:
            patch["edit_distance"] = edit_distance_ratio(original, final_draft)
    get_client().table(TABLE).update(patch).eq("generation_id", generation_id).execute()


# ------------------------------------------------------------------ 브랜드 보이스

def get_brand_voice() -> str:
    """활성 브랜드 보이스 문서를 반환. 없으면 빈 문자열."""
    if not is_configured():
        return ""
    try:
        res = (
            get_client()
            .table(VOICE_TABLE)
            .select("content")
            .eq("is_active", True)
            .limit(1)
            .execute()
        )
        return res.data[0]["content"] if res.data else ""
    except Exception:
        return ""


def save_brand_voice(content: str, note: str = "") -> None:
    """새 버전을 활성화하고 기존 활성본은 이력으로 내린다."""
    c = get_client()
    c.table(VOICE_TABLE).update({"is_active": False}).eq("is_active", True).execute()
    c.table(VOICE_TABLE).insert(
        {"content": content, "is_active": True, "note": note}
    ).execute()


def list_brand_voice_history(limit: int = 10) -> list[dict]:
    if not is_configured():
        return []
    try:
        res = (
            get_client()
            .table(VOICE_TABLE)
            .select("voice_id, created_at, is_active, note")
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        return res.data or []
    except Exception:
        return []


# ------------------------------------------------------------------ 학습 루프 (1단계)

RULES_TABLE = "learned_rules"
AUTO_ACTIVATE_HITS = 3   # 이 횟수 이상 관찰된 규칙만 자동 적용


def edit_distance_ratio(original: str, edited: str) -> float:
    """사람이 고친 비율 0~1. 0이면 그대로 승인, 1이면 전면 재작성."""
    from difflib import SequenceMatcher

    a, b = (original or "").strip(), (edited or "").strip()
    if not a and not b:
        return 0.0
    if not a or not b:
        return 1.0
    return round(1.0 - SequenceMatcher(None, a, b).ratio(), 4)


def get_active_rules(limit: int = 15) -> list[str]:
    """생성 프롬프트에 주입할 활성 규칙. 관찰 횟수가 많은 순."""
    if not is_configured():
        return []
    try:
        res = (
            get_client()
            .table(RULES_TABLE)
            .select("rule_text")
            .eq("is_active", True)
            .order("hit_count", desc=True)
            .limit(limit)
            .execute()
        )
        return [r["rule_text"] for r in (res.data or [])]
    except Exception:
        return []


def list_rules(limit: int = 60) -> list[dict]:
    if not is_configured():
        return []
    try:
        res = (
            get_client()
            .table(RULES_TABLE)
            .select("*")
            .order("hit_count", desc=True)
            .limit(limit)
            .execute()
        )
        return res.data or []
    except Exception:
        return []


def set_rule_active(rule_id: str, active: bool) -> None:
    get_client().table(RULES_TABLE).update({"is_active": active}).eq(
        "rule_id", rule_id
    ).execute()


def delete_rule(rule_id: str) -> None:
    get_client().table(RULES_TABLE).delete().eq("rule_id", rule_id).execute()


def upsert_rule(rule_text: str, category: str, evidence_ids: list) -> str:
    """같은 취지의 규칙이 이미 있으면 hit_count 를 올리고, 없으면 새로 만든다.

    문자열 완전 일치가 아니라 앞부분 기준으로 느슨하게 대조한다.
    """
    c = get_client()
    existing = c.table(RULES_TABLE).select("rule_id, rule_text, hit_count, evidence").execute().data or []

    key = rule_text.strip()[:24]
    for r in existing:
        if r["rule_text"].strip()[:24] == key:
            hits = (r.get("hit_count") or 1) + 1
            ev = (r.get("evidence") or []) + evidence_ids
            c.table(RULES_TABLE).update(
                {
                    "hit_count": hits,
                    "evidence": ev[-30:],
                    "is_active": hits >= AUTO_ACTIVATE_HITS,
                    "updated_at": "now()",
                }
            ).eq("rule_id", r["rule_id"]).execute()
            return r["rule_id"]

    res = c.table(RULES_TABLE).insert(
        {
            "rule_text": rule_text,
            "category": category,
            "evidence": evidence_ids,
            "hit_count": 1,
            "is_active": False,
        }
    ).execute()
    return res.data[0]["rule_id"] if res.data else ""


def list_unmined_edits(limit: int = 20) -> list[dict]:
    """분석 대기 중인 승인 건 (실제로 수정이 있었던 것만)."""
    if not is_configured():
        return []
    try:
        res = (
            get_client()
            .table(TABLE)
            .select("generation_id, topic, ai_raw_output, human_modified_output, edit_distance")
            .eq("approval_status", "approved")
            .eq("mined", False)
            .not_.is_("human_modified_output", "null")
            .limit(limit)
            .execute()
        )
        return [r for r in (res.data or []) if (r.get("edit_distance") or 0) > 0.02]
    except Exception:
        return []


def mark_mined(ids: list) -> None:
    for i in ids:
        get_client().table(TABLE).update({"mined": True}).eq("generation_id", i).execute()


def edit_distance_series(limit: int = 60) -> list[dict]:
    """수정률 시계열. 이 값이 내려가야 학습이 되고 있는 것."""
    if not is_configured():
        return []
    try:
        res = (
            get_client()
            .table(TABLE)
            .select("approved_at, edit_distance, topic")
            .eq("approval_status", "approved")
            .not_.is_("edit_distance", "null")
            .order("approved_at", desc=False)
            .limit(limit)
            .execute()
        )
        return res.data or []
    except Exception:
        return []


# ------------------------------------------------------------------ 대결 평가

DUELS_TABLE = "duels"


def create_duel(
    topic: str,
    ours_text: str,
    ref_text: str,
    ref_source: str = "",
    deliverable: str = "",
    price_standard: float | None = None,
    generation_id: str | None = None,
) -> str:
    """블라인드 대결 생성. 좌우 배치를 무작위로 섞어 저장한다."""
    import random

    res = (
        get_client()
        .table(DUELS_TABLE)
        .insert(
            {
                "topic": topic,
                "ours_text": ours_text,
                "ref_text": ref_text,
                "ref_source": ref_source,
                "deliverable": deliverable,
                "price_standard": price_standard,
                "generation_id": generation_id,
                "ours_is_a": random.choice([True, False]),
            }
        )
        .execute()
    )
    return res.data[0]["duel_id"] if res.data else ""


def list_pending_duels(limit: int = 20) -> list[dict]:
    if not is_configured():
        return []
    try:
        res = (
            get_client()
            .table(DUELS_TABLE)
            .select("*")
            .is_("winner", "null")
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        return res.data or []
    except Exception:
        return []


def decide_duel(
    duel_id: str,
    winner: str,
    would_pay_ours: bool,
    would_pay_ref: bool,
    note: str = "",
    ai_agreed: bool | None = None,
) -> None:
    get_client().table(DUELS_TABLE).update(
        {
            "winner": winner,
            **({"ai_agreed": ai_agreed} if ai_agreed is not None else {}),
            "would_pay_ours": would_pay_ours,
            "would_pay_ref": would_pay_ref,
            "note": note,
            "decided_at": "now()",
        }
    ).eq("duel_id", duel_id).execute()


def duel_stats(limit: int = 200) -> dict:
    """승률과 지불의사율. 절대 기준 판정의 근거."""
    if not is_configured():
        return {}
    try:
        res = (
            get_client()
            .table(DUELS_TABLE)
            .select("winner, would_pay_ours, would_pay_ref, decided_at, topic")
            .not_.is_("winner", "null")
            .order("decided_at", desc=False)
            .limit(limit)
            .execute()
        )
        rows = res.data or []
    except Exception:
        return {}

    n = len(rows)
    if not n:
        return {"n": 0}
    wins = sum(1 for r in rows if r.get("winner") == "ours")
    pay_ours = sum(1 for r in rows if r.get("would_pay_ours"))
    pay_ref = sum(1 for r in rows if r.get("would_pay_ref"))
    recent = rows[-10:]
    recent_wins = sum(1 for r in recent if r.get("winner") == "ours")
    return {
        "n": n,
        "wins": wins,
        "win_rate": wins / n,
        "pay_rate_ours": pay_ours / n,
        "pay_rate_ref": pay_ref / n,
        "recent_n": len(recent),
        "recent_win_rate": recent_wins / len(recent),
        "rows": rows,
    }


# ------------------------------------------------------------------ 검수자 보정

def record_human_score(generation_id: str, human_score: float, ai_creativity: float | None) -> None:
    """사람 채점 저장 + AI 채점과의 편차 계산."""
    patch = {"human_score": human_score}
    if ai_creativity is not None:
        patch["score_gap"] = round(float(ai_creativity) - float(human_score), 2)
    get_client().table(TABLE).update(patch).eq("generation_id", generation_id).execute()


def calibration_stats(limit: int = 100) -> dict:
    """AI 채점이 사람 기준과 얼마나 어긋나는지."""
    if not is_configured():
        return {}
    try:
        res = (
            get_client()
            .table(TABLE)
            .select("topic, approved_at, creativity_score, human_score, score_gap")
            .not_.is_("human_score", "null")
            .order("approved_at", desc=False)
            .limit(limit)
            .execute()
        )
        rows = res.data or []
    except Exception:
        return {}
    if not rows:
        return {"n": 0}

    gaps = [float(r["score_gap"]) for r in rows if r.get("score_gap") is not None]
    if not gaps:
        return {"n": len(rows)}
    recent = gaps[-10:]
    return {
        "n": len(gaps),
        "mean_gap": sum(gaps) / len(gaps),
        "recent_gap": sum(recent) / len(recent),
        "abs_gap": sum(abs(g) for g in gaps) / len(gaps),
        "rows": rows,
    }


def calibration_note() -> str:
    """검수 프롬프트에 넣을 보정 문구. 최근 편차가 크면 스스로 조정하게 한다."""
    s = calibration_stats()
    if not s.get("n") or s["n"] < 3:
        return ""
    g = s["recent_gap"]
    if abs(g) < 5:
        return ""
    if g > 0:
        return (
            f"\\n[채점 보정] 너는 최근 {len(str(''))and ''}평가에서 사람 기준보다 평균 "
            f"{g:.0f}점 후하게 매겼다. 같은 원고를 그만큼 더 엄격하게 보라. "
            "특히 '무난하다'고 느껴지면 50점 이하로 내려라.\\n"
        )
    return (
        f"\\n[채점 보정] 너는 최근 평가에서 사람 기준보다 평균 {abs(g):.0f}점 "
        "낮게 매겼다. 잘된 부분을 놓치고 있지 않은지 다시 보라.\\n"
    )


def list_internal_duel_candidates(limit: int = 20) -> list[dict]:
    """우리끼리 대결에 쓸 수 있는 승인/대기 결과물."""
    if not is_configured():
        return []
    try:
        res = (
            get_client()
            .table(TABLE)
            .select("generation_id, topic, ai_raw_output, human_modified_output, creativity_score")
            .in_("approval_status", ["approved", "pending"])
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        return res.data or []
    except Exception:
        return []


def create_internal_duel(a: dict, b: dict) -> str:
    """우리 결과물 두 개를 붙인다. AI 점수가 높은 쪽을 'ours'로 둔다."""
    import random

    sa = float(a.get("creativity_score") or 0)
    sb = float(b.get("creativity_score") or 0)
    hi, lo = (a, b) if sa >= sb else (b, a)
    hi_s, lo_s = (sa, sb) if sa >= sb else (sb, sa)

    res = (
        get_client()
        .table(DUELS_TABLE)
        .insert(
            {
                "kind": "internal",
                "topic": f"{hi.get('topic','')} ↔ {lo.get('topic','')}",
                "ours_text": hi.get("human_modified_output") or hi.get("ai_raw_output") or "",
                "ref_text": lo.get("human_modified_output") or lo.get("ai_raw_output") or "",
                "ours_ai_score": hi_s,
                "ref_ai_score": lo_s,
                "generation_id": hi.get("generation_id"),
                "ours_is_a": random.choice([True, False]),
                "deliverable": "검수자 검증",
            }
        )
        .execute()
    )
    return res.data[0]["duel_id"] if res.data else ""


def critic_agreement() -> dict:
    """우리끼리 대결에서 AI 판단과 사람 판단이 얼마나 일치했나."""
    if not is_configured():
        return {}
    try:
        res = (
            get_client()
            .table(DUELS_TABLE)
            .select("ai_agreed, decided_at")
            .eq("kind", "internal")
            .not_.is_("winner", "null")
            .order("decided_at", desc=False)
            .limit(200)
            .execute()
        )
        rows = res.data or []
    except Exception:
        return {}
    if not rows:
        return {"n": 0}
    agree = sum(1 for r in rows if r.get("ai_agreed"))
    return {"n": len(rows), "agree": agree, "rate": agree / len(rows)}


# ------------------------------------------------------------------ Threads

def get_threads_credentials() -> tuple:
    """registered_services 에 등록된 Threads 토큰/유저ID. 없으면 환경변수."""
    import os as _os

    if is_configured():
        try:
            res = (
                get_client()
                .table("registered_services")
                .select("api_key, endpoint_url")
                .eq("service_type", "Threads API")
                .eq("is_active", True)
                .limit(1)
                .execute()
            )
            if res.data:
                r = res.data[0]
                if r.get("api_key") and r.get("endpoint_url"):
                    return r["api_key"], r["endpoint_url"]
        except Exception:
            pass
    return (
        _os.environ.get("THREADS_ACCESS_TOKEN", ""),
        _os.environ.get("THREADS_USER_ID", ""),
    )


def list_auto_replies(active_only: bool = False) -> list[dict]:
    if not is_configured():
        return []
    try:
        q = get_client().table("threads_auto_replies").select("*")
        if active_only:
            q = q.eq("is_active", True)
        return q.order("created_at", desc=True).execute().data or []
    except Exception:
        return []


def add_auto_reply(keyword: str, content: str) -> None:
    get_client().table("threads_auto_replies").insert(
        {"trigger_keyword": keyword, "reply_content": content, "is_active": True}
    ).execute()


def toggle_auto_reply(reply_id: str, active: bool) -> None:
    get_client().table("threads_auto_replies").update({"is_active": active}).eq(
        "reply_id", reply_id
    ).execute()


def delete_auto_reply(reply_id: str) -> None:
    get_client().table("threads_auto_replies").delete().eq("reply_id", reply_id).execute()


def record_threads_post(media_id: str, generation_id: str | None, preview: str, is_head: bool) -> None:
    try:
        get_client().table("threads_posts").insert(
            {
                "media_id": media_id,
                "generation_id": generation_id,
                "text_preview": preview[:200],
                "is_chain_head": is_head,
            }
        ).execute()
    except Exception:
        pass


def threads_posts_today() -> int:
    """24시간 내 발행 건수. 250건 한도 추적용."""
    import datetime as _d

    if not is_configured():
        return 0
    try:
        since = (_d.datetime.now(_d.timezone.utc) - _d.timedelta(hours=24)).isoformat()
        res = (
            get_client()
            .table("threads_posts")
            .select("media_id")
            .gte("posted_at", since)
            .execute()
        )
        return len(res.data or [])
    except Exception:
        return 0


def watched_threads(limit: int = 20) -> list[dict]:
    if not is_configured():
        return []
    try:
        return (
            get_client()
            .table("threads_posts")
            .select("media_id, text_preview, posted_at")
            .eq("watch_replies", True)
            .eq("is_chain_head", True)
            .order("posted_at", desc=True)
            .limit(limit)
            .execute()
        ).data or []
    except Exception:
        return []


def already_replied_ids() -> set:
    if not is_configured():
        return set()
    try:
        res = get_client().table("threads_replied").select("reply_to_id").execute()
        return {r["reply_to_id"] for r in (res.data or [])}
    except Exception:
        return set()


def record_replied(reply_to_id: str, media_id: str, keyword: str) -> None:
    try:
        get_client().table("threads_replied").insert(
            {"reply_to_id": reply_to_id, "media_id": media_id, "keyword": keyword}
        ).execute()
    except Exception:
        pass
