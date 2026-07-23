
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


def update_human_approval(
    generation_id: str,
    final_draft: str,
    status: str = "approved",
) -> None:
    """태블릿에서 수정한 정답지 저장 (SFT 학습용)."""
    get_client().table(TABLE).update(
        {"human_modified_output": final_draft, "approval_status": status}
    ).eq("generation_id", generation_id).execute()
