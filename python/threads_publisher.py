"""Meta 공식 Threads API 연동.

공식 제한 (프로필당 24시간 롤링):
  - 게시물 250건
  - 답글 1,000건
스레드 체인은 배치 엔드포인트가 없어 이전 글 ID를 받아가며 순차로 올려야 한다.

발행은 2단계다: 컨테이너 생성 → publish.
"""

import os
import time
from typing import Optional

import requests

BASE_URL = "https://graph.threads.net/v1.0"
TEXT_LIMIT = 500
DAILY_POST_LIMIT = 250
TIMEOUT = 20


class ThreadsError(Exception):
    pass


class ThreadsAPI:
    def __init__(self, access_token: str, threads_user_id: str):
        if not access_token or not threads_user_id:
            raise ThreadsError("access_token 과 threads_user_id 가 모두 필요합니다.")
        self.access_token = access_token
        self.user_id = str(threads_user_id)

    # ------------------------------------------------------------------ 내부

    def _post(self, path: str, payload: dict) -> dict:
        payload = {**payload, "access_token": self.access_token}
        r = requests.post(f"{BASE_URL}/{path}", data=payload, timeout=TIMEOUT)
        try:
            data = r.json()
        except ValueError:
            raise ThreadsError(f"HTTP {r.status_code}: 응답이 JSON이 아닙니다")
        if "error" in data:
            e = data["error"]
            raise ThreadsError(f"{e.get('code','')} {e.get('message','')}".strip())
        if not r.ok:
            raise ThreadsError(f"HTTP {r.status_code}: {str(data)[:200]}")
        return data

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        params = {**(params or {}), "access_token": self.access_token}
        r = requests.get(f"{BASE_URL}/{path}", params=params, timeout=TIMEOUT)
        try:
            data = r.json()
        except ValueError:
            raise ThreadsError(f"HTTP {r.status_code}: 응답이 JSON이 아닙니다")
        if "error" in data:
            e = data["error"]
            raise ThreadsError(f"{e.get('code','')} {e.get('message','')}".strip())
        return data

    # ------------------------------------------------------------------ 한도

    def publishing_limit(self) -> dict:
        """남은 발행 한도. {'used': n, 'total': 250, 'remaining': m}"""
        try:
            d = self._get(
                f"{self.user_id}/threads_publishing_limit", {"fields": "quota_usage,config"}
            )
            row = (d.get("data") or [{}])[0]
            used = int(row.get("quota_usage") or 0)
            total = int((row.get("config") or {}).get("quota_total") or DAILY_POST_LIMIT)
            return {"used": used, "total": total, "remaining": max(0, total - used)}
        except Exception as e:
            return {"error": str(e)}

    # ------------------------------------------------------------------ 발행

    def create_post(self, text: str, reply_to_id: Optional[str] = None) -> str:
        """글 하나를 올리고 media_id 를 돌려준다. 실패 시 예외."""
        text = (text or "").strip()
        if not text:
            raise ThreadsError("본문이 비어 있습니다.")
        if len(text) > TEXT_LIMIT:
            raise ThreadsError(f"본문이 {len(text)}자입니다. 최대 {TEXT_LIMIT}자.")

        payload = {"media_type": "TEXT", "text": text}
        if reply_to_id:
            payload["reply_to_id"] = str(reply_to_id)

        container = self._post(f"{self.user_id}/threads", payload)
        creation_id = container.get("id")
        if not creation_id:
            raise ThreadsError(f"컨테이너 생성 실패: {str(container)[:200]}")

        published = self._post(
            f"{self.user_id}/threads_publish", {"creation_id": creation_id}
        )
        media_id = published.get("id")
        if not media_id:
            raise ThreadsError(f"발행 실패: {str(published)[:200]}")
        return media_id

    def publish_chain(self, parts: list, pause: float = 2.0) -> list:
        """스레드 체인. 배치 API가 없어 앞 글 ID를 받아가며 순차 발행한다.

        중간에 실패하면 그때까지 올라간 id 목록과 함께 예외를 올린다
        (이미 올라간 글을 되돌릴 수 없으므로 호출자가 알아야 한다).
        """
        ids = []
        parent = None
        for i, p in enumerate(parts):
            try:
                mid = self.create_post(p, reply_to_id=parent)
            except ThreadsError as e:
                raise ThreadsError(
                    f"{i+1}번째 글에서 실패({e}). 이미 발행됨: {len(ids)}건 {ids}"
                )
            ids.append(mid)
            parent = mid
            if i < len(parts) - 1:
                time.sleep(pause)   # 연속 호출 완화
        return ids

    # ------------------------------------------------------------------ 답글

    def fetch_replies(self, media_id: str) -> list:
        d = self._get(
            f"{media_id}/replies",
            {"fields": "id,text,username,timestamp", "reverse": "false"},
        )
        return d.get("data") or []

    def reply_to(self, reply_to_id: str, text: str) -> str:
        return self.create_post(text, reply_to_id=reply_to_id)


# ---------------------------------------------------------------------- 자동 답글

def run_auto_reply(api: "ThreadsAPI", media_id: str, rules: list, already: set) -> list:
    """키워드가 걸린 댓글에 답글을 단다.

    rules: [{"trigger_keyword": "자료", "reply_content": "..."}]
    already: 이미 답글을 단 댓글 id 집합. **이게 없으면 감시할 때마다
             같은 댓글에 답글이 계속 달린다.**

    반환: [{"reply_to_id":..., "keyword":..., "new_id":...}]
    """
    done = []
    active = [r for r in rules if r.get("is_active", True)]
    if not active:
        return done

    for rep in api.fetch_replies(media_id):
        rid = rep.get("id")
        body = rep.get("text") or ""
        if not rid or rid in already:
            continue
        for rule in active:
            kw = (rule.get("trigger_keyword") or "").strip()
            if kw and kw in body:
                try:
                    new_id = api.reply_to(rid, rule["reply_content"])
                    done.append({"reply_to_id": rid, "keyword": kw, "new_id": new_id})
                    already.add(rid)
                except ThreadsError:
                    pass
                break   # 한 댓글에 규칙 하나만 적용
    return done


def split_into_chain(text: str, limit: int = TEXT_LIMIT) -> list:
    """긴 글을 스레드 체인용으로 나눈다. 문단 → 문장 순으로 자른다."""
    text = (text or "").strip()
    if len(text) <= limit:
        return [text] if text else []

    parts, buf = [], ""
    for para in text.split("\n\n"):
        para = para.strip()
        if not para:
            continue
        if len(buf) + len(para) + 2 <= limit:
            buf = f"{buf}\n\n{para}" if buf else para
            continue
        if buf:
            parts.append(buf)
            buf = ""
        if len(para) <= limit:
            buf = para
            continue
        # 문단 하나가 한도를 넘으면 문장 단위로 다시 자른다
        sent, cur = para.replace("? ", "?|").replace(". ", ".|").split("|"), ""
        for s in sent:
            if len(cur) + len(s) + 1 <= limit:
                cur = f"{cur} {s}".strip()
            else:
                if cur:
                    parts.append(cur)
                cur = s[:limit]
        if cur:
            buf = cur
    if buf:
        parts.append(buf)
    return parts


def from_env() -> Optional["ThreadsAPI"]:
    tok = os.environ.get("THREADS_ACCESS_TOKEN", "")
    uid = os.environ.get("THREADS_USER_ID", "")
    if not (tok and uid):
        return None
    return ThreadsAPI(tok, uid)
