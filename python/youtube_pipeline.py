
"""유튜브 벤치마킹 → 유전자 분석 → 원고 작성 → 검수 재수정 루프.

붙여넣은 원본에서 critic_node 이후가 잘려 있어 채워 넣었고,
youtube-transcript-api 1.x 에 맞게 호출부를 고쳤다.
"""

import json
import os
import re
from typing import TypedDict

from langchain_anthropic import ChatAnthropic
from langgraph.graph import END, START, StateGraph
from youtube_transcript_api import YouTubeTranscriptApi

MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-5")
MAX_REVISIONS = int(os.environ.get("MAX_REVISIONS", "3"))
PASS_SCORE = float(os.environ.get("PASS_SCORE", "85"))


class ContentState(TypedDict, total=False):
    youtube_target: str
    topic: str
    video_id: str
    transcript: str
    analyzed_gene: str
    draft: str
    score: float
    critique: str
    revision_count: int


creator_llm = ChatAnthropic(model=MODEL, temperature=0.7, max_tokens=4000)
critic_llm = ChatAnthropic(model=MODEL, temperature=0.2, max_tokens=1000)


# ------------------------------------------------------------------ 유틸

_YT_RE = re.compile(r"(?:youtu\.be/|watch\?v=|/shorts/|/embed/|/v/)([0-9A-Za-z_-]{11})")


def extract_video_id(url_or_id: str) -> str:
    s = (url_or_id or "").strip()
    if re.fullmatch(r"[0-9A-Za-z_-]{11}", s):
        return s
    m = _YT_RE.search(s)
    if m:
        return m.group(1)
    raise ValueError("올바른 유튜브 URL 또는 Video ID가 아닙니다.")


def text_of(response) -> str:
    c = response.content
    if isinstance(c, list):
        return "".join(
            p.get("text", "") if isinstance(p, dict) else str(p) for p in c
        ).strip()
    return str(c).strip()


def parse_json(text: str) -> dict:
    cleaned = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.MULTILINE).strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                pass
    return {}


# ------------------------------------------------------------------ 노드

def youtube_analyst_node(state: ContentState) -> dict:
    video_id = extract_video_id(state["youtube_target"])
    try:
        fetched = YouTubeTranscriptApi().fetch(video_id, languages=["ko", "en"])
        transcript_text = " ".join(snippet.text for snippet in fetched).strip()
    except Exception as e:
        transcript_text = f"자막 추출 불가: {e}"

    prompt = (
        "당신은 최고 수준의 콘텐츠 유전자 분석가입니다.\n"
        "아래 대본을 분석하여 첫 3초 후킹, 리텐션 구조, CTA 성공 패턴을 추출하세요.\n"
        "자막을 가져오지 못했다면 그 사실을 명시하고 일반적인 패턴만 제시하세요.\n\n"
        f"{transcript_text[:3000]}"
    )
    return {
        "video_id": video_id,
        "transcript": transcript_text[:1000],
        "analyzed_gene": text_of(creator_llm.invoke(prompt)),
    }


def creator_node(state: ContentState) -> dict:
    revision_count = state.get("revision_count", 0)
    if revision_count == 0:
        prompt = (
            f"주제: {state['topic']}\n"
            f"유전자 분석:\n{state.get('analyzed_gene','')}\n\n"
            "위 유전자를 이식하여 최상급 원고를 작성해 주세요. "
            "과장광고 표현과 근거 없는 수치는 쓰지 마세요."
        )
    else:
        prompt = (
            f"이전 원고:\n{state.get('draft','')}\n\n"
            f"검수 피드백:\n{state.get('critique','')}\n\n"
            "피드백을 반영하여 완성본을 다시 작성하세요."
        )
    return {
        "draft": text_of(creator_llm.invoke(prompt)),
        "revision_count": revision_count + 1,
    }


def critic_node(state: ContentState) -> dict:
    prompt = (
        "원고 품질을 엄격히 평가하고 JSON으로만 응답하세요. 다른 말은 쓰지 마세요.\n"
        '형식: {"score": 0~100 정수, "critique": "구체적인 수정 지시"}\n'
        "평가 기준: 후킹 강도, 리텐션 구조, CTA 명확성, 과장광고 여부, 사실 정확성\n\n"
        f"원고:\n{state.get('draft','')}"
    )
    out = parse_json(text_of(critic_llm.invoke(prompt)))
    try:
        score = float(out.get("score"))
    except (TypeError, ValueError):
        score = 0.0
    return {"score": score, "critique": out.get("critique", "")}


def route_after_critic(state: ContentState) -> str:
    if state.get("score", 0) >= PASS_SCORE:
        return "end"
    if state.get("revision_count", 0) >= MAX_REVISIONS:
        return "end"
    return "revise"


# ------------------------------------------------------------------ 그래프

def build_graph():
    g = StateGraph(ContentState)
    g.add_node("analyst", youtube_analyst_node)
    g.add_node("creator", creator_node)
    g.add_node("critic", critic_node)
    g.add_edge(START, "analyst")
    g.add_edge("analyst", "creator")
    g.add_edge("creator", "critic")
    g.add_conditional_edges(
        "critic", route_after_critic, {"revise": "creator", "end": END}
    )
    return g.compile()


app = build_graph()
