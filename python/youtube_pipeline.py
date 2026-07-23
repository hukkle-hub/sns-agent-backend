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
# 디렉터·검수자는 판단하는 자리다. 가장 좋은 모델을 쓴다.
# 작성자는 지시를 따르는 자리라 더 가벼운 모델로 충분하다.
CRITIC_MODEL = os.environ.get("CRITIC_MODEL", "claude-opus-4-8")
WRITER_MODEL = os.environ.get("WRITER_MODEL", MODEL)
MAX_REVISIONS = int(os.environ.get("MAX_REVISIONS", "3"))
PASS_SCORE = float(os.environ.get("PASS_SCORE", "85"))
CREATIVITY_MIN = float(os.environ.get("CREATIVITY_MIN", "70"))
# 이 결과물이 시장에서 얼마짜리로 팔리는가. 검수자가 지불의사를 판정하는 기준선.
PRICE_STANDARD = os.environ.get("PRICE_STANDARD", "")


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
    platform: str
    platforms: list
    channel_outputs: dict
    cards: list
    creativity: float
    would_pay: bool
    price_standard: str
    spec: dict
    open_questions: list
    threads_chain: list


# 최신 모델은 temperature 파라미터를 받지 않는다(400 invalid_request_error)
creator_llm = ChatAnthropic(model=WRITER_MODEL, max_tokens=4000)
# 판단에는 생각할 자리가 필요하다. 1000 토큰으로는 채점밖에 못 한다.
critic_llm = ChatAnthropic(model=CRITIC_MODEL, max_tokens=4000)


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


def _rules_block() -> str:
    """수정 이력에서 누적된 규칙을 프롬프트에 주입한다.

    브랜드 보이스가 '무엇을 지향하는가'라면, 이 규칙은 '실제로 무엇을 틀렸는가'다.
    사람이 반복해서 고친 부분이므로 브랜드 보이스보다 우선한다.
    """
    try:
        import db_manager
        rules = db_manager.get_active_rules()
    except Exception:
        rules = []
    if not rules:
        return ""
    body = "\n".join(f"- {r}" for r in rules)
    return (
        "\n\n=== 과거 수정에서 학습한 규칙 (최우선) ===\n"
        + body
        + "\n=== 규칙 끝 ===\n"
        "위 규칙은 실제로 사람이 반복해서 고친 내용이다. "
        "브랜드 보이스와 충돌하면 이 규칙을 우선한다.\n"
    )


def _voice_block() -> str:
    """활성 브랜드 보이스를 프롬프트에 끼워 넣을 블록으로 만든다."""
    try:
        import db_manager
        v = db_manager.get_brand_voice()
    except Exception:
        v = ""
    if not v:
        return ""
    return (
        "\n\n=== 반드시 따라야 할 브랜드 보이스 ===\n"
        + v.strip()
        + "\n=== 브랜드 보이스 끝 ===\n"
        "위 문서의 말투, 금지 표현, 관점을 반드시 지켜라. 충돌하면 브랜드 보이스가 우선이다.\n"
    )


DIRECTOR_SYS = """너는 콘텐츠 디렉터다. 작성자가 글을 쓰기 전에 무엇을 쓸지 정한다.
너는 이 파이프라인에서 가장 판단력이 좋은 자리이며, 나중에 이 기준으로 직접 채점한다.
그러니 나중에 스스로 지킬 수 있는 기준만 세워라.

할 일:
1. 주어진 주제·자료·브랜드 보이스·누적 규칙을 읽고 이번 글의 목표를 정한다.
2. 어떤 각도로 쓸지 정한다. 흔한 각도를 피하고 왜 그 각도인지 근거를 댄다.
3. 반드시 들어가야 할 것과 절대 넣지 말 것을 구체적으로 적는다.
4. 성공 기준을 3~5개 적는다. 이건 나중에 채점표가 되므로
   '자연스러울 것' 같은 판정 불가능한 문장을 쓰지 마라.
   좋은 예: "첫 문장에 숫자나 장면이 있어 스크롤을 멈추게 한다"
5. 지시가 모호해 임의로 정하면 결과가 크게 달라질 지점이 있으면
   open_questions 에 적는다. 없으면 빈 배열.

주의: 자료가 부실하면 그 사실을 spec 에 명시하고, 없는 사실을 지어내지 말라고 못박아라.

반드시 JSON만 출력한다.
형식: {"goal":"...","angle":"...","why_angle":"...","must_include":["..."],
"must_avoid":["..."],"success_criteria":["..."],"open_questions":["..."]}"""


def director_node(state: ContentState) -> dict:
    prompt = (
        f"주제: {state.get('topic','')}\n"
        f"대상 채널: {state.get('platform') or '미지정'}\n\n"
        f"조사 결과:\n{(state.get('analyzed_gene') or '(없음)')[:3000]}\n"
        + _voice_block()
        + _rules_block()
    )
    out = parse_json(text_of(critic_llm.invoke([("system", DIRECTOR_SYS), ("human", prompt)])))
    return {
        "spec": out or {},
        "open_questions": out.get("open_questions") or [],
    }


def _spec_block(state: ContentState) -> str:
    sp = state.get("spec") or {}
    if not sp:
        return ""
    def _lines(k):
        v = sp.get(k) or []
        return "\n".join(f"  - {x}" for x in v) if isinstance(v, list) else f"  - {v}"
    return (
        "\n\n=== 이번 글의 기준 (디렉터 지시) ===\n"
        f"목표: {sp.get('goal','')}\n"
        f"각도: {sp.get('angle','')}  ({sp.get('why_angle','')})\n"
        f"반드시 포함:\n{_lines('must_include')}\n"
        f"절대 금지:\n{_lines('must_avoid')}\n"
        f"성공 기준:\n{_lines('success_criteria')}\n"
        "=== 기준 끝 ===\n"
    )


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
    plat = state.get("platform") or ""
    plat_line = (
        f"\n작성 대상 채널: {plat}. 브랜드 보이스 문서의 'Platform Tailoring Rules' 중 "
        f"{plat} 항목을 그대로 따른다(길이, 해시태그 개수, 어조).\n"
        if plat else ""
    )
    return {
        "draft": text_of(creator_llm.invoke(prompt + _spec_block(state) + plat_line + _voice_block() + _rules_block())),
        "revision_count": revision_count + 1,
    }


def critic_node(state: ContentState) -> dict:
    price = state.get("price_standard") or PRICE_STANDARD
    price_line = (
        f"\n\n[지불의사 판정]\n이 결과물의 시장가 기준은 {price} 이다. "
        "그 돈을 주고 전문가에게 맡겼을 때 받을 결과물과 비교하라. "
        "돈을 지불할 만하면 would_pay=true, 아니면 false. "
        "판단이 애매하면 false 로 한다. 관대하게 매기지 마라.\n"
        if price else ""
    )
    prompt = (
        "원고를 두 축으로 엄격히 평가하고 JSON으로만 응답하라.\n\n"
        "[축 1 · 결함 점검 → score 0~100]\n"
        "과장광고, 사실 오류, 채널 규칙 위반, CTA 부재를 감점한다.\n"
        "이 축은 '문제가 없는가'만 본다. 문제가 없다고 좋은 글은 아니다.\n\n"
        "[축 2 · 수준 평가 → creativity 0~100]\n"
        "다음을 가점한다. 기본값은 50이며, 근거 없이 올리지 마라.\n"
        "- 첫 문장이 같은 주제의 흔한 글과 구별되는가\n"
        "- 이 브랜드가 아니면 할 수 없는 말이 들어있는가\n"
        "- 구성에 의도가 보이는가 (나열이 아니라 설계인가)\n"
        "- 읽고 나서 남는 것이 있는가\n"
        "무난하고 매끄럽기만 한 글은 50점이다. 흔한 표현으로 채워졌으면 40점 이하다.\n"
        "'전문가가 썼다면 이 정도는 넘었을 것'을 기준선으로 삼아라.\n"
        + price_line
        + "\ncritique 에는 무엇을 고쳐야 축 2가 올라가는지 구체적으로 적어라.\n"
        '형식: {"score":정수,"creativity":정수,"would_pay":true|false,"critique":"..."}\n'
        "다른 말은 쓰지 마라.\n"
        + _spec_block(state)
        + "\n위 '성공 기준'을 하나씩 충족했는지 따져서 creativity 를 매겨라. "
        "기준을 못 채웠으면 몇 점인지가 아니라 어느 기준을 어떻게 못 채웠는지 critique 에 적어라.\n"
        + _voice_block()
        + _rules_block()
        + (f"\n작성 대상 채널: {state.get('platform') or '미지정'}\n")
        + f"\n원고:\n{state.get('draft','')}"
    )
    # 사람 채점과의 편차가 크면 스스로 조정하도록 보정 문구를 덧붙인다
    try:
        import db_manager
        prompt += db_manager.calibration_note()
    except Exception:
        pass
    out = parse_json(text_of(critic_llm.invoke(prompt)))

    def _num(k, default=0.0):
        try:
            return float(out.get(k))
        except (TypeError, ValueError):
            return default

    return {
        "score": _num("score"),
        "creativity": _num("creativity"),
        "would_pay": bool(out.get("would_pay")),
        "critique": out.get("critique", ""),
    }


THREADS_SYS = """너는 스레드(Threads) 전용 카피라이터다. 원고를 스레드 체인으로 바꾼다.

제약:
- 각 글은 반드시 500자 이하. 넘으면 발행 자체가 거부된다.
- 3~6개 글로 구성한다.
- 1번 글이 전부를 좌우한다. 첫 두 줄에서 멈추게 하라. 인사말로 시작하지 마라.
- 각 글은 그 자체로 읽히되, 다음 글을 궁금하게 끝낸다.
- 마지막 글에 행동 유도를 넣는다.
- 해시태그는 마지막 글에만, 3개 이하.
- 원고에 없는 사실을 만들지 마라.

중요: 링크는 본문에 넣지 마라. 스레드는 외부 링크가 많으면 노출이 줄어든다.
링크가 필요하면 마지막 글에서 댓글을 유도하는 방식으로 처리한다.

반드시 JSON만 출력한다.
형식: {"chain": ["1번 글", "2번 글", ...]}"""


def threads_node(state: ContentState) -> dict:
    """스레드 채널이 선택된 경우 체인 원고를 따로 만든다."""
    plats = state.get("platforms") or []
    if not any("스레드" in p or "Threads" in p for p in plats):
        return {}
    out = parse_json(
        text_of(
            creator_llm.invoke(
                [
                    ("system", THREADS_SYS),
                    ("human", (state.get("draft") or "") + _voice_block() + _rules_block()),
                ]
            )
        )
    )
    chain = [c.strip() for c in (out.get("chain") or []) if c and c.strip()]
    # 한도를 넘는 조각은 기계적으로 다시 자른다
    safe = []
    for c in chain:
        if len(c) <= 500:
            safe.append(c)
        else:
            try:
                from threads_publisher import split_into_chain

                safe.extend(split_into_chain(c))
            except Exception:
                safe.append(c[:500])
    return {"threads_chain": safe[:8]}


def adapter_node(state: ContentState) -> dict:
    plats = state.get("platforms") or ([state["platform"]] if state.get("platform") else [])
    if not plats:
        return {}
    prompt = (
        f"대상 채널: {', '.join(plats)}\n\n원고:\n{state.get('draft','')}"
        + _voice_block()
        + _rules_block()
    )
    out = parse_json(text_of(creator_llm.invoke([("system", ADAPTER_SYS), ("human", prompt)])))
    ch = out.get("channels") if isinstance(out.get("channels"), dict) else {}

    cards = []
    if any("인스타" in p for p in plats):
        c = parse_json(
            text_of(creator_llm.invoke([("system", CARD_SYS), ("human", state.get("draft", ""))]))
        )
        if isinstance(c.get("cards"), list):
            cards = c["cards"][:6]

    return {"channel_outputs": ch, "cards": cards}


def route_after_critic(state: ContentState) -> str:
    """결함 점수와 수준 점수를 모두 넘어야 통과.

    예전에는 결함만 없으면 통과였다. 그래서 '문제는 없지만 흔한 글'이
    계속 승인됐다. 수준 축을 별도 관문으로 둔다.
    """
    passed = (
        state.get("score", 0) >= PASS_SCORE
        and state.get("creativity", 0) >= CREATIVITY_MIN
    )
    if passed:
        return "adapt"
    if state.get("revision_count", 0) >= MAX_REVISIONS:
        return "adapt"
    return "revise"


# ------------------------------------------------------------------ 그래프

def build_graph():
    g = StateGraph(ContentState)
    g.add_node("analyst", youtube_analyst_node)
    g.add_node("director", director_node)
    g.add_node("creator", creator_node)
    g.add_node("critic", critic_node)
    g.add_edge(START, "analyst")
    g.add_edge("analyst", "director")
    g.add_edge("director", "creator")
    g.add_edge("creator", "critic")
    g.add_node("adapter", adapter_node)
    g.add_node("threads", threads_node)
    g.add_conditional_edges(
        "critic", route_after_critic, {"revise": "creator", "adapt": "adapter"}
    )
    g.add_edge("adapter", "threads")
    g.add_edge("threads", END)
    return g.compile()


app = build_graph()
