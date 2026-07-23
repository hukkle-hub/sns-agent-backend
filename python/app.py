import hmac
import json
import os
import threading

import requests
import streamlit as st
import streamlit.components.v1 as components

import db_manager
from db_manager import supabase, update_human_approval

MAIN_WEB_APP_URL = "https://hukkle-hub.github.io/sns-agent-app/index.html"
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")
N8N_WEBHOOK_URL = os.environ.get("N8N_WEBHOOK_URL", "")
REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "15"))
SFT_GOAL = int(os.environ.get("SFT_GOAL", "300"))

st.set_page_config(
    page_title="AI Agency Control Center",
    page_icon="📱",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# 📱 태블릿 반응형 CSS
# ---------------------------------------------------------------------------
st.markdown(
    """
    <style>
    .main .block-container {
        padding: 0.8rem !important;
        max-width: 100% !important;
    }
    header {visibility: hidden !important;}
    footer {visibility: hidden !important;}

    /* Streamlit Custom Component 부모 div 높이 강제 통제 */
    div[data-testid="stCustomComponentV1"],
    div[data-testid="stCustomComponentV1"] > iframe {
        height: 82vh !important;
        max-height: 82vh !important;
        width: 100% !important;
    }
    iframe {
        width: 100% !important;
        height: 82vh !important;
        border: 1px solid #e0e0e0 !important;
        border-radius: 12px !important;
    }

    .stTabs [data-baseweb="tab-list"] { gap: 6px; }
    .stTabs [data-baseweb="tab"] {
        height: 3.2rem !important;
        font-size: 1.1rem !important;
        font-weight: 600 !important;
        padding: 0px 16px !important;
    }
    .stTextArea textarea {
        font-size: 1.05rem !important;
        line-height: 1.6 !important;
    }
    .stButton>button {
        width: 100%;
        min-height: 3.3rem !important;
        font-size: 1.1rem !important;
        font-weight: bold !important;
        border-radius: 10px !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# 🔐 보안 로그인
# ---------------------------------------------------------------------------
def check_password() -> bool:
    if not ADMIN_PASSWORD:
        st.error(
            "⚠️ ADMIN_PASSWORD 가 설정되지 않았습니다. "
            "이 주소를 아는 누구나 접속할 수 있습니다. Render 환경변수에 추가하세요."
        )
        return True

    if st.session_state.get("authenticated"):
        return True

    st.title("🔒 AI 관제탑 보안 로그인")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.info("태블릿 접근 권한 확인이 필요합니다.")
        pwd = st.text_input("🔑 관리자 비밀번호", type="password")
        if st.button("관제탑 접속", type="primary"):
            if hmac.compare_digest(pwd, ADMIN_PASSWORD):
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("비밀번호가 올바르지 않습니다.")
    return False


if not check_password():
    st.stop()

if not db_manager.is_configured():
    st.error(
        "SUPABASE_URL / SUPABASE_KEY 가 없어 DB 기능이 모두 비활성화됩니다. "
        "Render 환경변수를 확인하세요."
    )

st.title("📱 AI 에이전시 통합 관제탑")


# ---------------------------------------------------------------------------
# ⚡ 비동기 백그라운드 작업
# ---------------------------------------------------------------------------
def run_agent_background(youtube_url: str, new_topic: str, platforms=None) -> None:
    """별도 스레드에서 실행. st.* 는 절대 호출하지 않는다 (컨텍스트 없음)."""
    gen_id = None
    try:
        res = (
            supabase.table("content_generations")
            .insert(
                {
                    "topic": new_topic,
                    "ai_raw_output": "⏳ AI 에이전트 그룹이 자막 분석 및 원고 작성을 진행 중입니다...",
                    "ai_self_score": 0.0,
                    "revision_count": 0,
                    "approval_status": "processing",
                }
            )
            .execute()
        )
        if not res.data:
            return
        gen_id = res.data[0]["generation_id"]

        from youtube_pipeline import app as langgraph_app

        result = langgraph_app.invoke(
            {
                "youtube_target": youtube_url,
                "topic": new_topic,
                "revision_count": 0,
                "platforms": platforms or [],
                "platform": (platforms or [""])[0],
            }
        )

        supabase.table("content_generations").update(
            {
                "ai_raw_output": result.get("draft", "생성 실패"),
                "ai_self_score": result.get("score", 0.0),
                "revision_count": result.get("revision_count", 0),
                "approval_status": "pending",
                "channel_outputs": result.get("channel_outputs") or None,
                "cards": result.get("cards") or None,
                "creativity_score": result.get("creativity"),
                "would_pay": result.get("would_pay"),
                "spec": result.get("spec") or None,
                "open_questions": result.get("open_questions") or None,
                "threads_chain": result.get("threads_chain") or None,
            }
        ).eq("generation_id", gen_id).execute()

    except Exception as e:
        if gen_id:
            try:
                supabase.table("content_generations").update(
                    {
                        "ai_raw_output": f"❌ 생성 오류: {e}",
                        "approval_status": "failed",
                    }
                ).eq("generation_id", gen_id).execute()
            except Exception:
                pass


def _plan_worker(topic: str, channels: list, when_iso: str) -> None:
    """주제 하나를 생성하고 곧바로 발행 예약까지 건다."""
    gen_id = None
    try:
        res = (
            supabase.table("content_generations")
            .insert(
                {
                    "topic": topic,
                    "ai_raw_output": "⏳ 주간 플랜 생성 중...",
                    "ai_self_score": 0.0,
                    "revision_count": 0,
                    "approval_status": "processing",
                }
            )
            .execute()
        )
        if not res.data:
            return
        gen_id = res.data[0]["generation_id"]

        from youtube_pipeline import app as langgraph_app

        result = langgraph_app.invoke(
            {
                "youtube_target": "",
                "topic": topic,
                "revision_count": 0,
                "platforms": channels,
                "platform": channels[0] if channels else "",
            }
        )

        supabase.table("content_generations").update(
            {
                "ai_raw_output": result.get("draft", "생성 실패"),
                "ai_self_score": result.get("score", 0.0),
                "revision_count": result.get("revision_count", 0),
                "approval_status": "pending",
                "channel_outputs": result.get("channel_outputs") or None,
                "cards": result.get("cards") or None,
                "creativity_score": result.get("creativity"),
                "would_pay": result.get("would_pay"),
                "spec": result.get("spec") or None,
                "open_questions": result.get("open_questions") or None,
                "threads_chain": result.get("threads_chain") or None,
                "scheduled_at": when_iso,
                "publish_status": "scheduled",
                "target_channel": ", ".join(channels),
            }
        ).eq("generation_id", gen_id).execute()

    except Exception as e:
        if gen_id:
            try:
                supabase.table("content_generations").update(
                    {"ai_raw_output": f"❌ 생성 오류: {e}", "approval_status": "failed"}
                ).eq("generation_id", gen_id).execute()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 🗂️ 탭
# ---------------------------------------------------------------------------
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs(
    [
        "🌐 SNS 에이전트 앱",
        "🚀 신규 에이전트 가동",
        "📥 승인 대기 관제탑",
        "📊 AI 학습 데이터셋",
        "⚙️ 서비스 직접 등록",
        "🎙️ 브랜드 보이스",
        "🗓️ 주간 플랜",
        "🧠 학습 현황",
        "⚔️ 대결 평가",
    ]
)

# --------------------------------------------------- TAB 1: 웹 앱 연동
with tab1:
    st.caption(f"연동 주소: {MAIN_WEB_APP_URL} · [🔗 새 창 열기]({MAIN_WEB_APP_URL})")
    components.iframe(MAIN_WEB_APP_URL, height=750, scrolling=True)


# --------------------------------------------------- TAB 2: 작업 가동
with tab2:
    st.subheader("🎯 새로운 벤치마킹 & 원고 생성 지시")
    with st.form("agent_trigger_form"):
        youtube_url = st.text_input(
            "🔗 벤치마킹 유튜브 URL", placeholder="https://www.youtube.com/watch?v=..."
        )
        new_topic = st.text_input(
            "💡 적용할 신규 주제", placeholder="예: 신제품 출시 안내 / 여름 시즌 기획전"
        )
        platforms = st.multiselect(
            "📣 발행 채널 (복수 선택)",
            ["인스타그램", "네이버 블로그/카페", "유튜브", "상품 상세페이지", "스레드(Threads)"],
            default=["인스타그램"],
            help="선택한 채널마다 길이·어조·해시태그를 다르게 재가공합니다. 인스타그램을 고르면 카드뉴스 6장도 함께 생성됩니다.",
        )
        submit_btn = st.form_submit_button("🚀 AI 에이전트 그룹 가동하기", type="primary")

    if submit_btn:
        if not (youtube_url and new_topic):
            st.warning("유튜브 URL과 주제를 모두 입력하세요.")
        elif not db_manager.is_configured():
            st.error("Supabase 설정이 없어 작업을 시작할 수 없습니다.")
        else:
            threading.Thread(
                target=run_agent_background,
                args=(youtube_url, new_topic, platforms),
                daemon=False,
            ).start()
            st.success("🚀 백그라운드 작업이 시작되었습니다! '📥 승인 대기 관제탑' 탭에서 확인하세요.")


# --------------------------------------------------- TAB 3: 승인 관제탑
@st.fragment(run_every=REFRESH_SECONDS)
def render_approval_control_center():
    if not db_manager.is_configured():
        st.info("Supabase 설정이 필요합니다.")
        return

    try:
        items = (
            supabase.table("content_generations")
            .select("*")
            .in_("approval_status", ["pending", "processing", "failed"])
            .order("created_at", desc=True)
            .execute()
            .data
        )
    except Exception as e:
        st.error(f"목록을 불러오지 못했습니다: {e}")
        return

    if not items:
        st.info("🎉 현재 대기 중인 원고가 없습니다.")
        return

    st.subheader(f"📥 관제 대기 목록 ({len(items)}건)")

    try:
        webhooks = (
            supabase.table("registered_services")
            .select("*")
            .eq("is_active", True)
            .execute()
            .data
        )
    except Exception:
        webhooks = []
    webhook_options = {
        s["service_name"]: s["endpoint_url"] for s in webhooks if s.get("endpoint_url")
    }

    for item in items:
        content_id = item["generation_id"]
        topic = item.get("topic", "(제목 없음)")
        score = item.get("ai_self_score")
        ai_draft = item.get("ai_raw_output", "")
        status = item.get("approval_status")

        if status == "processing":
            st.warning(f"⏳ [AI 실시간 생성 중...] {topic}")
            continue

        if status == "failed":
            st.error(f"❌ [생성 실패] {topic}")
            st.caption(ai_draft[:300])
            if st.button("삭제", key=f"del_failed_{content_id}"):
                supabase.table("content_generations").delete().eq(
                    "generation_id", content_id
                ).execute()
                st.rerun(scope="app")
            continue

        score_txt = f"{float(score):.0f}점" if score is not None else "미평가"
        channels = item.get("channel_outputs") or {}
        cards = item.get("cards") or []

        with st.expander(f"📌 [{score_txt}] {topic}", expanded=True):
            if channels:
                names = list(channels.keys())
                ctabs = st.tabs([f"📄 원본"] + [f"📣 {n}" for n in names])
                with ctabs[0]:
                    st.text_area("원본 원고", value=ai_draft, height=200,
                                 key=f"src_{content_id}", disabled=True)
                for i, n in enumerate(names, start=1):
                    with ctabs[i]:
                        st.code(channels[n], language=None)
                        st.caption("코드블록 오른쪽 아이콘으로 복사하세요")

            oq = item.get("open_questions") or []
            if oq:
                st.warning("**디렉터가 확인을 요청했습니다**")
                for q in oq:
                    st.markdown(f"- {q}")

            sp = item.get("spec") or {}
            if sp:
                with st.expander("🎬 디렉터가 세운 기준 (검수는 이 기준으로 채점됨)"):
                    st.markdown(f"**목표** · {sp.get('goal','')}")
                    st.markdown(f"**각도** · {sp.get('angle','')} — {sp.get('why_angle','')}")
                    if sp.get("success_criteria"):
                        st.markdown("**성공 기준**")
                        for c in sp["success_criteria"]:
                            st.markdown(f"- {c}")

            if cards:
                st.markdown("**🖼️ 카드뉴스 6장**")
                for c in cards:
                    with st.container(border=True):
                        st.markdown(f"**{c.get('no','')}. {c.get('title','')}**")
                        st.write(c.get("body", ""))
                        st.caption("이미지 생성 프롬프트 (Gemini/Veo 에 붙여넣기)")
                        st.code(c.get("visual_prompt", ""), language=None)

            col1, col2 = st.columns([3, 1])
            with col1:
                edited_draft = st.text_area(
                    "최종 원고 (승인 시 이 내용이 저장됩니다)",
                    value=ai_draft,
                    height=280,
                    key=f"edit_{content_id}",
                )
            with col2:
                st.markdown("### 🎛️ 발행 조종석")
                ai_cre = item.get("creativity_score")
                st.caption(
                    f"AI 수준 점수: {float(ai_cre):.0f}" if ai_cre is not None else "AI 수준 점수: -"
                )
                my_score = st.slider(
                    "내 채점 (수준)", 0, 100, int(ai_cre or 50), 5,
                    key=f"hs_{content_id}",
                    help="AI 채점이 맞는지 대조합니다. 편차가 크면 검수자가 스스로 보정합니다.",
                )
                if st.button("💾 채점만 저장", key=f"sc_{content_id}", use_container_width=True):
                    try:
                        db_manager.record_human_score(content_id, my_score, ai_cre)
                        st.success(f"저장 · 편차 {float(ai_cre or 0)-my_score:+.0f}")
                    except Exception as e:
                        st.error(f"실패: {e}")
                options = list(webhook_options.keys()) or ["기본 웹훅"]
                selected_svc = st.selectbox(
                    "발행 채널 선택", options, key=f"svc_{content_id}"
                )

                if st.button("👍 1초 승인 & 자동 발행", key=f"app_{content_id}", type="primary"):
                    target_url = webhook_options.get(selected_svc, N8N_WEBHOOK_URL)
                    try:
                        if target_url:
                            requests.post(
                                target_url,
                                json={
                                    "content_id": content_id,
                                    "topic": topic,
                                    "final_text": edited_draft,
                                },
                                timeout=15,
                            ).raise_for_status()
                        else:
                            st.warning("발행 채널이 없어 승인만 처리합니다.")
                        update_human_approval(content_id, edited_draft, "approved", original=ai_draft)
                        try:
                            db_manager.record_human_score(content_id, my_score, ai_cre)
                        except Exception:
                            pass
                        st.success("발행 완료!")
                        st.rerun(scope="app")
                    except Exception as e:
                        st.error(f"발행 실패: {e}")

                chain = item.get("threads_chain") or []
                if chain:
                    st.caption(f"🧵 스레드 체인 {len(chain)}개")
                    if st.button("🧵 스레드 발행", key=f"th_{content_id}", use_container_width=True):
                        try:
                            tok, uid = db_manager.get_threads_credentials()
                            if not (tok and uid):
                                st.error("⚙️ 탭에서 Threads API 를 먼저 등록하세요.")
                            elif db_manager.threads_posts_today() + len(chain) > 250:
                                st.error("24시간 발행 한도(250건)에 걸립니다.")
                            else:
                                from threads_publisher import ThreadsAPI

                                api = ThreadsAPI(tok, uid)
                                ids = api.publish_chain(chain)
                                for i, mid in enumerate(ids):
                                    db_manager.record_threads_post(
                                        mid, content_id, chain[i], i == 0
                                    )
                                update_human_approval(
                                    content_id, edited_draft, "approved", original=ai_draft
                                )
                                st.success(f"스레드 {len(ids)}개 발행 완료")
                                st.rerun(scope="app")
                        except Exception as e:
                            st.error(f"스레드 발행 실패: {e}")

                if st.button("🚫 반려", key=f"rej_{content_id}"):
                    update_human_approval(content_id, edited_draft, "rejected", original=ai_draft)
                    st.rerun(scope="app")


with tab3:
    render_approval_control_center()


# --------------------------------------------------- TAB 4: 학습 데이터셋
with tab4:
    st.subheader("📊 AI 학습 데이터 축적 현황")
    if not db_manager.is_configured():
        st.info("Supabase 설정이 필요합니다.")
    else:
        try:
            approved_data = (
                supabase.table("content_generations")
                .select("*")
                .eq("approval_status", "approved")
                .execute()
                .data
            )
        except Exception as e:
            st.error(f"조회 실패: {e}")
            approved_data = []

        approved_count = len(approved_data)
        st.metric("학습 데이터셋 (SFT)", f"{approved_count} 건", delta=f"목표: {SFT_GOAL}건")
        st.progress(min(1.0, approved_count / SFT_GOAL) if SFT_GOAL else 0.0)

        jsonl_lines = []
        for d in approved_data:
            if d.get("human_modified_output"):
                jsonl_lines.append(
                    json.dumps(
                        {
                            "messages": [
                                {
                                    "role": "system",
                                    "content": "너는 자율 진화하는 전문 콘텐츠 크리에이터 AI다.",
                                },
                                {"role": "user", "content": f"주제: {d['topic']}"},
                                {
                                    "role": "assistant",
                                    "content": d["human_modified_output"],
                                },
                            ]
                        },
                        ensure_ascii=False,
                    )
                )

        if jsonl_lines:
            st.download_button(
                "📥 SFT 미세조정 데이터셋 (JSONL) 다운로드",
                data="\n".join(jsonl_lines),
                file_name="ai_agency_sft_dataset.jsonl",
                mime="application/jsonl",
                type="primary",
            )
        else:
            st.caption("승인된 수정본이 쌓이면 여기서 JSONL 로 내려받을 수 있습니다.")


# --------------------------------------------------- TAB 5: 서비스 등록
with tab5:
    st.subheader("⚙️ 외부 연동 서비스 직접 등록")
    if not db_manager.is_configured():
        st.info("Supabase 설정이 필요합니다.")
    else:
        with st.form("register_service_form"):
            svc_name = st.text_input("서비스 이름", placeholder="예: 워드프레스 블로그")
            svc_type = st.selectbox(
                "서비스 유형", ["웹훅(Webhook)", "API Key", "SNS 채널 URL", "Threads API"]
            )
            if svc_type == "Threads API":
                st.info(
                    "Meta 개발자 센터에서 발급한 **60일 장기 토큰**과 **Threads User ID**를 "
                    "넣으세요. 일반 App ID가 아니라 설정 페이지 하단의 **Threads App ID** "
                    "기준으로 발급한 토큰이어야 합니다."
                )
                endpoint_url = st.text_input("Threads User ID", key="th_uid")
                api_key = st.text_input("60일 장기 액세스 토큰", type="password", key="th_tok")
            else:
                endpoint_url = st.text_input("엔드포인트 / Webhook URL")
                api_key = st.text_input("API Key (선택)", type="password")
            reg_btn = st.form_submit_button("➕ 서비스 등록", type="primary")

        if reg_btn and svc_name:
            supabase.table("registered_services").insert(
                {
                    "service_name": svc_name,
                    "service_type": svc_type,
                    "endpoint_url": endpoint_url,
                    "api_key": api_key,
                    "is_active": True,
                }
            ).execute()
            st.success(f"✅ '{svc_name}' 등록 완료!")
            st.rerun()

        # ── 스레드 자동 답글
        st.divider()
        st.markdown("### 🧵 스레드 자동 답글")
        st.caption(
            "지정한 키워드가 댓글에 보이면 자동으로 답글을 답니다. "
            "이미 답글을 단 댓글은 다시 달지 않습니다."
        )

        tok, uid = db_manager.get_threads_credentials()
        if not (tok and uid):
            st.info("위에서 Threads API 를 먼저 등록하세요.")
        else:
            lim = None
            try:
                from threads_publisher import ThreadsAPI, run_auto_reply

                api = ThreadsAPI(tok, uid)
                lim = api.publishing_limit()
            except Exception as e:
                st.warning(f"Threads 연결 확인 실패: {e}")

            if lim and not lim.get("error"):
                st.metric(
                    "24시간 발행 한도",
                    f"{lim['used']} / {lim['total']}",
                    delta=f"남은 {lim['remaining']}건",
                )

            with st.form("ar_form"):
                k = st.text_input("트리거 키워드", placeholder="예: 자료")
                v = st.text_area(
                    "자동 답글 내용",
                    placeholder="아래에서 확인하실 수 있습니다: https://...",
                    height=90,
                )
                if st.form_submit_button("➕ 규칙 추가") and k.strip() and v.strip():
                    try:
                        db_manager.add_auto_reply(k.strip(), v.strip())
                        st.success("추가됨")
                        st.rerun()
                    except Exception as e:
                        st.error(f"실패: {e}")

            st.warning(
                "**첫 댓글에 외부 링크는 1개까지.** 2개 이상이면 원글까지 노출이 차단됩니다."
            )

            for r in db_manager.list_auto_replies():
                cc1, cc2, cc3 = st.columns([5, 1, 1])
                mk = "🟢" if r.get("is_active") else "⚪"
                cc1.markdown(f"{mk} **{r['trigger_keyword']}** → {r['reply_content'][:60]}")
                if cc2.button(
                    "끄기" if r.get("is_active") else "켜기",
                    key=f"ta_{r['reply_id']}",
                    use_container_width=True,
                ):
                    db_manager.toggle_auto_reply(r["reply_id"], not r.get("is_active"))
                    st.rerun()
                if cc3.button("삭제", key=f"td_{r['reply_id']}", use_container_width=True):
                    db_manager.delete_auto_reply(r["reply_id"])
                    st.rerun()

            watched = db_manager.watched_threads()
            if watched and st.button(f"🔄 지금 댓글 확인 ({len(watched)}개 글)"):
                rules = db_manager.list_auto_replies(active_only=True)
                if not rules:
                    st.warning("활성 규칙이 없습니다.")
                else:
                    already = db_manager.already_replied_ids()
                    total = 0
                    for w in watched:
                        try:
                            done = run_auto_reply(api, w["media_id"], rules, already)
                            for d in done:
                                db_manager.record_replied(
                                    d["reply_to_id"], w["media_id"], d["keyword"]
                                )
                            total += len(done)
                        except Exception as e:
                            st.caption(f"{w['media_id'][:12]}… 확인 실패: {e}")
                    st.success(f"자동 답글 {total}건 작성")

        st.divider()
        try:
            services = (
                supabase.table("registered_services")
                .select("*")
                .order("created_at", desc=True)
                .execute()
                .data
            )
        except Exception as e:
            st.error(f"조회 실패: {e}")
            services = []

        for s in services:
            c1, c2, c3 = st.columns([2, 3, 1])
            c1.write(f"**{s['service_name']}** ({s['service_type']})")
            c2.code(s.get("endpoint_url") or "N/A")
            if c3.button("삭제", key=f"del_{s['service_id']}"):
                supabase.table("registered_services").delete().eq(
                    "service_id", s["service_id"]
                ).execute()
                st.rerun()


# --------------------------------------------------- TAB 6: 브랜드 보이스

INTERVIEWER_SYS = """너는 소셜 미디어 브랜딩 전문가다. 브랜드 보이스 문서를 만들기 위한 인터뷰를 진행한다.

규칙:
1. 질문은 반드시 한 번에 하나씩만 한다. 여러 개를 나열하지 않는다.
2. 답변이 모호하거나 일반적이면("좋아요", "정성을 다합니다") 넘어가지 말고 되물어라.
   왜 그 답이 부족한지 짧게 짚고, 더 구체적인 답을 끌어낼 각도를 제시한다.
   예: "그건 같은 업계 누구나 하는 말이라 차별점이 안 됩니다. 고객이 남긴 반응 중
   기억나는 문장이 있나요?"
3. 좋은 답이 나오면 짧게 인정하고 다음으로 넘어간다. 과한 칭찬은 하지 않는다.
4. 구체적인 일화, 실제로 쓰는 표현, 절대 쓰기 싫은 표현을 캐낸다.
5. 8~10개 문답이면 충분하다. 다 모이면 "인터뷰는 여기까지면 충분합니다"라고 알린다.
6. 한국어로 편하게 대화한다.

반드시 아래를 모두 다룬다.

[1] 정체성과 독자
  - 브랜드/사람 이름과 정체 (누가 말하는가)
  - 핵심 메시지 한 줄
  - 타깃 독자의 구체적인 모습 (나이·상황·고민까지)

[2] 말투와 스타일
  - 원하는 어조 (구체적인 예시 문장으로)
  - 절대 쓰지 않을 표현 — 반드시 실제 문구를 받아낸다
  - 강조하고 싶은 서술 방식

[3] 채널별 규칙 — 각 채널을 하나씩 물어본다
  - 인스타그램: 길이, 캡션 톤, 해시태그 개수
  - 네이버 블로그/카페: 길이, 어조, 사진 배치
  - 유튜브: 제목·설명 스타일
  - 상품 상세페이지: 어조, 필수로 넣는 정보
"""


SYNTHESIS_SYS = """인터뷰 내용을 종합해 브랜드 보이스 문서를 마크다운으로 작성하라.

아래 구조를 정확히 지킨다.

# Brand Voice & Content Policy

## 1. Identity & Audience
- **브랜드/사람 이름**:
- **핵심 메시지**:
- **타깃 독자**:

## 2. Tone & Style Rules
- **말투**:
- **금지사항**:
- **강조사항**:

## 3. Platform Tailoring Rules
- **인스타그램**:
- **네이버 블로그/카페**:
- **유튜브**:
- **상품 상세페이지**:

규칙:
- 인터뷰에서 실제로 나온 말과 일화만 쓴다. 없는 내용을 지어내지 않는다.
- "신뢰감 있는", "친근한", "정성스러운" 같이 어느 브랜드에나 붙는 표현은 금지한다.
- 금지사항에는 실제 문구를 그대로 적는다. (예: "최고", "100% 보장")
- 채널별 규칙에는 길이·해시태그 개수 같은 구체적 수치를 넣는다.
- 인터뷰에서 안 나온 항목은 지어내지 말고 "(미정)"이라고 적는다.
- 마크다운 본문만 출력한다. 인사말이나 설명은 붙이지 않는다."""


TEMPLATE_MD = """# Brand Voice & Content Policy

## 1. Identity & Audience
- **브랜드/사람 이름**: (누가 말하는가)
- **핵심 메시지**: (한 문장으로)
- **타깃 독자**: (나이·상황·고민까지 구체적으로)

## 2. Tone & Style Rules
- **말투**: 
- **금지사항**: (예: "최고", "100% 보장", "만병통치")
- **강조사항**: 

## 3. Platform Tailoring Rules
- **인스타그램**: 짧은 캡션 + 해시태그 5~8개, 사진 중심
- **네이버 블로그/카페**: 800자 이상, 사진 사이사이 설명
- **유튜브**: 제목 30자 내외, 설명란에 구매 링크
- **상품 상세페이지**: 가격·사양 명시, 과장 표현 없이
"""


def _voice_llm(max_tokens=2000):
    from langchain_anthropic import ChatAnthropic

    return ChatAnthropic(
        model=os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-5"),
        max_tokens=max_tokens,
    )


def _to_text(res):
    c = res.content
    if isinstance(c, list):
        return "".join(p.get("text", "") if isinstance(p, dict) else str(p) for p in c)
    return str(c)


with tab6:
    st.subheader("🎙️ 브랜드 보이스 인터뷰")

    current = db_manager.get_brand_voice() if db_manager.is_configured() else ""
    if current:
        st.success("활성 브랜드 보이스가 적용 중입니다. 원고 작성·검수에 자동 반영됩니다.")
        with st.expander("현재 문서 보기"):
            st.markdown(current)
    else:
        st.info("아직 브랜드 보이스가 없습니다. 인터뷰를 진행하면 모든 원고에 반영됩니다.")

    st.divider()

    if "voice_msgs" not in st.session_state:
        st.session_state["voice_msgs"] = []

    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("🆕 인터뷰 시작 / 다시 시작", use_container_width=True):
            st.session_state["voice_msgs"] = []
            st.session_state["voice_draft"] = ""
            try:
                first = _to_text(
                    _voice_llm(800).invoke(
                        [("system", INTERVIEWER_SYS), ("human", "인터뷰를 시작해줘.")]
                    )
                )
                st.session_state["voice_msgs"] = [{"role": "assistant", "content": first}]
            except Exception as e:
                st.error(f"시작 실패: {e}")
            st.rerun()
    with col_b:
        done = st.button(
            "📄 문서 생성",
            use_container_width=True,
            disabled=len(st.session_state["voice_msgs"]) < 4,
        )

    if st.button("📝 인터뷰 없이 템플릿으로 직접 작성", use_container_width=True):
        st.session_state["voice_draft"] = TEMPLATE_MD
        st.rerun()

    for m in st.session_state["voice_msgs"]:
        with st.chat_message(m["role"]):
            st.markdown(m["content"])

    if st.session_state["voice_msgs"]:
        reply = st.chat_input("답변을 입력하세요")
        if reply:
            st.session_state["voice_msgs"].append({"role": "user", "content": reply})
            history = [("system", INTERVIEWER_SYS)] + [
                ("human" if m["role"] == "user" else "ai", m["content"])
                for m in st.session_state["voice_msgs"]
            ]
            try:
                nxt = _to_text(_voice_llm(800).invoke(history))
                st.session_state["voice_msgs"].append({"role": "assistant", "content": nxt})
            except Exception as e:
                st.error(f"응답 실패: {e}")
            st.rerun()

    if done:
        transcript = "\n\n".join(
            f"{'답변' if m['role']=='user' else '질문'}: {m['content']}"
            for m in st.session_state["voice_msgs"]
        )
        with st.spinner("문서 작성 중..."):
            try:
                st.session_state["voice_draft"] = _to_text(
                    _voice_llm(3000).invoke(
                        [("system", SYNTHESIS_SYS), ("human", transcript)]
                    )
                )
            except Exception as e:
                st.error(f"문서 생성 실패: {e}")

    draft = st.session_state.get("voice_draft", "")
    if draft:
        st.divider()
        st.markdown("### 📄 brand_voice.md")
        edited = st.text_area("검토 후 수정하세요", value=draft, height=420, key="voice_edit")
        c1, c2 = st.columns(2)
        with c1:
            st.download_button(
                "⬇️ brand_voice.md 내려받기",
                data=edited,
                file_name="brand_voice.md",
                mime="text/markdown",
                use_container_width=True,
            )
        with c2:
            if st.button("✅ 적용 (모든 원고에 반영)", type="primary", use_container_width=True):
                try:
                    db_manager.save_brand_voice(edited, note="인터뷰 생성")
                    st.session_state["voice_draft"] = ""
                    st.success("적용 완료. 이제부터 생성되는 원고에 반영됩니다.")
                    st.rerun()
                except Exception as e:
                    st.error(f"저장 실패: {e}")

    hist = db_manager.list_brand_voice_history() if db_manager.is_configured() else []
    if hist:
        with st.expander(f"버전 이력 ({len(hist)}건)"):
            for h in hist:
                mark = "🟢 활성" if h.get("is_active") else "⚪ 이력"
                st.caption(f"{mark} · {str(h.get('created_at',''))[:16]} · {h.get('note') or '-'}")


# --------------------------------------------------- TAB 7: 주간 플랜

import datetime as _dt
import zoneinfo as _zi

KST = _zi.ZoneInfo("Asia/Seoul")

PLAN_SYS = """너는 소셜 미디어 콘텐츠 기획자다.
브랜드 보이스 문서를 근거로 이번 주에 다룰 핵심 주제 3가지를 제안한다.

규칙:
- 브랜드 보이스에 적힌 타깃 독자와 핵심 메시지에서 출발한다. 일반론을 쓰지 마라.
- 세 주제는 성격이 서로 달라야 한다(예: 정보형 / 신뢰형 / 전환형).
- 각 주제마다 왜 이번 주에 이걸 다뤄야 하는지 한 문장으로 근거를 붙인다.
- 브랜드 보이스가 비어 있으면 그 사실을 먼저 알리고, 일반적인 제안임을 명시한다.
- 반드시 JSON만 출력한다.

출력 형식:
{"topics":[{"title":"...","angle":"정보형","why":"...","channels":["인스타그램","네이버 블로그/카페"]}]}"""


with tab7:
    st.subheader("🗓️ 이번 주 콘텐츠 플랜")

    if not db_manager.is_configured():
        st.info("Supabase 설정이 필요합니다.")
    else:
        voice = db_manager.get_brand_voice()
        if not voice:
            st.warning(
                "브랜드 보이스가 비어 있습니다. 🎙️ 탭에서 먼저 만들면 제안 품질이 크게 달라집니다."
            )

        if st.button("💡 이번 주 주제 3가지 제안받기", type="primary"):
            with st.spinner("기획 중..."):
                try:
                    raw = _to_text(
                        _voice_llm(1500).invoke(
                            [
                                ("system", PLAN_SYS),
                                (
                                    "human",
                                    f"브랜드 보이스:\n{voice or '(없음)'}\n\n"
                                    f"오늘: {_dt.datetime.now(KST):%Y-%m-%d (%a)}",
                                ),
                            ]
                        )
                    )
                    import json as _j
                    import re as _re

                    cleaned = _re.sub(r"^```(?:json)?|```$", "", raw.strip(), flags=_re.M).strip()
                    st.session_state["plan_topics"] = _j.loads(cleaned).get("topics", [])
                except Exception as e:
                    st.error(f"제안 실패: {e}")

        topics = st.session_state.get("plan_topics", [])
        if topics:
            st.divider()
            st.markdown("### 주제를 고르고 예약 시각을 정하세요")

            today = _dt.datetime.now(KST).date()
            for i, t in enumerate(topics):
                with st.container(border=True):
                    st.markdown(f"**{i+1}. {t.get('title','')}**  ·  `{t.get('angle','')}`")
                    st.caption(t.get("why", ""))

                    chans = st.multiselect(
                        "채널",
                        ["인스타그램", "네이버 블로그/카페", "유튜브", "상품 상세페이지", "스레드(Threads)"],
                        default=t.get("channels") or ["인스타그램"],
                        key=f"pc_{i}",
                    )
                    c1, c2 = st.columns(2)
                    with c1:
                        d = st.date_input(
                            "발행일", value=today + _dt.timedelta(days=i + 1), key=f"pd_{i}"
                        )
                    with c2:
                        tm = st.time_input("시각 (KST)", value=_dt.time(8, 0), key=f"pt_{i}")

                    if st.button("🚀 생성 + 예약", key=f"pb_{i}", use_container_width=True):
                        when = _dt.datetime.combine(d, tm, tzinfo=KST)
                        if when <= _dt.datetime.now(KST):
                            st.error("예약 시각이 이미 지났습니다.")
                        elif not chans:
                            st.error("채널을 하나 이상 고르세요.")
                        else:
                            threading.Thread(
                                target=_plan_worker,
                                args=(t.get("title", ""), chans, when.isoformat()),
                                daemon=False,
                            ).start()
                            st.success(
                                f"백그라운드 생성 시작 · {when:%m/%d %H:%M} 발행 예약. "
                                "완료되면 아래 목록에 나타납니다."
                            )

        st.divider()
        st.markdown("### 📌 예약 현황")
        if st.button("새로고침", key="sched_refresh"):
            st.rerun()

        rows = db_manager.list_scheduled()
        if not rows:
            st.caption("예약된 콘텐츠가 없습니다.")
        for r in rows:
            icon = {"scheduled": "🕐", "published": "✅", "failed": "❌"}.get(
                r.get("publish_status"), "•"
            )
            when = str(r.get("scheduled_at") or "")[:16].replace("T", " ")
            cols = st.columns([5, 1])
            cols[0].write(
                f"{icon} **{r.get('topic','')}** · {when} · {r.get('target_channel') or '-'}"
            )
            if r.get("publish_error"):
                cols[0].caption(f"오류: {r['publish_error'][:120]}")
            if r.get("publish_status") == "scheduled":
                if cols[1].button("취소", key=f"cx_{r['generation_id']}"):
                    db_manager.cancel_schedule(r["generation_id"])
                    st.rerun()


# --------------------------------------------------- TAB 8: 학습 현황

MINER_SYS = """너는 AI 원고와 사람이 고친 최종본을 비교해 '무엇을 틀렸는지' 규칙으로 뽑아내는 분석가다.

규칙:
- 여러 사례에서 반복되는 패턴만 뽑는다. 한 번뿐인 수정은 무시한다.
- 각 규칙은 다음 생성 때 그대로 지킬 수 있을 만큼 구체적이어야 한다.
  나쁨: "더 자연스럽게 쓸 것"
  좋음: "느낌표는 문장당 최대 1회. 3회 이상 쓰면 매번 삭제됨"
- 무엇을 하지 말라만 쓰지 말고, 대신 무엇을 하라도 함께 적는다.
- 근거 없는 추측을 하지 마라. 실제 수정에서 관찰된 것만 쓴다.
- 규칙은 최대 6개. 적어도 된다.
- 반드시 JSON만 출력한다.

category 는 다음 중 하나: 표현 | 길이 | 구조 | 어조 | 정보 | 기타

출력 형식:
{"rules":[{"rule_text":"...","category":"표현"}]}"""


with tab8:
    st.subheader("🧠 학습 현황")

    if not db_manager.is_configured():
        st.info("Supabase 설정이 필요합니다.")
    else:
        # ── 성장 지표
        series = db_manager.edit_distance_series()
        st.markdown("#### 수정률 추이")
        st.caption("사람이 고친 비율. **이 선이 내려가야 학습이 되고 있는 것**입니다.")

        if len(series) < 2:
            st.info(
                f"승인 데이터 {len(series)}건. 2건 이상 쌓이면 그래프가 나옵니다."
            )
        else:
            vals = [float(s.get("edit_distance") or 0) for s in series]
            st.line_chart({"수정률": vals})
            half = max(1, len(vals) // 2)
            early, late = sum(vals[:half]) / half, sum(vals[half:]) / len(vals[half:])
            delta = early - late
            c1, c2, c3 = st.columns(3)
            c1.metric("최근 수정률", f"{vals[-1]*100:.0f}%")
            c2.metric("초기 평균", f"{early*100:.0f}%")
            c3.metric("후반 평균", f"{late*100:.0f}%", delta=f"{-delta*100:.0f}%p")
            if delta > 0.03:
                st.success("수정률이 내려가고 있습니다. 학습이 작동 중입니다.")
            elif len(vals) >= 6:
                st.warning(
                    "아직 내려가지 않았습니다. 규칙이 실제 문제를 짚고 있는지 아래에서 확인하세요."
                )

        st.divider()

        # ── 오답 분석
        pending = db_manager.list_unmined_edits()
        st.markdown("#### 오답 분석")
        st.caption("AI 원고와 사람 수정본의 차이에서 규칙을 뽑아냅니다.")
        st.write(f"분석 대기: **{len(pending)}건**")

        if st.button(
            "🔍 지금 분석해서 규칙 뽑기",
            type="primary",
            disabled=len(pending) < 2,
            help="수정이 있었던 승인 건이 2건 이상이면 실행할 수 있습니다.",
        ):
            with st.spinner("수정 이력 분석 중..."):
                try:
                    blocks = []
                    for p in pending[:10]:
                        blocks.append(
                            f"[사례 {p['generation_id'][:8]}] 주제: {p.get('topic','')}\n"
                            f"--- AI 원고 ---\n{(p.get('ai_raw_output') or '')[:1500]}\n"
                            f"--- 사람 수정본 ---\n{(p.get('human_modified_output') or '')[:1500]}"
                        )
                    raw = _to_text(
                        _voice_llm(2000).invoke(
                            [("system", MINER_SYS), ("human", "\n\n".join(blocks))]
                        )
                    )
                    import json as _j
                    import re as _re

                    cleaned = _re.sub(r"^```(?:json)?|```$", "", raw.strip(), flags=_re.M).strip()
                    rules = _j.loads(cleaned).get("rules", [])
                    ids = [p["generation_id"] for p in pending[:10]]
                    for r in rules:
                        db_manager.upsert_rule(
                            r.get("rule_text", ""), r.get("category", "기타"), ids
                        )
                    db_manager.mark_mined(ids)
                    st.success(f"{len(rules)}개 규칙 추출 완료 · {len(ids)}건 분석 처리")
                    st.rerun()
                except Exception as e:
                    st.error(f"분석 실패: {e}")

        st.divider()

        # ── 검수자 정확도
        st.markdown("#### 검수자 정확도")
        st.caption("AI 채점이 내 기준과 맞는지. 어긋나면 검수 프롬프트가 스스로 보정합니다.")

        cal = db_manager.calibration_stats()
        agree = db_manager.critic_agreement()

        if not cal.get("n"):
            st.info("승인 화면에서 '내 채점'을 매기면 여기에 편차가 쌓입니다.")
        else:
            k1, k2, k3 = st.columns(3)
            k1.metric("채점 건수", cal["n"])
            k2.metric(
                "최근 편차",
                f"{cal['recent_gap']:+.0f}점",
                help="양수면 AI가 후하게, 음수면 짜게 매기는 중",
            )
            k3.metric("평균 오차", f"{cal['abs_gap']:.0f}점")

            if cal["n"] >= 3 and abs(cal["recent_gap"]) >= 5:
                direction = "후하게" if cal["recent_gap"] > 0 else "짜게"
                st.warning(
                    f"AI가 평균 {abs(cal['recent_gap']):.0f}점 {direction} 매기고 있습니다. "
                    "다음 검수부터 자동 보정이 들어갑니다."
                )
            elif cal["n"] >= 3:
                st.success("AI 채점이 내 기준과 5점 이내로 맞고 있습니다.")

            if cal.get("rows") and len(cal["rows"]) >= 2:
                st.line_chart(
                    {
                        "AI": [float(r.get("creativity_score") or 0) for r in cal["rows"]],
                        "나": [float(r.get("human_score") or 0) for r in cal["rows"]],
                    }
                )

        if agree.get("n"):
            st.metric(
                "우리끼리 대결 일치율",
                f"{agree['rate']*100:.0f}%",
                help=f"AI가 더 높게 채점한 쪽을 나도 고른 비율 ({agree['agree']}/{agree['n']})",
            )

        st.divider()

        # ── 누적 규칙
        rules = db_manager.list_rules()
        active_n = sum(1 for r in rules if r.get("is_active"))
        st.markdown(f"#### 누적 규칙 ({len(rules)}개 · 적용 중 {active_n}개)")
        st.caption(
            f"{db_manager.AUTO_ACTIVATE_HITS}회 이상 관찰된 규칙만 자동 적용됩니다. "
            "그 미만은 직접 확인 후 켜세요."
        )

        if not rules:
            st.info("아직 규칙이 없습니다. 승인 몇 건 쌓고 위에서 분석을 돌려보세요.")

        for r in rules:
            with st.container(border=True):
                c1, c2, c3 = st.columns([6, 1, 1])
                mark = "🟢" if r.get("is_active") else "⚪"
                c1.markdown(f"{mark} {r.get('rule_text','')}")
                c1.caption(f"`{r.get('category','-')}` · {r.get('hit_count',1)}회 관찰")
                label = "끄기" if r.get("is_active") else "켜기"
                if c2.button(label, key=f"tg_{r['rule_id']}", use_container_width=True):
                    db_manager.set_rule_active(r["rule_id"], not r.get("is_active"))
                    st.rerun()
                if c3.button("삭제", key=f"dr_{r['rule_id']}", use_container_width=True):
                    db_manager.delete_rule(r["rule_id"])
                    st.rerun()


# --------------------------------------------------- TAB 9: 대결 평가

DELIVERABLES = {
    "홈페이지 구성안": 3_000_000,
    "상세페이지 원고": 500_000,
    "블로그 원고 1건": 150_000,
    "인스타 카드뉴스 1세트": 200_000,
    "유튜브 대본 1편": 300_000,
    "직접 입력": 0,
}

with tab9:
    st.subheader("⚔️ 대결 평가")
    st.caption(
        "우리 결과물과 실제 전문가 결과물을 **어느 쪽인지 모르는 상태로** 비교합니다. "
        "지표는 점수가 아니라 **승률**입니다."
    )

    if not db_manager.is_configured():
        st.info("Supabase 설정이 필요합니다.")
    else:
        stats = db_manager.duel_stats()
        if stats.get("n"):
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("전체 승률", f"{stats['win_rate']*100:.0f}%", help=f"{stats['wins']}/{stats['n']}")
            c2.metric("최근 10전", f"{stats['recent_win_rate']*100:.0f}%")
            c3.metric("우리 지불의사", f"{stats['pay_rate_ours']*100:.0f}%")
            c4.metric("전문가 지불의사", f"{stats['pay_rate_ref']*100:.0f}%")

            if stats["n"] >= 10:
                if stats["win_rate"] >= 0.5:
                    st.success("전문가 결과물과 대등하거나 앞서고 있습니다.")
                else:
                    st.warning(
                        "아직 전문가 쪽이 우세합니다. 진 판의 메모를 모아 규칙으로 만드세요."
                    )
            else:
                st.info(f"{stats['n']}전 기록. 10전은 넘어야 승률이 의미를 가집니다.")
        else:
            st.info("아직 대결 기록이 없습니다. 아래에서 첫 대결을 등록하세요.")

        st.divider()

        # ── 대결 등록
        with st.expander("➕ 새 대결 등록", expanded=not stats.get("n")):
            st.caption(
                "전문가 결과물은 **비교 전용**으로만 보관되며 생성 프롬프트에 절대 들어가지 않습니다."
            )
            d_topic = st.text_input("주제", key="d_topic")
            d_kind = st.selectbox("결과물 종류", list(DELIVERABLES.keys()), key="d_kind")
            d_price = st.number_input(
                "시장가 기준 (원)",
                min_value=0,
                step=50_000,
                value=DELIVERABLES.get(d_kind, 0),
                key="d_price",
                help="이 금액을 주고 전문가에게 맡겼을 때 받을 결과물이 비교 대상입니다.",
            )
            colx, coly = st.columns(2)
            with colx:
                d_ours = st.text_area("우리 결과물", height=220, key="d_ours")
            with coly:
                d_ref = st.text_area("전문가 결과물", height=220, key="d_ref")
            d_src = st.text_input("전문가 결과물 출처 (URL 등)", key="d_src")

            if st.button("대결 등록", type="primary", disabled=not (d_ours.strip() and d_ref.strip())):
                try:
                    db_manager.create_duel(
                        topic=d_topic,
                        ours_text=d_ours,
                        ref_text=d_ref,
                        ref_source=d_src,
                        deliverable=d_kind,
                        price_standard=float(d_price) or None,
                    )
                    st.success("등록 완료. 아래에서 블라인드로 판정하세요.")
                    st.rerun()
                except Exception as e:
                    st.error(f"등록 실패: {e}")

        # ── 우리끼리 대결 (검수자 검증)
        with st.expander("🔁 우리 결과물끼리 대결 (검수자 채점 검증)"):
            st.caption(
                "AI가 더 높게 채점한 쪽을 나도 고르는지 봅니다. "
                "자주 어긋나면 검수 기준 자체가 틀린 것입니다."
            )
            cands = db_manager.list_internal_duel_candidates()
            if len(cands) < 2:
                st.info("결과물이 2건 이상 있어야 합니다.")
            else:
                labels = {
                    f"{c.get('topic','(제목없음)')[:30]} · AI {float(c.get('creativity_score') or 0):.0f}점": c
                    for c in cands
                }
                keys = list(labels.keys())
                pick_a = st.selectbox("첫 번째", keys, index=0, key="ia")
                pick_b = st.selectbox("두 번째", keys, index=min(1, len(keys) - 1), key="ib")
                if st.button("대결 붙이기", disabled=pick_a == pick_b):
                    try:
                        db_manager.create_internal_duel(labels[pick_a], labels[pick_b])
                        st.success("등록 완료")
                        st.rerun()
                    except Exception as e:
                        st.error(f"실패: {e}")

        # ── 블라인드 판정
        pend = db_manager.list_pending_duels()
        st.markdown(f"### 판정 대기 ({len(pend)}건)")

        for d in pend:
            ours_a = d.get("ours_is_a")
            text_a = d["ours_text"] if ours_a else d["ref_text"]
            text_b = d["ref_text"] if ours_a else d["ours_text"]
            price = d.get("price_standard")
            price_txt = f"{int(price):,}원" if price else "미설정"

            with st.container(border=True):
                st.markdown(f"**{d.get('topic') or '(주제 없음)'}**")
                st.caption(f"{d.get('deliverable') or '-'} · 시장가 기준 {price_txt}")

                ca, cb = st.columns(2)
                with ca:
                    st.markdown("#### A")
                    st.text_area("A", value=text_a, height=260, key=f"ta_{d['duel_id']}",
                                 disabled=True, label_visibility="collapsed")
                with cb:
                    st.markdown("#### B")
                    st.text_area("B", value=text_b, height=260, key=f"tb_{d['duel_id']}",
                                 disabled=True, label_visibility="collapsed")

                st.markdown(f"**{price_txt}을 지불한다면 어느 쪽을 사겠습니까?**")
                pa = st.checkbox(f"A에 {price_txt} 지불할 만하다", key=f"pa_{d['duel_id']}")
                pb = st.checkbox(f"B에 {price_txt} 지불할 만하다", key=f"pb_{d['duel_id']}")
                note = st.text_input(
                    "이유 (진 판의 이유가 다음 규칙이 됩니다)", key=f"nt_{d['duel_id']}"
                )

                b1, b2 = st.columns(2)
                is_internal = d.get("kind") == "internal"

                def _decide(picked_ours: bool):
                    # internal 대결에서 'ours' 는 AI 가 더 높게 채점한 쪽이다
                    agreed = picked_ours if is_internal else None
                    db_manager.decide_duel(
                        d["duel_id"],
                        "ours" if picked_ours else "ref",
                        would_pay_ours=(pa if ours_a else pb),
                        would_pay_ref=(pb if ours_a else pa),
                        note=note,
                        ai_agreed=agreed,
                    )

                if b1.button("◀ A 선택", key=f"wa_{d['duel_id']}", use_container_width=True):
                    _decide(bool(ours_a))
                    st.rerun()
                if b2.button("B 선택 ▶", key=f"wb_{d['duel_id']}", use_container_width=True):
                    _decide(not ours_a)
                    st.rerun()
