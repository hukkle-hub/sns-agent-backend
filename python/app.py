
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
def run_agent_background(youtube_url: str, new_topic: str) -> None:
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
            }
        )

        supabase.table("content_generations").update(
            {
                "ai_raw_output": result.get("draft", "생성 실패"),
                "ai_self_score": result.get("score", 0.0),
                "revision_count": result.get("revision_count", 0),
                "approval_status": "pending",
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


# ---------------------------------------------------------------------------
# 🗂️ 탭
# ---------------------------------------------------------------------------
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    [
        "🌐 SNS 에이전트 앱",
        "🚀 신규 에이전트 가동",
        "📥 승인 대기 관제탑",
        "📊 AI 학습 데이터셋",
        "⚙️ 서비스 직접 등록",
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
            "💡 적용할 신규 주제", placeholder="예: 고흥 유자청 겨울 선물세트"
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
                args=(youtube_url, new_topic),
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
        with st.expander(f"📌 [{score_txt}] {topic}", expanded=True):
            col1, col2 = st.columns([3, 1])
            with col1:
                edited_draft = st.text_area(
                    "원고 검수 및 수정",
                    value=ai_draft,
                    height=320,
                    key=f"edit_{content_id}",
                )
            with col2:
                st.markdown("### 🎛️ 발행 조종석")
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
                        update_human_approval(content_id, edited_draft, "approved")
                        st.success("발행 완료!")
                        st.rerun(scope="app")
                    except Exception as e:
                        st.error(f"발행 실패: {e}")

                if st.button("🚫 반려", key=f"rej_{content_id}"):
                    update_human_approval(content_id, edited_draft, "rejected")
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
            svc_type = st.selectbox("서비스 유형", ["웹훅(Webhook)", "API Key", "SNS 채널 URL"])
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
