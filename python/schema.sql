-- ============================================================
-- SNS 콘텐츠 파이프라인 스키마
-- Supabase 대시보드 → SQL Editor 에 붙여넣고 Run
-- ============================================================

-- 1. 콘텐츠 생성 및 자가검수/정답지 저장 테이블
CREATE TABLE IF NOT EXISTS content_generations (
    generation_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
    topic TEXT NOT NULL,
    ai_raw_output TEXT NOT NULL,
    ai_self_score NUMERIC,
    revision_count INTEGER DEFAULT 0,
    human_modified_output TEXT,
    approval_status TEXT DEFAULT 'pending' -- 'processing', 'pending', 'approved', 'rejected', 'failed'
);

-- 2. 태블릿에서 직접 등록하는 외부 연동 서비스 테이블
CREATE TABLE IF NOT EXISTS registered_services (
    service_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    service_name TEXT NOT NULL,
    service_type TEXT NOT NULL, -- '웹훅(Webhook)', 'API Key', 'SNS 채널 URL'
    endpoint_url TEXT,
    api_key TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- 3. 벤치마킹 수집 데이터 테이블
CREATE TABLE IF NOT EXISTS benchmarks (
    benchmark_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    target_platform TEXT,
    source_url TEXT,
    raw_script TEXT,
    extracted_hooks JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- ------------------------------------------------------------
-- 조회 성능용 인덱스
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_content_generations_status
    ON content_generations (approval_status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_registered_services_active
    ON registered_services (is_active);
CREATE INDEX IF NOT EXISTS idx_benchmarks_platform
    ON benchmarks (target_platform, created_at DESC);

-- ------------------------------------------------------------
-- 보안: registered_services 에는 API 키 원문이 들어갑니다.
-- RLS 를 켜지 않으면 anon 키를 가진 누구나 읽어갈 수 있습니다.
-- 서버(service_role 키)에서만 접근한다면 아래를 반드시 실행하세요.
-- ------------------------------------------------------------
ALTER TABLE registered_services ENABLE ROW LEVEL SECURITY;
ALTER TABLE content_generations ENABLE ROW LEVEL SECURITY;
ALTER TABLE benchmarks ENABLE ROW LEVEL SECURITY;
-- RLS 를 켜면 anon 키로는 전부 차단됩니다(service_role 키는 우회).
-- 앱에서 anon 키로 접근해야 한다면 여기에 별도 정책을 추가해야 합니다.

-- ============================================================
-- 스레드(Threads) 연동 — 2026-07 추가
-- ============================================================

-- 자동 답글 키워드 설정
CREATE TABLE IF NOT EXISTS threads_auto_replies (
    reply_id        UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    trigger_keyword TEXT NOT NULL,
    reply_content   TEXT NOT NULL,
    is_active       BOOLEAN DEFAULT true,
    hit_count       INTEGER DEFAULT 0,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- 이미 답글을 단 댓글 (중복 답글 방지 — 이게 없으면 감시할 때마다 같은 댓글에 계속 달림)
CREATE TABLE IF NOT EXISTS threads_replied (
    reply_to_id  TEXT PRIMARY KEY,
    media_id     TEXT,
    keyword      TEXT,
    replied_at   TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- 발행 기록 (24시간 250건 한도 추적 + 댓글 감시 대상)
CREATE TABLE IF NOT EXISTS threads_posts (
    id            BIGSERIAL PRIMARY KEY,
    media_id      TEXT UNIQUE NOT NULL,
    generation_id UUID,
    text_preview  TEXT,
    is_chain_head BOOLEAN DEFAULT true,
    watch_replies BOOLEAN DEFAULT true,
    posted_at     TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_threads_posts_watch
  ON threads_posts (watch_replies, posted_at DESC);

ALTER TABLE threads_auto_replies ENABLE ROW LEVEL SECURITY;
ALTER TABLE threads_replied      ENABLE ROW LEVEL SECURITY;
ALTER TABLE threads_posts        ENABLE ROW LEVEL SECURITY;

-- 스레드 체인 원고 보관
ALTER TABLE content_generations
  ADD COLUMN IF NOT EXISTS threads_chain JSONB;
