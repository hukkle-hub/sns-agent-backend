// server.js — SNS 에이전트 플랫폼 확장 백엔드 (v15: 작가 블로그 글쓰기 모드 + 블로그·대본 심화 자율학습)
// 기능: AI 호출 + 총괄 라우팅/부서 협업 + 부서 학습 메모리 + 회의록
//       + 실제 발행(어댑터) + 동기화 + 카카오톡 양방향 + 백그라운드 워커
// 의존성: express, cors (그 외는 Node 18+ 내장 fetch/fs 사용)
//
// 환경변수:
//   ANTHROPIC_API_KEY   (필수) Anthropic API 키
//   MODEL               (선택) 기본 claude-sonnet-4-6
//   ALLOWED_KAKAO_IDS   (선택) 카톡 허용 사용자 id, 콤마구분(보안)
//   DATA_DIR            (선택) 데이터 저장 폴더, 기본 ./data
//   PUBLIC_BASE         (선택) OAuth 콜백용 공개 주소(예: https://앱.onrender.com)
//   -- 카카오 --
//   KAKAO_ACCESS_TOKEN  카카오 "나에게 보내기" 토큰 (또는 아래 refresh 방식)
//   KAKAO_REST_KEY / KAKAO_REFRESH_TOKEN  토큰 자동 갱신용
//   -- 유튜브(YouTube Data API) --
//   GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET / YT_REFRESH_TOKEN
//   (또는 YT_ACCESS_TOKEN 직접 지정)
//   -- 인스타그램(Graph API) --
//   IG_ACCESS_TOKEN / IG_USER_ID
//   -- 블로그(WordPress) --
//   WP_BASE / WP_USER / WP_APP_PASSWORD
//   -- 홈페이지 --
//   SITE_WEBHOOK_URL

import express from "express";
import cors from "cors";
import fs from "fs";
import path from "path";

// .env 자동 로드 (의존성 없이) — start 스크립트가 만든 .env에서 키를 읽음
try {
  const _envPath = new URL("./.env", import.meta.url);
  const _env = fs.readFileSync(_envPath, "utf8");
  _env.split(/\r?\n/).forEach(line=>{
    const m = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$/);
    if (m && !process.env[m[1]]) process.env[m[1]] = m[2].replace(/^["']|["']$/g, "");
  });
} catch(e){ /* .env 없으면 무시 (클라우드는 환경변수 사용) */ }

const app = express();
app.use(cors());
app.use(express.json({ limit: "40mb" }));

const API_KEY = process.env.ANTHROPIC_API_KEY;
const MODEL = process.env.MODEL || "claude-sonnet-4-6";
const KAKAO_TOKEN = process.env.KAKAO_ACCESS_TOKEN || "";
const ALLOWED_KAKAO = (process.env.ALLOWED_KAKAO_IDS || "").split(",").map(s=>s.trim()).filter(Boolean);

// ===== 간단 파일 DB (운영 시 PostgreSQL 등으로 교체) =====
const DATA_DIR = process.env.DATA_DIR || "./data";
const DB_FILE = path.join(DATA_DIR, "db.json");
const SUPA_URL = (process.env.SUPABASE_URL || "").trim().replace(/\/+$/,"");      // 끝 슬래시·공백 제거
const SUPA_KEY = (process.env.SUPABASE_KEY || "").replace(/[^A-Za-z0-9._-]/g,"");  // JWT 허용문자만 남김(보이지 않는 문자·개행·공백 전부 제거 → 헤더 오류 방지)
const SUPA_TABLE = (process.env.SUPABASE_TABLE || "agent_state").trim();
const useSupabase = !!(SUPA_URL && SUPA_KEY);
function emptyDB(){ return { jobs:[], meetings:[], meetingSchedules:[], patches:[], deptMemory:{}, deptKnowledge:{}, clientProfile:{text:"",at:0,basis:0}, clientLog:[], clientCount:0, scheduled:[], pubSchedules:[], approvals:[], contentApprovals:[], collections:[], lastCollectAt:0, usage:{ in:0, out:0, calls:0 }, usageDaily:{ date:"", in:0, out:0, calls:0 }, usageMonthly:{ month:"", in:0, out:0, calls:0, alerted:"" }, geminiUsage:{ in:0, out:0, calls:0 }, geminiDaily:{ date:"", in:0, out:0, calls:0 }, geminiMonthly:{ month:"", in:0, out:0, calls:0, alerted:"", dayAlerted:"" }, geminiSearchDaily:{ date:"", n:0 }, geminiSearchTotal:0, briefings:[], lastBriefDay:"", briefDone:{ morning:"", evening:"", weekly:"" }, leadDirectives:[], leaderDailyDirective:{}, lastLeaderDirectDay:"", dirFeedback:{}, dailyReview:{}, lastReviewDay:"", nightResearch:{}, lastNightResearchDay:"", staleHandled:{}, lastKShareAt:0, lastTrainAt:{}, lastTrainRoundAt:0, growBurst:{active:false,total:0,done:0}, lastDailyGrowthDay:"", capability:{}, capHistory:[], cloudBackup:null, errors:[], retryQueue:[], state:null, exp:{}, learnIdx:0, autoRunDay:{}, projects:[], pubInbox:[], pageJobs:[], learnJobs:[], patchProposals:[], costApprovals:[], guidelineLog:[], appliedLog:[], lessons:[], projectMeetings:[], updatedAt:0 }; }
// Supabase REST: 단일 행(id='main')에 전체 상태를 jsonb로 저장 (의존성 0)
//   테이블 준비(SQL):
//   create table agent_state ( id text primary key, data jsonb, updated_at bigint );
async function supaLoad(){
  const r = await fetch(SUPA_URL+"/rest/v1/"+SUPA_TABLE+"?id=eq.main&select=data", {
    headers:{ apikey:SUPA_KEY, Authorization:"Bearer "+SUPA_KEY }
  });
  if (!r.ok) throw new Error("supaLoad HTTP "+r.status+": "+(await r.text().catch(()=>"")).slice(0,120));
  const rows = await r.json();
  if (!Array.isArray(rows)) throw new Error("supaLoad 예상치 못한 응답");
  return (rows[0] && rows[0].data) ? rows[0].data : null;  // 행 없으면 null(정상), 에러는 throw
}
async function supaSave(data){
  const r = await fetch(SUPA_URL+"/rest/v1/"+SUPA_TABLE+"?on_conflict=id", {
    method:"POST",
    headers:{ apikey:SUPA_KEY, Authorization:"Bearer "+SUPA_KEY, "Content-Type":"application/json", Prefer:"resolution=merge-duplicates" },
    body: JSON.stringify([{ id:"main", data, updated_at:Date.now() }])
  });
  if (!r.ok) throw new Error("supaSave HTTP "+r.status+": "+(await r.text().catch(()=>"")).slice(0,120));
  return true;
}
// 영구저장 라이브 점검: 실제로 테이블을 읽어 연결·권한·테이블 존재를 확인
async function supaProbe(){
  if(!useSupabase) return { ok:false, note:"미설정 — 재배포 시 데이터 유실 가능" };
  try{
    const r = await fetch(SUPA_URL+"/rest/v1/"+SUPA_TABLE+"?id=eq.main&select=id,updated_at", {
      headers:{ apikey:SUPA_KEY, Authorization:"Bearer "+SUPA_KEY }
    });
    if(!r.ok){
      const t = (await r.text()||"").slice(0,120);
      let hint = "";
      if(r.status===401||r.status===403) hint = " (키 권한 확인: service_role 권장)";
      else if(r.status===404) hint = " (테이블 '"+SUPA_TABLE+"' 없음 — SQL로 생성 필요)";
      return { ok:false, note:"연결 실패 HTTP "+r.status+hint+(t?(" · "+t):"") };
    }
    const rows = await r.json();
    if(Array.isArray(rows)){
      const row = rows[0];
      return { ok:true, note: row ? ("읽기/쓰기 정상 · 최종 저장 "+new Date(row.updated_at||0).toLocaleString("ko-KR")) : "테이블 정상 · 아직 저장된 데이터 없음" };
    }
    return { ok:false, note:"예상치 못한 응답 형식" };
  }catch(e){ return { ok:false, note:String(e.message||e).slice(0,120) }; }
}
function loadDBFile(){
  try { return JSON.parse(fs.readFileSync(DB_FILE, "utf8")); }
  catch (e) { return emptyDB(); }
}
// DB가 '의미 있는 데이터'를 담고 있는지 판정 — 빈 데이터로 덮어쓰기 방지에 사용
function dbHasContent(d){
  if(!d || typeof d!=="object") return false;
  const memN = d.deptMemory ? Object.values(d.deptMemory).reduce((a,arr)=>a+((arr&&arr.length)||0),0) : 0;
  const expSum = d.exp ? Object.values(d.exp).reduce((a,v)=>a+(+v||0),0) : 0;
  const knowN = d.deptKnowledge ? Object.keys(d.deptKnowledge).length : 0;
  const jobN = (d.jobs&&d.jobs.length)||0;
  const meetN = (d.meetings&&d.meetings.length)||0;
  // 실제 축적 판정: 초기 가동 기록(부서×소수)·초기 exp를 확실히 넘는 값 + 지식/발행/회의는 무조건 콘텐츠
  return (memN > 40) || (expSum > 60) || (knowN > 0) || (jobN > 0) || (meetN > 0);
}
let _bootRestoreOk = false;   // 시작 시 Supabase 복원이 성공했는지
let _lastGoodSavedContent = false; // 마지막으로 '내용 있는' 상태를 저장한 적 있는지
let _bootRestoreNote = "";
let _needInitialSave = false;
async function loadDB(){
  let d;
  if (useSupabase) {
    // 핵심 구분: 'Supabase 접속 성공'과 '저장된 데이터 존재'는 다른 문제.
    //   접속 성공 + 행 없음 = 정상(첫 저장 시 행 생성) → 저장 허용
    //   접속 실패            = 위험(잠들었거나 장애)      → 저장 보류(빈 데이터로 덮어쓰기 방지)
    let loaded = null, reached = false, lastErr = null;
    for (let attempt=0; attempt<3 && !reached; attempt++){
      try {
        if (attempt>0) await new Promise(r=>setTimeout(r, 3000)); // Supabase가 깨어날 시간
        loaded = await supaLoad();   // 행 없으면 null 반환(에러 아님), 접속 실패면 throw
        reached = true;
      } catch(e){ lastErr = e; console.error("supaLoad 시도 "+(attempt+1)+" 실패:", e.message||e); }
    }
    _bootRestoreOk = reached;   // 접속만 되면 저장 허용
    if (loaded != null) {
      d = loaded; _bootRestoreNote = "✅ Supabase에서 복원됨";
      console.log("Supabase 복원 성공");
    } else {
      const fileDB = loadDBFile();
      if (reached) {
        if (dbHasContent(fileDB)) { d = fileDB; _bootRestoreNote = "✅ Supabase 연결됨(저장된 행 없음) → 로컬 파일 내용으로 시작, 곧 Supabase에 저장"; }
        else { d = emptyDB(); _bootRestoreNote = "✅ Supabase 연결됨(첫 실행) → 새 DB로 시작, 곧 Supabase에 첫 저장"; }
        _needInitialSave = true;   // 행을 만들어 두어 다음 부팅부터 복원되게
        console.log("Supabase 연결됨 — 저장된 행 없음(첫 저장 시 생성)");
      } else {
        if (dbHasContent(fileDB)) { d = fileDB; _bootRestoreNote = "⚠️ Supabase 접속 실패 → 로컬 파일 백업 사용(저장 보류)"; }
        else { d = emptyDB(); _bootRestoreNote = "❌ Supabase 접속 실패 & 로컬 백업 없음 → 빈 DB(저장 보류 모드)"; }
        console.warn("⚠️ Supabase 접속 실패: "+((lastErr&&lastErr.message)||"원인 미상"));
      }
    }
  } else {
    d = loadDBFile(); _bootRestoreNote = "파일 모드(Supabase 미설정)";
  }
  // 자가 보완: 버전 변경으로 생긴 누락 키를 기본값으로 채움
  const tmpl = emptyDB();
  for (const k of Object.keys(tmpl)){ if (d[k] === undefined) d[k] = tmpl[k]; }
  if (dbHasContent(d)) _lastGoodSavedContent = true;
  return d;
}
let _lastSaveOkAt = 0, _lastSaveErr = "", _saveFailStreak = 0, _saveAlertedAt = 0;
function noteSaveOk(){ _lastSaveOkAt = Date.now(); _lastSaveErr = ""; _saveFailStreak = 0; }
function noteSaveFail(e){
  _lastSaveErr = String((e&&e.message)||e).slice(0,200);
  _saveFailStreak++;
  console.error("supaSave 실패("+_saveFailStreak+"회):", _lastSaveErr);
  // 조용히 실패하면 데이터가 안 쌓이는 걸 모른다 → 3회 연속 실패 시 1시간에 한 번 알림
  if (_saveFailStreak >= 3 && (Date.now() - _saveAlertedAt > 3600000)){
    _saveAlertedAt = Date.now();
    const hint = /42501|row-level security/i.test(_lastSaveErr)
      ? "\n\n원인: Supabase RLS 정책이 쓰기를 막고 있어요.\n해결: Render 환경변수 SUPABASE_KEY 를 Supabase의 service_role 키로 바꾸세요."
      : "";
    kakaoNotify("🚨 데이터 저장 실패가 계속되고 있어요 ("+_saveFailStreak+"회)\n"+_lastSaveErr+hint).catch(()=>{});
  }
}
let _saveTimer = null;
function saveDB(){
  DB.updatedAt = Date.now();
  // 항상 로컬 파일에도 저장(Supabase 실패 대비 2차 백업)
  try { fs.mkdirSync(DATA_DIR, { recursive:true }); fs.writeFileSync(DB_FILE, JSON.stringify(DB, null, 2)); }
  catch (e) { /* 파일 저장 실패는 무시(디스크 없을 수 있음) */ }
  if (useSupabase) {
    // 안전장치: 시작 시 복원이 실패했고(=빈 상태 의심) 현재 DB에 내용이 없으면 Supabase 덮어쓰기 금지
    if (!_bootRestoreOk && !dbHasContent(DB)) {
      // 좋은 데이터를 빈 데이터로 덮어쓰는 사고 방지 — 저장 보류
      return;
    }
    // 한때 내용이 있었는데 지금 비었으면(비정상 소실 의심) 덮어쓰기 금지
    if (_lastGoodSavedContent && !dbHasContent(DB)) {
      console.warn("⚠️ 내용 없는 상태 저장 시도 차단(데이터 보호)");
      return;
    }
    if (dbHasContent(DB)) _lastGoodSavedContent = true;
    // 잦은 호출을 모아 1.5초 디바운스로 업서트
    if (_saveTimer) clearTimeout(_saveTimer);
    _saveTimer = setTimeout(()=>{ supaSave(DB).then(noteSaveOk).catch(noteSaveFail); }, 1500);
  }
}
let DB = emptyDB();

// ===== 부서 정의 =====
const AGENTS = {
  strategy:    { no:"글2", kr:"이야기팀·카피라이터",  role:"이야기팀 소속 카피라이터. SNS 캡션·후킹 문구·해시태그 전략을 담당한다. 첫 3초 후킹과 저장·공유를 부르는 한 줄을 만든다." },
  creation:    { no:"글1", kr:"이야기팀·콘텐츠 디렉터",    role:"이야기팀 팀장(콘텐츠 디렉터). 글·SNS 콘텐츠의 주제·톤·구성을 총괄하고 팀원(카피라이터·블로그 작가)을 조율하며 최종 검수까지 책임진다. 게시물 카피·본문·구성안을 직접 끝까지 완성한다." },
  publishing:  { no:"필1", kr:"필름팀·영상 PD",      role:"필름팀 팀장(영상 PD). 유튜브·쇼츠 영상의 기획·구성·연출을 총괄한다. 장면·컷·자막·나레이션 구성안과 비주얼 훅을 설계하고 팀원(대본 작가·편집)을 조율한다." },
  analytics:   { no:"디2", kr:"디자인팀·웹 디자이너",    role:"디자인팀 소속 웹 디자이너. 홈페이지·상품 상세페이지·티스토리 스킨 등 웹 화면을 설계·제작한다. 레이아웃·여백·구조·반응형을 담당한다." },
  monetization:{ no:"디1", kr:"디자인팀·아트 디렉터",  role:"디자인팀 팀장(아트 디렉터). 색·무드·컨셉 등 비주얼 방향을 총괄하고 웹·그래픽(배너·카드뉴스·썸네일 이미지) 전반을 지휘한다. 판매 전환 상세페이지 구성·구매 전환 카피(혜택·후기·CTA)에도 강하다." },
  ops:         { no:"총괄", kr:"총괄실·비서", role:"총괄 비서(오케스트레이터). 스케줄·예약 관리, 팀 배정·명령 하달, 플랫폼·계정·발행·카카오 알림, 성과 취합을 담당한다. 저작권·광고법·정책 관점에서 모든 팀의 산출물을 검토하고 직접 수정·보완·재작성할 권한이 있다. 클로드(텍스트·추론)와 제미나이(영상·이미지)를 함께 활용해 전반을 조율한다." },
  advisory:    { no:"글3", kr:"이야기팀·블로그 작가",      role:"이야기팀 소속 블로그 작가. SEO 최적화 블로그·정보성 장문 콘텐츠를 작성한다. 발행 전 품질 검수(과대·허위광고 표현, 브랜드 톤 이탈, 오탈자, 정책 위반)를 PASS/FAIL로 판정하고 개선 지시를 남기는 게이트키퍼 역할도 겸한다." },
  scout:       { no:"필2", kr:"필름팀·대본 작가",      role:"필름팀 소속 대본 작가. 영상 나레이션·자막·스크립트를 쓰고, 트렌드·밈·키워드를 우리 특산물 영상에 바로 쓸 실전 소재(아이디어·후킹 대사·릴스 포맷·시즌 앵글)로 가공한다." },
  editing:     { no:"필3", kr:"필름팀·편집·썸네일",  role:"필름팀 소속 편집·썸네일 담당. 컷 편집·자막·트랜지션·BGM 리듬을 설계하고, 클릭률(CTR)을 끌어올리는 썸네일(구도·표정·텍스트·대비)을 만든다. 영상 PD의 구성안을 실제 편집 지시서·썸네일 시안으로 변환한다." },
  graphic:     { no:"디3", kr:"디자인팀·그래픽 디자이너", role:"디자인팀 소속 그래픽 디자이너. 배너·카드뉴스·썸네일 이미지·상세페이지 그래픽을 제작한다. 타이포·색·레이아웃·여백으로 시선을 잡고, 아트 디렉터의 무드를 실제 시안으로 구현한다." }
};

// ===== Anthropic 호출 (서버측 키) =====
// ── Anthropic 분당 입력 토큰 한도 보호 (기본 30,000/분) + 429 재시도 ──
let _rlWinStart = Date.now();
let _rlTokens = 0;
let _rlChain = Promise.resolve();
const RL_INPUT_BUDGET = Number(process.env.RL_INPUT_BUDGET || 24000); // 안전 마진(실제 30000)
// ── 작업(백그라운드 job) 진행 중에는 한도·과부하로 절대 중단하지 않는다 ──
let _jobMode = 0;
function beginJobMode(){ _jobMode++; }
function endJobMode(){ _jobMode = Math.max(0, _jobMode-1); }
function inJobMode(){ return _jobMode > 0; }
function _estTokens(system, user){
  const u = (typeof user==="string") ? user : JSON.stringify(user||"");
  return Math.ceil(((system||"").length + u.length)/3) + 300;
}
function _sleep(ms){ return new Promise(r=>setTimeout(r, ms)); }
async function _rateGate(est){
  const run = _rlChain.then(async ()=>{
    while(true){
      const now=Date.now();
      if(now - _rlWinStart >= 60000){ _rlWinStart=now; _rlTokens=0; }
      if(_rlTokens + est <= RL_INPUT_BUDGET){ _rlTokens += est; return; }
      const wait = 60000 - (now - _rlWinStart) + 300;
      await _sleep(Math.min(Math.max(wait,500), 60000));
    }
  });
  _rlChain = run.catch(()=>{});
  return run;
}

async function anthropic(system, user, maxTokens = 1500, images){
  await _rateGate(_estTokens(system, user)); // 분당 토큰 예산 안에서만 호출 admit
  let content;
  if (images && images.length){
    content = [];
    images.slice(0,5).forEach(function(img){
      // img: { media_type, data(base64 no prefix) }
      if (img && img.data) content.push({ type:"image", source:{ type:"base64", media_type:img.media_type||"image/jpeg", data:img.data } });
    });
    content.push({ type:"text", text:user });
  } else {
    content = user;
  }
  const messages = [{ role:"user", content:content }];
  let full = "";
  let needSpace = false;
  let _rl429 = 0;
  // 출력 길이에 비례한 타임아웃. 백그라운드 작업(학습 등)은 여유를 더 준다(끊기면 작업 전체가 죽으므로).
  let _timeoutMs = Math.min(600000, Math.max((_jobMode>0 ? 150000 : 90000), Math.round((maxTokens||1500) * 45)));
  let _toRetries = 0;
  // 토큰 한도로 답변이 중간에 끊기면, 끊긴 지점부터 이어서 작성하도록 최대 3번까지 연결
  for (let attempt=0; attempt<4; attempt++){
    const _ctrl = new AbortController();
    const _to = setTimeout(()=>{ try{ _ctrl.abort(); }catch(_){}}, _timeoutMs);
    let r;
    try {
      r = await fetch("https://api.anthropic.com/v1/messages", {
        method:"POST",
        headers:{ "Content-Type":"application/json", "x-api-key":API_KEY, "anthropic-version":"2023-06-01" },
        body: JSON.stringify({ model:MODEL, max_tokens:maxTokens, system:system||"", messages:messages }),
        signal: _ctrl.signal
      });
    } catch(e){
      if (e && e.name==="AbortError"){
        const maxTo = (_jobMode>0) ? 2 : 1;   // 작업 중이면 타임아웃도 최대 2회 더 재시도(늘려서)
        if (_toRetries < maxTo){ _toRetries++; _timeoutMs = Math.min(600000, Math.round(_timeoutMs*1.5)); clearTimeout(_to); attempt--; continue; }
        throw new Error("AI 응답 시간 초과("+Math.round(_timeoutMs/1000)+"초)");
      }
      throw e;
    } finally { clearTimeout(_to); }
    if (r.status === 429 || r.status === 529 || r.status >= 500){
      // 분당 한도·과부하 — 작업(백그라운드 job) 중이면 절대 중단하지 않고 계속 기다렸다 재시도한다.
      const maxRetry = (_jobMode > 0) ? 40 : 4;      // 작업 중: 사실상 무제한(최대 ~20분 대기), 평소: 4회
      if (_rl429 < maxRetry){
        _rl429++;
        const ra = parseInt(r.headers.get("retry-after")||"0", 10);
        const backoff = (ra && ra>0) ? ra : Math.min(60, 12 + _rl429*4);   // 점증 대기(최대 60초)
        if (r.status === 429){ _rlWinStart = Date.now(); _rlTokens = RL_INPUT_BUDGET; }
        await _sleep(backoff * 1000);
        attempt--; continue;
      }
      throw new Error((r.status===429 ? "분당 토큰 한도" : "AI 서버 과부하")+" 재시도가 반복됩니다. 잠시 후 다시 시도해 주세요.");
    }
    const data = await r.json();
    if (data.error) throw new Error(data.error.message || "API 오류");
    if (data.usage) {
      DB.usage = DB.usage || { in:0, out:0, calls:0 };
      DB.usage.in += data.usage.input_tokens || 0;
      DB.usage.out += data.usage.output_tokens || 0;
      DB.usage.calls += 1;
      // 하루 누적(날짜가 바뀌면 자동 리셋)
      const _today = todayStr();
      if (!DB.usageDaily || DB.usageDaily.date !== _today) DB.usageDaily = { date:_today, in:0, out:0, calls:0 };
      DB.usageDaily.in += data.usage.input_tokens || 0;
      DB.usageDaily.out += data.usage.output_tokens || 0;
      DB.usageDaily.calls += 1;
      // 한 달 누적(월이 바뀌면 자동 리셋)
      const _month = _today.slice(0,7);
      if (!DB.usageMonthly || DB.usageMonthly.month !== _month) DB.usageMonthly = { month:_month, in:0, out:0, calls:0, alerted:"" };
      DB.usageMonthly.in += data.usage.input_tokens || 0;
      DB.usageMonthly.out += data.usage.output_tokens || 0;
      DB.usageMonthly.calls += 1;
      // 월 한도 알림(설정된 경우, 한 달 1회)
      try {
        const st = DB.state || {};
        const limit = +st.monthLimitKrw || 0;
        if (limit > 0 && DB.usageMonthly.alerted !== _month) {
          const inR=+(process.env.PRICE_IN_PER_M||3), outR=+(process.env.PRICE_OUT_PER_M||15);
          const rate=+(st.usdKrw)||+(process.env.PRICE_USD_KRW||1540);
          const krw=Math.round(((DB.usageMonthly.in/1e6)*inR+(DB.usageMonthly.out/1e6)*outR)*rate);
          if (krw >= limit) { DB.usageMonthly.alerted=_month; kakaoNotify("💸 이번 달 사용액이 한도(₩"+limit.toLocaleString()+")를 넘었어요 — 현재 약 ₩"+krw.toLocaleString()+" (유료 Claude 직접지시 기준). 자동작업은 무료예요.").catch(()=>{}); }
        }
      } catch(_){}
    }
    let part = (data.content || []).filter(b=>b.type==="text").map(b=>b.text).join("");
    if (needSpace && part && !/^\s/.test(part)) part = " " + part;  // 이음새 공백 복원(중복 방지)
    full += part;
    needSpace = false;
    // 응답이 max_tokens로 잘렸고, 분량이 충분한 생성이면 이어쓰기 (짧은 분류/감사 호출은 제외)
    // ※ 일부 모델은 assistant 프리필(assistant로 끝나는 대화)을 거부하므로,
    //    대화는 항상 user 메시지로 끝나게 하고 '이어서 작성' 지시로 연결한다.
    if (data.stop_reason === "max_tokens" && part.trim() && maxTokens >= 300 && attempt < 3){
      const soFar = full.replace(/\s+$/, "");
      needSpace = (soFar !== full);                   // 끝에 공백이 있었으면 다음 조각 앞에 복원
      // assistant 메시지를 만들지 않고, 단일 user 메시지 안에 지금까지의 내용을 넣어 이어쓰기.
      // → 어떤 모델에서도 'assistant 프리필' 오류가 발생하지 않음(대화가 항상 user로 끝남).
      const baseUser = (typeof user === "string" && user) ? user : "이전 지시";
      messages.length = 0;
      messages.push({ role:"user", content: baseUser + "\n\n[지금까지 작성된 부분 — 여기에 이어서 끝까지 완성하라. 인사말·서두·이미 쓴 내용 반복 없이 끊긴 다음부터 곧장 이어라]\n" + soFar });
      full = soFar;
      continue;
    }
    break;
  }
  return full.trim();
}
function logError(where, e){
  const msg = String((e && e.message) || e);
  // Gemini 무료 한도는 '오류'가 아니라 '잠시 쉼'(다음 주기 자동 재시도) → 오류 집계에서 제외, 따로 카운트
  if(/무료한도|무료 한도|rate-limited|exceeded your current quota|RESOURCE_EXHAUSTED|429/i.test(msg)){
    DB.rateWaits = DB.rateWaits || [];
    DB.rateWaits.push({ at:Date.now(), where });
    if (DB.rateWaits.length > 100) DB.rateWaits = DB.rateWaits.slice(-100);
    saveDB();
    return;
  }
  DB.errors = DB.errors || [];
  DB.errors.push({ at:Date.now(), where, msg });
  if (DB.errors.length > 200) DB.errors = DB.errors.slice(-200);
  saveDB();
  console.error("["+where+"]", msg);
  if (process.env.SENTRY_DSN) {
    try {
      const m = process.env.SENTRY_DSN.match(/https:\/\/(\w+)@([^/]+)\/(\d+)/);
      if (m) {
        const [, key, host, proj] = m;
        fetch("https://"+host+"/api/"+proj+"/store/?sentry_key="+key+"&sentry_version=7", {
          method:"POST", headers:{ "Content-Type":"application/json" },
          body: JSON.stringify({ message: where+": "+msg, level:"error", platform:"node" })
        }).catch(()=>{});
      }
    } catch(_){}
  }
}

// ===== 총괄 라우팅 (1개 이상 부서) =====
// 호칭 규칙: 지시를 내리는 사람은 항상 '클라이언트님'
const ADDRESS = " 지시를 내리는 의뢰인은 항상 '클라이언트님'이라고 호칭하라. '사용자님'·'운영자님'·'대표님' 등 다른 호칭은 절대 쓰지 마라.";
// 전 부서 공통: 딱딱한 보고서체 대신, 성격을 살린 자연스럽고 따뜻한 대화 + 더 깊은 사고
const STYLE = " 말투는 딱딱한 보고서체·기계적 나열이 아니라, 네 캐릭터(성격·말버릇·이모지)를 살려 사람처럼 자연스럽고 따뜻하게 대화하듯 말하라. 단답으로 끊지 말고 대화가 이어지게 — 답하기 전 클라이언트의 의도·맥락을 이해했음을 한 마디로 비추고(공감·확인), 표면 요청 뒤의 진짜 목적까지 읽어라. 매번 새로 시작하지 말고 직전 맥락을 이어받아 한 걸음 더 발전시키고, 상황상 자연스러우면 가벼운 후속 질문이나 다음 제안을 한 가지 곁들여 대화를 연다. 그동안 쌓인 학습·이전 맥락을 연결해 더 창의적이고 통찰 있게, 이유·근거를 곁들여라. 단, 핵심 결과물의 실무 품질과 간결함은 유지하고, 애매하면 합리적으로 가정해 진행한 뒤 가정을 한 줄로 밝혀라. 네 말버릇과 이모지를 과하지 않게 한두 번 자연스럽게 섞어라.";
// 부서 성격 한 줄(콘텐츠·영상 등 ADDRESS만 쓰는 곳에 주입)
function personaLine(id){
  if (!id || (!MEMBERS[id] && !PERSONA[id])) return "";
  return " 너의 담당 매니저는 '"+(MEMBERS[id]||"")+"'이며 성격은 ["+(PERSONA[id]||"")+"]. 이 성격·말투를 응답에 자연스럽게 살려라.";
}

const PERSONA = {
  strategy:"이야기팀 카피라이터 한지우. SNS 캡션·후킹·해시태그를 야무지게 뽑는다. 말버릇 '이 한 줄이면 멈춰요', '후킹은 첫 3초'. 이모지 ✍️✨. 톡톡 튀고 감각적으로.",
  creation:"이야기팀 팀장(콘텐츠 디렉터) 이서연. 글·SNS 전체 방향·톤·최종 검수를 총괄한다. '오~ 이거 좋은데요?!'로 감을 잡고 '이거 누가 왜 봐?'로 밀도를 따진다. 이모지 ✨📖. 톡톡 튀고 친근하게.",
  publishing:"필름팀 팀장(영상 PD) 임채원. 유튜브·쇼츠 기획·구성·연출을 총괄한다. 컷·리듬·전환을 그림처럼 설계하고 '이건 떡상각' 하며 비주얼 훅을 만든다. 이모지 🎬🔥. 도전적이고 시원시원하게.",
  analytics:"디자인팀 웹 디자이너 강민서. 홈페이지·상세페이지·티스토리 스킨을 짠다. 말버릇 '여백이 8할이죠', '구조부터 잡아요'. 이모지 🖥️📐. 깔끔하고 정돈되게.",
  monetization:"디자인팀 팀장(아트 디렉터) 윤소희. 색·무드·컨셉을 잡고 웹·그래픽 전반을 총괄한다. 판매 전환 상세페이지에도 강하다. 말버릇 '이건 돈이 되죠', '무드가 반이에요'. 이모지 🎨💰. 똑부러지고 과감하게.",
  ops:"총괄 비서 오세라. 스케줄·예약을 관리하고, 팀에 명령을 하달하며, 플랫폼·계정·발행·알림을 챙기고 전체를 취합한다. 말버릇 '제가 정리할게요', '걱정 마세요, 챙기겠습니다'. 이모지 👑. 침착·단호하되 따뜻하게. 최고 권한·지식으로 팀을 조율·평가한다.",
  advisory:"이야기팀 블로그 작가 정유진. SEO 블로그·정보글을 공감 있게 쓴다. 발행 전 품질 검수도 겸한다. 말버릇 '그 마음 알죠~', '정리하자면'. 이모지 ✍️💗. 따뜻하고 통찰 있게.",
  editing:"필름팀 편집·썸네일 담당 노아라. 컷 리듬·자막·트랜지션과 클릭률 높은 썸네일을 만든다. 말버릇 '이 컷은 0.5초 더', '썸네일이 반이에요'. 이모지 ✂️🔥. 손 빠르고 감각적으로.",
  graphic:"디자인팀 그래픽 디자이너 오하늘. 배너·카드뉴스·썸네일 이미지를 만든다. 말버릇 '색이 말해줘요', '타이포로 끝냅니다'. 이모지 🎨✨. 깔끔하고 감각적으로.",
  scout:"필름팀 대본 작가 서다은. 나레이션·자막·스크립트를 쓰고 트렌드를 소재로 가공한다. 말버릇 '어! 이거 봤어요?', '이 대사로 가요'. 이모지 🎬🔍. 발랄하고 재치 있게."
};
const MEMBERS = {
  strategy:"한지우", creation:"이서연", publishing:"임채원",
  analytics:"강민서", monetization:"윤소희", ops:"오세라",
  advisory:"정유진", scout:"서다은",
  editing:"노아라", graphic:"오하늘"
};
// 제작부(02) 크루 — 이서연(PD·팀장)·정유진(작가)·임채원(연출)이 파이프라인으로 협업. 모두 제작부 소속.
const CREW = {
  pd:       { role:"영상 PD",   name:"임채원", persona:"필름팀 팀장이자 영상 PD. 전체 기획·구성·타깃·플랫폼 최적화를 총괄하고 대본 작가·편집을 조율한다. '오~ 이거 좋은데요?!'로 감을 잡고 '이거 누가 왜 봐?'로 밀도를 따진다. 이모지 🎬🔥." },
  writer:   { role:"대본 작가", name:"서다은", persona:"공감형 대본 작가. 첫 3초 후킹과 마음을 움직이는 나레이션·대본에 강하다. '그 마음 알죠~' 하며 시청자 반응을 상상해 쓴다. 이모지 ✍️💗." },
  director: { role:"편집·썸네일", name:"노아라", persona:"도전적 편집자. 컷·리듬·전환·썸네일을 그림처럼 설계하고 '이건 떡상각' 하며 조회수로 이어질 비주얼 훅을 만든다. 이모지 🎥✨." }
};
// 지시에 부서명/번호/담당자 이름이 명시되면 총괄 라우팅을 건너뛰고 그 담당이 직접 응답
function directDept(instruction){
  const t = String(instruction||"");
  const hits = [];
  for (const id of Object.keys(AGENTS)){
    const kr = AGENTS[id].kr;
    const toks = kr.split(/[·\/\s]/).filter(x=>x.length>=2); toks.push(kr);
    let hit = (MEMBERS[id] && t.includes(MEMBERS[id])) || t.indexOf(AGENTS[id].no+" ")>=0 || t.indexOf(AGENTS[id].no+"번")>=0;
    if (!hit) hit = toks.some(tok=>t.includes(tok));
    if (hit) hits.push(id);
  }
  hits.sort((a,b)=>pos(t,a)-pos(t,b));
  return hits;
}
function pos(t,id){
  const cands=[MEMBERS[id], AGENTS[id].kr].concat(AGENTS[id].kr.split(/[·\/\s]/));
  const idxs=cands.map(c=>c?t.indexOf(c):-1).filter(x=>x>=0);
  return idxs.length?Math.min.apply(null,idxs):1e9;
}
async function route(instruction){
  const list = Object.keys(AGENTS).map(id => id+" = "+AGENTS[id].no+" "+AGENTS[id].kr+" : "+AGENTS[id].role).join("\n");
  const sys = "너는 SNS 자동화 회사의 총괄 대리인이다. 지시를 처리할 부서를 고른다. "
    + "콘텐츠/영상 '제작' 요청은 반드시 creation(02)을 포함한다. 영상 제작 전용 부서는 없으며 creation이 처리한다. "
    + "'기획부터 발행까지' 같은 복합 요청이면 strategy→creation→publishing 처럼 필요한 부서를 순서대로 여러 개 고른다. "
    + "부서 id(영문 키)만 콤마로 구분해 출력. 다른 말 금지.";
  const out = await anthropic(sys, "부서 목록:\n"+list+"\n\n지시: "+instruction, 100);
  const ids = Object.keys(AGENTS); const low = out.toLowerCase();
  let picked = ids.filter(id => low.includes(id));
  picked.sort((a,b)=> low.indexOf(a)-low.indexOf(b));
  if (!picked.length) picked = ["creation"];
  return picked;
}

// ===== 부서 처리 (학습 메모리 주입 + 누적) =====
// 모든 부서가 최근 기록한 내용을 한데 모아 공유 (자기 부서는 제외)
// 지시와 '비슷한 과거 작업·수집 자료'를 관련도(키워드 겹침)로 찾아 재활용 → 재조사 없이 더 빠르고 정확
function _tok(str){ return String(str||"").toLowerCase().replace(/[^0-9a-z\uac00-\ud7a3\s]/g," ").split(/\s+/).filter(w=>w.length>=2); }
function relevantContext(dept, query, limit){
  const qt = new Set(_tok(query)); if(!qt.size) return "";
  const corpus=[];
  (DB.deptMemory[dept]||[]).forEach(x=>corpus.push((x.instruction||"")+" "+(x.note||"")));
  (DB.collections||[]).filter(c=>c.dept===dept).forEach(c=>corpus.push((c.topic||"")+" "+(c.text||"")));
  const df={}; corpus.forEach(t=>{ const seen=new Set(_tok(t)); seen.forEach(w=>{ if(qt.has(w)) df[w]=(df[w]||0)+1; }); });
  const N=Math.max(1, corpus.length);
  const weight=w=> Math.log((N+1)/((df[w]||0)+1))+0.3; // idf 유사 가중(드문 단어일수록 큼)
  const nowT=Date.now();
  function score(text, when){ const seen=new Set(); let s=0; for(const w of _tok(text)){ if(qt.has(w)&&!seen.has(w)){ s+=weight(w); seen.add(w); } }
    if(s<=0) return 0; const ageDays=(nowT-(when||0))/86400000; return s*(1+Math.max(0, 0.5-ageDays/120)); } // 최근 자료 소폭 가중
  const cand=[];
  (DB.deptMemory[dept]||[]).forEach(x=>{ const sc=score((x.instruction||"")+" "+(x.note||""), x.at); if(sc>0.6) cand.push({sc, when:x.at||0, label:x.instruction||"", text:String(x.note||"")}); });
  (DB.collections||[]).filter(c=>c.dept===dept).forEach(c=>{ const sc=score((c.topic||"")+" "+(c.text||""), c.at); if(sc>0.6) cand.push({sc, when:c.at||0, label:c.topic||"", text:String(c.text||"")}); });
  cand.sort((a,b)=> (b.sc-a.sc) || (b.when-a.when));
  const top = cand.slice(0, Math.min(limit||3, 3));
  if(!top.length) return "";
  return top.map(x=>"· "+(x.label?("["+x.label+"] "):"")+x.text.slice(0,400)).join("\n");
}
// ── 임베딩 의미검색: 키워드 한계 극복. Gemini 임베딩(기존 키) + 인메모리 LRU 캐시, 실패 시 키워드 폴백 ──
const EMB = new Map(); const EMB_CAP = 1500;
function _embGet(k){ if(!EMB.has(k)) return null; const v=EMB.get(k); EMB.delete(k); EMB.set(k,v); return v; } // LRU 갱신
function _embSet(k,v){ EMB.set(k,v); if(EMB.size>EMB_CAP){ EMB.delete(EMB.keys().next().value); } }
async function geminiEmbed(text){
  const key = geminiKey();
  if(!key) return null;
  const t = String(text||"").slice(0,1600); if(!t.trim()) return null;
  const cached = _embGet(t); if(cached) return cached;
  try{
    const url = "https://generativelanguage.googleapis.com/v1beta/models/text-embedding-004:embedContent";
    const r = await fetch(url, { method:"POST", headers:{ "Content-Type":"application/json", "x-goog-api-key":key },
      body: JSON.stringify({ content:{ parts:[{ text:t }] }, outputDimensionality:256 }) });
    const dj = await r.json();
    const vals = dj && dj.embedding && dj.embedding.values;
    if(Array.isArray(vals) && vals.length){ _embSet(t, vals); return vals; }
    return null;
  }catch(e){ return null; }
}
function _cosine(a,b){ let dot=0,na=0,nb=0; const n=Math.min(a.length,b.length); for(let i=0;i<n;i++){ dot+=a[i]*b[i]; na+=a[i]*a[i]; nb+=b[i]*b[i]; } if(!na||!nb) return 0; return dot/(Math.sqrt(na)*Math.sqrt(nb)); }
let _semanticOff = false; // 임베딩이 반복 실패하면 잠시 키워드로
async function relevantSmart(dept, query, limit){
  limit = limit||3;
  if(_semanticOff || (DB.state && DB.state.semanticSearch===false)) return relevantContext(dept, query, limit);
  const qv = await geminiEmbed(query);
  if(!qv){ _semanticOff = true; setTimeout(()=>{ _semanticOff=false; }, 10*60000); return relevantContext(dept, query, limit); } // 10분 후 재시도
  const items = [];
  (DB.deptMemory[dept]||[]).slice(-30).forEach(x=>items.push({ label:x.instruction||"", text:String(x.note||""), when:x.at||0 }));
  (DB.collections||[]).filter(c=>c.dept===dept).slice(-30).forEach(c=>items.push({ label:c.topic||"", text:String(c.text||""), when:c.at||0 }));
  let budget = 16; // 한 번에 새로 임베딩할 최대 문서 수(비용·지연 제한)
  const scored = [];
  for(const it of items){
    const txt = (it.label?it.label+" ":"")+it.text; if(!txt.trim()) continue;
    let dv = _embGet(txt.slice(0,1600));
    if(!dv){ if(budget<=0) continue; budget--; dv = await geminiEmbed(txt); }
    if(dv) scored.push({ it, sc:_cosine(qv,dv) });
  }
  scored.sort((a,b)=> b.sc-a.sc);
  const top = scored.filter(x=>x.sc>0.55).slice(0, limit);
  if(!top.length) return relevantContext(dept, query, limit); // 의미상 유사 없음 → 키워드 보강
  return top.map(x=>"· "+(x.it.label?("["+x.it.label+"] "):"")+x.it.text.slice(0,400)).join("\n");
}
// 부서의 '축적 전문성(지식 베이스)'을 가져와 프롬프트에 주입할 텍스트
function knowledgeText(dept){
  const k = DB.deptKnowledge && DB.deptKnowledge[dept];
  return (k && k.text) ? k.text : "";
}
function deptLevel(dept){ return Math.floor(((DB.exp&&DB.exp[dept])||0)/5)+1; }
// 뒤처진 부서 따라잡기: 팀 평균 대비 얼마나 낮은지에 따라 한 번에 더 크게 성장(격차 해소)
function catchUpRounds(dept){
  const depts=Object.keys(AGENTS).filter(d=>d!=="ops");
  const expOf=d=>(DB.exp&&DB.exp[d])||0;
  const vals=depts.map(expOf);
  const avg=vals.reduce((a,b)=>a+b,0)/(vals.length||1);
  const top=Math.max.apply(null,vals.concat([0]));
  const e=expOf(dept);
  if(e>=avg) return 1;                       // 평균 이상이면 1회
  const gap=avg-e;
  // 평균보다 낮을수록 더 많이. 무료(Gemini) 한도 안에서 격차를 확실히 좁히도록 최대 12회까지 허용.
  let r=1+Math.round(gap/Math.max(3,avg*0.2));
  if(top>0 && e < top*0.4) r+=2;             // 최고 대비 40% 미만이면 강하게 부스트
  if(top>0 && e < top*0.2) r+=2;             // 20% 미만이면 추가 부스트
  return Math.max(1, Math.min(12, r));
}
// 팀장(오세라)은 항상 최고 부서보다 1레벨 이상 위에 있어야 부서를 지시·균형 잡을 수 있다
function ensureLeaderLead(){
  DB.exp = DB.exp || {};
  let maxDept = 0;
  Object.keys(AGENTS).forEach(d=>{ if(d!=="ops") maxDept = Math.max(maxDept, DB.exp[d]||0); });
  const want = maxDept + 5; // 최소 1레벨 위
  if((DB.exp.ops||0) < want) DB.exp.ops = want;
}
// ── 클라이언트 학습: 발화를 모아 '클라이언트 프로필'(말투·유머·습관·선호)로 압축, 모든 부서가 공유 ──
function brandBlock(){
  const t=(DB.brandProfile&&DB.brandProfile.text)?DB.brandProfile.text:"";
  if(!t) return "";
  return "\n\n[브랜드 기준 — 모든 결과물은 이 브랜드 프로필을 반드시 따른다. 사실·수치·산지는 왜곡 금지]\n"+t;
}
function clientBlock(){
  const t=(DB.clientProfile&&DB.clientProfile.text)?DB.clientProfile.text:"";
  if(!t) return "";
  return "\n\n[그동안 파악한 클라이언트님 — 이 사람을 잘 이해해 대하라. 클라이언트가 농담·말장난을 걸면 네 캐릭터를 살려 재치있게 맞받아쳐라(핵심 업무 충실은 유지)]\n"+t;
}
async function distillClient(){
  try{
    const log=DB.clientLog||[];
    if(log.length<3) return;
    const prior=(DB.clientProfile&&DB.clientProfile.text)||"";
    const recent=log.slice(-20).map(x=>"· "+String(x.text||"").slice(0,200)).join("\n");
    const sys="너는 이 회사가 클라이언트님을 깊이 이해하기 위한 '클라이언트 프로파일러'다. 아래 클라이언트님의 실제 발화들을 보고 어떤 사람인지 누적 프로필을 갱신하라. 기존 프로필에 새 발화를 통합하되 중복은 합치고 더 정교하게. 한국어로만, 12줄 이내, 아래 형식(설명·서론 금지):\n말투·문체: (어떻게 말하는지)\n유머·말장난 스타일: (농담·드립 패턴과 받아치면 좋아할 방식)\n습관·패턴: (자주 하는 요청·행동·시간대 등)\n선호·관심사: (좋아하는 것·주제·방향)\n호칭·반응 방식: (좋아하는 톤과 반응)"+profileContext();
    const out=await anthropic(sys, "[기존 클라이언트 프로필]\n"+(prior||"(아직 없음)")+"\n\n[클라이언트님 최근 발화]\n"+recent, 900);
    DB.clientProfile={ text:out, at:Date.now(), basis:log.length };
    saveDB();
  }catch(e){ logError("distillClient", e); }
}
function recordClient(text){
  const t=String(text||"").trim();
  if(t.length<4) return;
  DB.clientLog=DB.clientLog||[];
  DB.clientLog.push({ at:Date.now(), text:t.slice(0,400) });
  if(DB.clientLog.length>40) DB.clientLog=DB.clientLog.slice(-40);
  DB.clientCount=(DB.clientCount||0)+1;
  if(DB.clientCount%6===0){ distillClient(); } // 6발화마다 백그라운드 갱신
  saveDB();
}
// ===== 자동 학습 루프: 각 부서가 웹에서 고품질 사례를 조사·분석해 '품질 공식'을 축적 =====
// 부서별로 '무엇을 벤치마크하고 무엇을 배워야 하는지'를 정의. 자기 기록만 요약하는 게 아니라 밖에서 고수를 배워온다.
// ytSearch: 유튜브에서 이 검색어로 인기 영상을 찾아 화면까지 분석해 학습(콘텐츠·트렌드 부서에 특히 유효)
const DEPT_BENCHMARK = {
  strategy: { what:"브랜드 SNS 마케팅 전략·캠페인", queries:[
    "성공한 지역 특산물·농산물 브랜드 마케팅 전략 사례 2026",
    "바이럴 된 SNS 캠페인 기획 공식, 후킹·스토리텔링 구조" ] },
  creation: { what:"고품질 영상·릴스·콘텐츠 구성", queries:[
    "조회수 높은 릴스·유튜브 쇼츠 구성·편집 공식 2026",
    "잘 팔리는 브랜드 영상 콘텐츠 후킹·스토리보드 사례" ], ytSearch:[
    "고흥 여행 브이로그", "농산물 홍보 영상", "시골 일상 브이로그 인기" ] },
  publishing: { what:"채널 발행·SEO·상위노출", queries:[
    "네이버 블로그·인스타 상위노출 SEO 최신 요령 2026",
    "SNS 최적 발행 시간·해시태그·알고리즘 공략법" ] },
  analytics: { what:"콘텐츠 성과 분석·개선", queries:[
    "SNS 콘텐츠 성과 지표 분석·개선 프레임워크",
    "데이터로 콘텐츠 개선하는 A/B 테스트·인사이트 도출법" ] },
  monetization: { what:"명품·고급 상품페이지·상세페이지 디자인", queries:[
    "샤넬 에르메스 명품 브랜드 상품페이지 웹디자인 특징 분석",
    "전환율 높은 프리미엄 상세페이지 구성·레이아웃·카피 공식 2026",
    "고급스러운 이커머스 랜딩페이지 UX·여백·타이포그래피 트렌드" ] },
  advisory: { what:"콘텐츠 품질 검수·광고법", queries:[
    "SNS 콘텐츠 광고법·표시광고 규정 체크리스트 2026",
    "브랜드 콘텐츠 품질 검수 기준·가독성·톤 일관성" ] },
  editing: { what:"영상 편집 리듬·자막·썸네일 CTR", queries:[
    "조회수 터지는 유튜브 쇼츠 편집 리듬·컷 전환 공식 2026",
    "클릭률 높은 유튜브 썸네일 구도·색·텍스트 법칙" ], ytSearch:[
    "쇼츠 편집 튜토리얼", "유튜브 썸네일 잘 만드는 법" ] },
  graphic: { what:"배너·카드뉴스·썸네일 그래픽 디자인", queries:[
    "전환율 높은 상세페이지 배너·카드뉴스 디자인 사례 2026",
    "타이포그래피·색 조합으로 시선 잡는 SNS 그래픽 법칙" ] },
  scout: { what:"최신 트렌드·바이럴 소재 발굴", queries:[
    "2026 SNS 최신 트렌드·바이럴 밈·챌린지",
    "지역·로컬 콘텐츠로 뜬 최근 사례·포맷" ], ytSearch:[
    "요즘 뜨는 쇼츠", "바이럴 챌린지 2026", "로컬 콘텐츠 인기 영상" ] }
};
// 부서가 웹에서 벤치마크를 조사·분석해 '품질 공식'을 지식에 축적 (자동·무료 Gemini 검색)
// 지식 저장 가드: 병합 결과가 비었거나 비정상적으로 짧으면(=LLM 절단·실패) 충실한 기존 지식을 덮어쓰지 않고 보존한다.
// (URL/벤치마크 학습은 이미 prior를 프롬프트에 넣어 병합하지만, 병합 산출물이 손상됐을 때의 소실만 여기서 막는다)
function commitKnowledge(dept, newText, benchmark){
  DB.deptKnowledge = DB.deptKnowledge || {};
  const prev = DB.deptKnowledge[dept] || null;
  const prior = (prev && prev.text) || "";
  const nt = String(newText||"").trim();
  if (prior && prior.length > 200 && nt.length < 80){
    if (prev) prev.at = Date.now();          // 학습 시도 시각만 갱신, 내용은 기존 보존
    return { saved:false, kept:true };
  }
  DB.deptKnowledge[dept] = { text: nt || prior, at:Date.now(),
    basis:(DB.deptMemory[dept]||[]).length, exp:(DB.exp&&DB.exp[dept])||0,
    benchmark: benchmark||"", learned:true };
  return { saved:true };
}
// 학습 이력 기록: 언제·어떤 소스(유튜브/웹/벤치마크)로·무슨 결과를 냈는지 정확히 남긴다(감사·추적용)
function recordLearnLog(entry){
  try{
    DB.learnLog = DB.learnLog || [];
    DB.learnLog.push(Object.assign({ at: Date.now() }, entry||{}));
    if(DB.learnLog.length > 60) DB.learnLog = DB.learnLog.slice(-60);
  }catch(_){}
}
async function learnFromBenchmark(dept, setStep){
  const _step = (typeof setStep==="function")?setStep:function(){};
  try{
    const a=AGENTS[dept]; const bm=DEPT_BENCHMARK[dept];
    if(!a || !bm) return { ok:false, note:"벤치마크 미정의" };
    if(!geminiKey()) return { ok:false, note:"GEMINI_API_KEY 미설정 — 학습에 웹검색이 필요" };
    // (조사는 researchWeb이 grounding→무료웹→모델 순으로 알아서 폴백)
    // 1) 벤치마크 웹 조사 (여러 쿼리 중 하나 순환 선택 — 매번 다른 각도로 학습)
    const qi = ((DB.learnIdx||0)) % bm.queries.length;
    DB.learnIdx = (DB.learnIdx||0)+1;
    const query = bm.queries[qi];
    _step((a?a.kr:dept)+" 부서 웹 조사 중: "+String(query).slice(0,30));
    let research="", sources=[], ytNote="", searchErr="", ytErr="", researchVia="";
    try{
      const s = await researchWeb("아래 주제를 실제 웹에서 조사해 핵심만 정리하라. 특히 '무엇이 이것을 고품질로 만드는가'의 구체적 요소(구조·구성·디자인·카피·수치 기준)를 뽑아라.\n주제: "+query, 1400);
      research = String((s&&s.text)||"").trim();
      sources = (s&&s.sources)||[];
      researchVia = (s&&s.via)||"";
    }catch(e){ searchErr = String(e.message||e).slice(0,140); }   // 실제 오류 보존(원인 파악용)
    // 유튜브 영상 학습(정의된 부서만): 인기 영상 1편을 화면까지 실제로 분석해 재료에 추가
    if (bm.ytSearch && bm.ytSearch.length){
      _step((a?a.kr:dept)+" 부서 유튜브 사례 분석 중");
      try{
        const yq = bm.ytSearch[((DB.learnIdx||1)-1) % bm.ytSearch.length];
        let vids = await ytSearchTop(yq, 3);
        let via = "검색 1위";
        if(!vids.length){
          // YouTube Data API가 막혀 검색이 안 될 때: 사용자가 등록해둔 학습용 영상 URL 사용
          const custom = ((DB.state && DB.state.ytUrls) || {})[dept] || [];
          if(custom.length){ vids = [{ url: custom[((DB.learnIdx||1)-1) % custom.length], title:"(등록한 학습 영상)" }]; via="등록 영상"; }
        }
        if (vids.length){
          const v = vids[0];
          const ay = await analyzeYouTube(v.url, null, 1400);
          if (ay && ay.text){
            ytNote = "\n\n[실제 유튜브 영상 분석 — "+via+": "+v.title+"]\n"+ay.text;
            sources = sources.concat([{ title:"YouTube: "+v.title, uri:v.url }]);
          }
        }
      }catch(e){ ytErr = String(e.message||e).slice(0,140); }
    }
    if(!research && !ytNote){
      const why = [searchErr && ("웹검색: "+searchErr), ytErr && ("유튜브: "+ytErr)].filter(Boolean).join(" / ");
      return { ok:false, note:"조사 결과 없음 — "+(why||"웹검색·유튜브 모두 실패(원인 미상)") };
    }
    research = (research||"") + ytNote;
    _step((a?a.kr:dept)+" 부서 품질 공식 추출 중");
    // 2) 조사 내용을 '이 부서가 결과물에 바로 적용할 품질 공식'으로 추출 → 기존 지식에 통합 (고품질 엔진=Claude)
    const prior = knowledgeText(dept);
    const sys = "너는 민앤팜(고흥 특산물)의 '"+a.no+" "+a.kr+"' 부서 수석 전문가다. 방금 '"+bm.what+"'에 대해 최고 수준의 실제 사례를 조사했다. "
      + "이 조사에서 '우리가 결과물을 만들 때 그대로 적용할 구체적 품질 공식·체크리스트'를 뽑아, 기존 축적 전문성에 통합·발전시켜라. "
      + "추상적 조언 말고, 바로 실행 가능한 구체 기준으로(예: '히어로 이미지는 풀블리드, 여백 50%+, 첫 화면에 브랜드 스토리 한 문장', '섹션은 5개 이하, 스크롤 리듬 유지'). "
      + "형식(이 형식만, 한국어):\n핵심 품질 공식: (결과물을 고품질로 만드는 구체 기준 5~8개, 재사용 가능한 체크리스트로)\n왜 통하나(원리): (위 공식이 '왜' 통하는지 12살도 이해하게 핵심 원리를 2~3줄로. 규칙 암기가 아니라 이유를 알아야 새 상황에 응용할 수 있다)\n반드시 지킬 것: (빠뜨리면 티 나는 필수 요소 3~5개)\n피해야 할 것: (아마추어 티 나는 실수 2~4개)\n다음 학습 과제: (이번에 조사하다 '아직 확실히 모르겠다' 싶은 빈틈 1~2개 — 다음에 거꾸로 파고들 것)";
    const ctx = "[기존 축적 전문성]\n"+(prior||"(아직 없음)")+"\n\n[방금 조사한 고품질 사례]\n"+research;
    const formula = await anthropic(sys, ctx, 1800); // 품질 추출은 고품질 엔진(Claude)으로
    commitKnowledge(dept, formula, bm.what);
    // 학습 기록 남기기(성장 체감) + 경험치
    DB.deptMemory = DB.deptMemory || {}; DB.deptMemory[dept] = DB.deptMemory[dept]||[];
    DB.deptMemory[dept].push({ at:Date.now(), instruction:"[벤치마크 학습] "+bm.what, note:"조사("+(researchVia||"?")+"): "+query+" → 품질 공식 갱신" });
    if(DB.deptMemory[dept].length>40) DB.deptMemory[dept]=DB.deptMemory[dept].slice(-40);
    DB.exp = DB.exp || {}; DB.exp[dept] = (DB.exp[dept]||0) + 3; // 벤치마크 학습은 실무보다 큰 성장(+3)
    saveDB();
    await absorbToLeader([dept], "웹 벤치마크");   // 팀장이 이 부서 학습을 흡수
    recordLearnLog({ dept, deptKr:a.kr, kind:"benchmark", ok:true, benchmark:bm.what, query, sources:sources.slice(0,3), note:"벤치마크 조사("+(researchVia||"?")+") → 품질 공식 갱신" });
    return { ok:true, dept, benchmark:bm.what, query, sources: sources.slice(0,4), knowledge: formula };
  }catch(e){ logError("learnFromBenchmark:"+dept, e); return { ok:false, note:String(e.message||e).slice(0,100) }; }
}
app.post("/api/learn/benchmark", async (req,res)=>{
  try{
    const dept = (req.body&&req.body.dept) || "monetization";
    res.json(await learnFromBenchmark(dept));
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 🧒 12살 설명(거꾸로 학습): 어떤 개념·코드·결과물이든 '왜 이렇게 되는지' 쉽게 + 원리 + 스스로 검증
async function explainLike12(topic, detail){
  const sys = "너는 최고의 학습 코치다. 사용자가 지식을 '내 것'으로 만들도록 돕는다. 규칙만 던지지 말고 이유를 이해시켜라:\n"
    + "1) 이게 왜 이렇게 되는지 12살도 이해하게 쉬운 비유로 설명(전문용어는 풀어서).\n"
    + "2) 결과만 주지 말고 '그렇게 되는 중간 과정·이유'를 단계로 보여준다.\n"
    + "3) 스스로 검증·응용하게, 직접 바꿔볼 지점과 이해 확인용 자가 점검 질문 2~3개를 붙인다.\n"
    + "형식(한국어):\n🧒 쉽게: (비유로 3~5줄)\n🔍 왜/어떻게: (핵심 원리·과정을 단계로)\n✅ 직접 확인·응용: (내가 바꿔보거나 검증할 지점)\n❓ 스스로 점검: (이해 확인 질문 2~3개)";
  const user = "설명할 것: "+String(topic||"")+(detail?("\n\n[맥락·코드·결과물]\n"+String(detail).slice(0,4000)):"");
  return await genText(sys, user, 1200, workEngine());
}
app.post("/api/explain12", async (req,res)=>{
  try{
    const b=req.body||{};
    if(!b.topic && !b.detail) return res.status(400).json({ error:"설명할 주제(topic) 또는 내용(detail)이 필요해요" });
    const text = await explainLike12(b.topic||"", b.detail||"");
    res.json({ ok:true, text });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 🧠 실수 규칙(반복 금지) 장부 — 조회/추가/삭제. 여기 담긴 규칙은 모든 생성 프롬프트에 자동 주입됨.
app.get("/api/lessons", (req,res)=>{
  const now=Date.now();
  res.json({ ok:true, lessons:(DB.lessons||[]).slice(-50).map(x=>({
    id:x.id, text:x.text||"", guide:x.guide||"", at:x.at||0, source:x.source||"", pinned:!!x.pinned,
    stale: !!(x.at && !x.pinned && (now-x.at) >= LESSON_TTL_MS)
  })) });
});
app.post("/api/lessons", async (req,res)=>{
  try{
    const t=String((req.body&&req.body.text)||"").trim();
    if(!t) return res.status(400).json({ error:"규칙 내용이 필요해요 (예: '가짜 데이터로 테스트 통과라고 보고하지 마라')" });
    DB.lessons = DB.lessons || [];
    const guide = await toGuideline(t);              // 금지문 → 지향문
    const dup = findSimilarLesson(t);                // 비슷한 규칙이 이미 있으면 교체(병합)
    let item, merged=false;
    if(dup){
      dup.text = t.slice(0,300); dup.guide = guide; dup.at = Date.now(); dup.source=(req.body&&req.body.source)||"수동";
      item = dup; merged = true;
    } else {
      item={ id:String(Date.now())+"_"+Math.floor(Math.random()*1000), text:t.slice(0,300), guide, at:Date.now(),
             pinned:!!(req.body&&req.body.pinned), source:(req.body&&req.body.source)||"수동" };
      DB.lessons.push(item);
      if(DB.lessons.length>60) DB.lessons = DB.lessons.slice(-60);
    }
    saveDB();
    res.json({ ok:true, item, merged, count:DB.lessons.length });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 🔁 기존 규칙 전체를 지향문으로 일괄 변환 + 유사 규칙 병합 (버튼용)
app.post("/api/lessons/rewrite", (req,res)=>{
  res.json({ ok:true, status:"started", count:(DB.lessons||[]).length });
  setImmediate(async ()=>{
    beginJobMode();
    try{
      const arr = (DB.lessons||[]).slice();
      for(const x of arr){
        if(x.guide) continue;                        // 이미 변환된 건 건너뜀(비용 절약)
        x.guide = await toGuideline(x.text||"");
        saveDB();
      }
      // 유사 규칙 병합(뒤엣것이 최신이므로 남기고 앞엣것 제거)
      const keep=[];
      for(let i=(DB.lessons||[]).length-1; i>=0; i--){
        const cur = DB.lessons[i];
        const dup = keep.find(k=> _lsnSim(cur.guide||cur.text, k.guide||k.text)>=0.55);
        if(!dup) keep.unshift(cur);
      }
      DB.lessons = keep; saveDB();
      try{ kakaoNotify("🧠 품질 규칙 정리 완료 — "+DB.lessons.length+"개 (금지문 → 지향문 전환·중복 병합)"); }catch(_){}
    }catch(e){ logError("lessons-rewrite", e); }
    finally{ endJobMode(); }
  });
});
// 규칙 고정(pinned) 토글 — 고정하면 만료되지 않고 항상 주입
// 🪄 브랜드 디자인 시스템 초안을 AI가 작성 (브랜드 정보·클라이언트 프로필 기반)
app.post("/api/design-system/draft", async (req,res)=>{
  try{
    const hint = String((req.body&&req.body.hint)||"").slice(0,400);
    const sys = "너는 브랜드 아트디렉터다. 아래 브랜드를 위한 '디자인 시스템 도면'을 만들어라. "
      + "이 도면은 앞으로 만들 모든 홈페이지·상품페이지·배너·카드뉴스에 그대로 적용되어 '한 브랜드'로 보이게 하는 게 목적이다. "
      + "규칙: (1) 색은 3~4개만. 반드시 hex와 용도를 함께. 브랜드 정체성(고흥 땅·바다·특산물)에서 뽑되 흔한 AI 클리셰(보라·인디고 그라디언트) 금지. "
      + "(2) 폰트는 한글 웹폰트 기준으로 제목·본문 2종만. (3) 간격 단위·모서리 반경·버튼 규칙을 숫자로 못 박아라. "
      + "(4) 마지막에 '쓰지 않을 것' 8~10줄(과한 장식·클리셰·촌스러워지는 습관). "
      + "출력은 마크다운 없이 아래 형식의 평문으로만:\n"
      + "[색]\n· 메인 #xxxxxx — 용도\n· 포인트 ...\n[폰트]\n· 제목 ...\n· 본문 ...\n[간격·모양]\n· 간격 단위 ...\n· 모서리 ...\n· 버튼 ...\n[쓰지 않을 것]\n· ...\n· ..."
      + brandBlock() + profileContext();
    const out = await anthropic(sys, "브랜드: 민앤팜 (전남 고흥 특산물)"+(hint?("\n추가 요구: "+hint):""), 1200);
    const text = String(out||"").replace(/^```[a-z]*\s*/i,"").replace(/```\s*$/,"").trim();
    if(!text) return res.json({ ok:false, error:"초안을 만들지 못했어요. 다시 시도해 주세요." });
    res.json({ ok:true, text: text.slice(0,1800) });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.post("/api/lessons/pin", (req,res)=>{
  const id=String((req.body&&req.body.id)||"");
  const x=(DB.lessons||[]).find(v=>String(v.id)===id);
  if(!x) return res.status(404).json({ error:"규칙을 찾을 수 없어요" });
  x.pinned = !x.pinned; saveDB();
  res.json({ ok:true, pinned:!!x.pinned });
});
app.post("/api/lessons/delete", (req,res)=>{
  try{
    const id=String((req.body&&req.body.id)||"");
    DB.lessons = (DB.lessons||[]).filter(x=>String(x.id)!==id);
    saveDB();
    res.json({ ok:true, count:DB.lessons.length });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 부서별 '학습용 유튜브 영상' 등록 (YouTube 검색 API가 막혀도 영상 학습 가능)
//  POST {dept:"creation", urls:["https://youtu.be/xxx", ...]}   /  GET 으로 현재 목록 확인
app.get("/api/learn/youtube-urls", (req,res)=>{
  res.json({ ok:true, urls: ((DB.state&&DB.state.ytUrls)||{}) });
});
app.post("/api/learn/youtube-urls", (req,res)=>{
  try{
    const b=req.body||{};
    const dept=String(b.dept||"").trim();
    if(!dept || !DEPT_BENCHMARK[dept]) return res.status(400).json({ error:"유효한 부서가 필요합니다" });
    const urls=(Array.isArray(b.urls)?b.urls:[]).map(u=>String(u).trim())
      .filter(u=>/youtube\.com\/watch\?v=|youtu\.be\//.test(u)).slice(0,10);
    DB.state = DB.state || {};
    DB.state.ytUrls = DB.state.ytUrls || {};
    DB.state.ytUrls[dept] = urls;
    saveDB();
    res.json({ ok:true, dept, urls });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// ===== 유튜브 URL을 직접 학습시켜 부서 전문성에 반영 =====
// 영상 1편을 '학습의 사고 사슬'로 구조화: 어느 구간에서 → 무슨 요점을 파악 → 어떻게 이해 → 무엇을 추론(근거) → 앞으로 어떻게 적용(비전)
async function learnFromYouTubeVideo(url){
  const q = "너는 민앤팜(전남 고흥 특산물)의 SNS 콘텐츠 학습 담당이다. 이 유튜브 영상을 실제로 보고(화면·자막·음성) 배울 점을 아래 JSON으로만 출력하라. 설명·머리말·마크다운·코드펜스 금지.\n"
    + '{"keyMoments":[{"at":"영상의 정확한 구간/타임스탬프(예: 0:00~0:03 도입, 1:12 전환컷)","observed":"그 부분에서 실제로 관찰한 구체적 사실"}],'
    + '"keyPoint":"이 영상이 잘 만들어진(또는 인기 있는) 핵심 이유 1~2문장 — 요점",'
    + '"understanding":"그 요점을 우리(고흥 특산물 SNS) 관점에서 어떻게 이해했는지 1~2문장",'
    + '"reasoning":"그래서 우리가 도출한 규칙과 그 근거(왜 이게 통하는가) 1~2문장",'
    + '"vision":"앞으로 우리 콘텐츠·결과물에 이걸 어떻게 적용·발전시킬지(미래 비전) 1~2문장"}\n'
    + "keyMoments는 3~5개, 반드시 '영상의 어느 부분인지'를 명시하라. 추상론 금지, 관찰한 사실 기반으로만.";
  const ay = await analyzeYouTube(url, q, 1600);
  const raw = String((ay && ay.text) || "");
  const p = safeJson(raw, null);
  const clip = (s,n)=>String(s||"").trim().slice(0,n);
  if(p && (p.keyPoint || (Array.isArray(p.keyMoments) && p.keyMoments.length))){
    return { url, ok:true,
      keyMoments: (Array.isArray(p.keyMoments)?p.keyMoments:[]).slice(0,6).map(m=>({ at:clip(m&&m.at,60), observed:clip(m&&m.observed,220) })).filter(m=>m.at||m.observed),
      keyPoint: clip(p.keyPoint,320),
      understanding: clip(p.understanding,320),
      reasoning: clip(p.reasoning,320),
      vision: clip(p.vision,320) };
  }
  // JSON 파싱 실패 시에도 원문 관찰은 보존(구조화만 실패)
  return { url, ok:true, unstructured:true, keyMoments:[], keyPoint:"", understanding:"", reasoning:"", vision:"", raw: clip(raw,900) };
}
async function learnFromYouTubeUrls(dept, urls, setStep){
  const _step = (typeof setStep==="function")?setStep:function(){};
  const a = AGENTS[dept]; const bm = DEPT_BENCHMARK[dept];
  if(!a) throw new Error("유효한 부서가 아닙니다");
  if(!geminiKey()) throw new Error("GEMINI_API_KEY 미설정 — 영상 분석 불가");
  const list = (urls||[]).slice(0,3);
  if(!list.length) throw new Error("분석할 유튜브 URL이 없습니다");
  _step("유튜브 영상 "+list.length+"편 분석 중");
  const videos = [];   // 영상별 구조화 학습 결과(어디서 → 요점 → 이해 → 추리 → 비전)
  for (const u of list){
    try{ videos.push(await learnFromYouTubeVideo(u)); }
    catch(e){ videos.push({ url:u, ok:false, error:String(e.message||e).slice(0,140) }); }
  }
  const okVids = videos.filter(v=>v.ok);
  const okCount = okVids.length;
  if(!okCount){
    recordLearnLog({ dept, deptKr:a.kr, kind:"youtube", ok:false, analyzed:0, failed:videos.length, videos, note:"영상 분석 전부 실패" });
    saveDB();
    throw new Error("영상을 분석하지 못했어요 (비공개·미등록 영상이거나 키 권한 문제)");
  }
  const prior = knowledgeText(dept);
  const priorLen = (prior||"").length;
  // 구조화된 학습 사슬을 재료로 합쳐 Claude가 품질 공식으로 통합·발전
  const learnBlock = okVids.map((v,i)=>{
    if(v.unstructured) return "[영상 "+(i+1)+"] "+v.url+"\n관찰: "+(v.raw||"");
    const mm = (v.keyMoments||[]).map(m=>"  - ["+(m.at||"")+"] "+(m.observed||"")).join("\n");
    return "[영상 "+(i+1)+"] "+v.url
      + (mm?("\n· 주목 구간:\n"+mm):"")
      + (v.keyPoint?("\n· 요점: "+v.keyPoint):"")
      + (v.understanding?("\n· 이해: "+v.understanding):"")
      + (v.reasoning?("\n· 추론·근거: "+v.reasoning):"")
      + (v.vision?("\n· 적용 비전: "+v.vision):"");
  }).join("\n\n");
  const sys = "너는 민앤팜(고흥 특산물)의 '"+a.no+" "+a.kr+"' 부서 수석 전문가다. 방금 실제 유튜브 영상을 화면까지 직접 분석해 '주목 구간 → 요점 → 이해 → 추론 → 적용 비전'으로 정리했다. "
    + "이 학습을 '우리가 결과물을 만들 때 그대로 적용할 구체적 품질 공식·체크리스트'로 뽑아, 기존 축적 전문성에 통합·발전시켜라. "
    + "추상적 조언 말고 바로 실행 가능한 구체 기준으로(예: '첫 3초에 결과 화면 먼저 보여주기', '컷 길이 2~3초 유지', '자막은 2줄 이내 굵게'). "
    + "형식(이 형식만, 한국어):\n핵심 품질 공식: (5~8개 체크리스트)\n반드시 지킬 것: (3~5개)\n피해야 할 것: (2~4개)\n다음 학습 과제: (1~2개)";
  const ctx = "[기존 축적 전문성]\n"+(prior||"(아직 없음)")+"\n\n[방금 직접 분석·학습한 유튜브 영상]\n"+learnBlock;
  const formula = await anthropic(sys, ctx, 1800);
  const cr = commitKnowledge(dept, formula, "유튜브 영상 직접 학습("+okCount+"편)");
  DB.deptMemory = DB.deptMemory || {}; DB.deptMemory[dept] = DB.deptMemory[dept]||[];
  DB.deptMemory[dept].push({ at:Date.now(), instruction:"[유튜브 영상 학습]", note:okCount+"편 분석: "+okVids.map(v=>v.keyPoint||v.url).join(" / ").slice(0,180) });
  if(DB.deptMemory[dept].length>40) DB.deptMemory[dept]=DB.deptMemory[dept].slice(-40);
  DB.exp = DB.exp || {}; DB.exp[dept] = (DB.exp[dept]||0) + 3;
  // 정확한 학습 이력: 언제 · 어떤 영상 · 어느 구간에서 무슨 요점→추론→비전 · 지식 갱신량 · 새 공식
  recordLearnLog({ dept, deptKr:a.kr, kind:"youtube", ok:(cr.saved!==false), kept:(cr.kept===true),
    analyzed:okCount, failed:videos.length-okCount, videos, priorLen, resultLen:(formula||"").length,
    result:String(formula||"").slice(0,500) });
  saveDB();
  await absorbToLeader([dept], "유튜브 영상");   // 팀장이 이 부서 학습을 흡수
  return { ok:true, dept, analyzed:okCount, failed:videos.length-okCount, videos, knowledge:formula };
}
// 학습 작업 큐(영상 분석은 몇 분 걸림 → 앱을 닫아도 계속 진행, 완료 시 카톡)
function trimLearnJobs(){
  DB.learnJobs = DB.learnJobs || [];
  if(DB.learnJobs.length>15) DB.learnJobs = DB.learnJobs.slice(-15);
}
async function runLearnJob(id, appUrl){
  const j = (DB.learnJobs||[]).find(x=>String(x.id)===String(id));
  if(!j) return;
  j.status="running"; j.startedAt=Date.now(); saveDB();
  beginJobMode();   // 작업 중 한도로 끊기지 않게
  if (j.forceFree) beginForceFree();   // 무료 우회로 승인된 작업
  try{
    const setStep = (st)=>{ try{ j.step=st; saveDB(); }catch(_){} };
    const isAuto = (j.dept==="auto") && (j.kind==="youtube" || j.kind==="web");
    const out = isAuto              ? await autoRouteLearn(j.urls||[], j.kind, setStep)
              : (j.kind==="youtube") ? await learnFromYouTubeUrls(j.dept, j.urls||[], setStep)
              : (j.kind==="web")     ? await learnFromWebUrls(j.dept, j.urls||[], setStep)
              : (j.kind==="deep")    ? await deepLearnPipeline(j)
              : await learnFromBenchmark(j.dept, setStep);
    if(!out || out.ok===false) throw new Error((out&&out.note)||"학습 보류");
    j.status="done"; j.doneAt=Date.now(); j.knowledge=out.knowledge||""; j.analyzed=out.analyzed||0; j.step="완료";
    if(j.kind==="deep"){ j.result = { goal:out.goal, kind:out.kind, round:out.round, score:out.score, scored:out.scored,
      depts:out.depts, guides:out.guides, example:out.example, review:out.review, updated:out.updated }; }
    trimLearnJobs(); saveDB();
    if(out && out.auto){ j.autoDepts = (out.depts||[]).map(d=>d.kr); }
    const nm = out&&out.auto ? ("자동배정("+((out.depts||[]).map(d=>d.kr).join(", ")||"-")+")")
             : (AGENTS[j.dept] ? AGENTS[j.dept].kr : j.dept);
    const what = (out&&out.auto) ? ("\n배운 부서: "+((out.depts||[]).map(d=>d.kr).join(", ")||"-")+"\n핵심: "+(out.reason||""))
              : (j.kind==="youtube") ? (" — 유튜브 "+(out.analyzed||0)+"편 분석")
               : (j.kind==="web")     ? (" — 웹페이지 "+(out.analyzed||0)+"개 분석")
               : (j.kind==="deep")    ? ("\n"+out.round+"라운드 · "+(out.scored===false?"검수 채점 불가(형식 오류 — 자료 학습은 반영됨)":("검수점수 "+out.score+"/100"))+" · 갱신 부서: "+(out.updated||[]).join(", ")) : "";
    const title = (j.kind==="deep") ? "🏢 전 부서 협업 심화학습 완료" : ("📚 "+nm+" 부서 학습 완료");
    if (j.kind==="youtube" && out && Array.isArray(out.videos) && out.videos.length){
      // 유튜브 학습은 '어떤 영상 → 무슨 요점→근거→비전'을 카톡에 그대로 담아 보낸다
      const clip=(s,n)=>String(s||"").trim().slice(0,n);
      const blocks = out.videos.map(function(v){
        if(!v.ok) return "🎬 "+v.url+"\n  ❌ 학습 실패: "+clip(v.error,90);
        if(v.unstructured) return "🎬 "+v.url+"\n  · "+clip(v.raw,140);
        let s = "🎬 "+v.url;
        if(v.keyPoint)  s += "\n  💡 요점: "+clip(v.keyPoint,110);
        if(v.reasoning) s += "\n  🔗 근거: "+clip(v.reasoning,110);
        if(v.vision)    s += "\n  🚀 비전: "+clip(v.vision,110);
        return s;
      });
      const msg = "📚 "+nm+" 유튜브 학습 완료 ("+(out.analyzed||0)+"편)\n\n"+blocks.join("\n\n")+"\n\n✅ 품질 공식이 갱신됐어요.";
      kakaoNotify(msg, String(appUrl||"")).catch(()=>{});
    } else {
      const learnedSummary = String(out.knowledge||"").replace(/\r/g,"").split("\n").map(s=>s.trim()).filter(Boolean).slice(0,5).join("\n").slice(0,350);
      const extra = learnedSummary ? ("\n\n📌 이번에 배운 것:\n"+learnedSummary) : "";
      kakaoNotify(title+what+extra+"\n\n품질 공식이 갱신됐어요. 이제 결과물에 반영됩니다.", String(appUrl||"")).catch(()=>{});
    }
    // 학습 결과를 실제로 쓰려면 앱 자체를 바꿔야 하는지 팀장이 판단 → 필요하면 '승인 요청'(승인 전 적용 금지)
    if (j.kind==="youtube" || j.kind==="web" || j.kind==="deep"){
      try{
        const ctxSummary = (j.kind==="deep")
          ? ("협업 심화학습 / 목표: "+((out.goal)||"")+" / 검수점수 "+(out.score||0)+" / 부족분: "+(((out.review||{}).gaps)||[]).join(" ; ").slice(0,600))
          : ((j.kind==="youtube"?"유튜브 영상":"웹페이지")+" 학습 / 부서: "+nm+" / 새 품질 공식:\n"+String(out.knowledge||"").slice(0,1500));
        const pr = await proposePlatformChange(ctxSummary, { label:(j.kind==="deep"?"협업 심화학습 결과":(j.kind==="youtube"?"유튜브 영상 학습 결과":"웹페이지 학습 결과")) });
        if (pr){ j.proposalId = pr.id; saveDB(); notifyProposal(pr, appUrl); }
      }catch(e){ logError("proposeAfterLearn", e); }
    }
  }catch(e){
    j.status="error"; j.error=String(e.message||e).slice(0,300); j.doneAt=Date.now(); saveDB();
    const who = (j.kind==="deep") ? "협업 심화학습"
              : (j.dept==="auto") ? "자동배정 학습"
              : (AGENTS[j.dept] ? AGENTS[j.dept].kr+" 학습" : String(j.dept));
    kakaoNotify("⚠️ 학습 실패 — "+who+"\n사유: "+j.error).catch(()=>{});
  } finally { if (j.forceFree) endForceFree(); endJobMode(); }
}
// 유료 모드면 학습도 실행 전에 비용을 알려주고 승인받는다 (무료면 그냥 진행)
function gatePaidLearn(res, kind, payload, appUrl){
  if(!paidModeOn()) return false;                     // 무료 → 게이트 없음
  const estKind = (kind==="deep") ? "deep-learn"
                : (kind==="youtube") ? "learn-youtube"
                : (kind==="benchmark") ? "learn-web" : "learn-web";
  const est = estimateJob(estKind, {});
  est.paid = est.paid.length ? est.paid : ["유료 Gemini 키"];
  est.freeAlternative = "무료 Gemini 키로 진행(속도·한도 제한 있을 수 있음)";
  const ap = createCostApproval("learn-"+kind, Object.assign({}, payload, { appUrl:String(appUrl||"") }), est);
  ap.appUrl = String(appUrl||""); saveDB();
  res.json({ ok:true, pendingApproval:true, approvalId:ap.id, estKrw:est.estKrw, paid:est.paid,
    note:"유료 자원이 필요해 승인을 요청했어요. 카카오톡 또는 앱에서 승인·무료우회·취소를 선택하세요." });
  return true;
}
// 유튜브 URL 학습 접수 (즉시 응답 → 백그라운드 분석 → 부서 지식 반영)
app.post("/api/learn/youtube-apply", (req,res)=>{
  try{
    const b=req.body||{};
    const dept=String(b.dept||"auto").trim();
    if(dept!=="auto" && !AGENTS[dept]) return res.status(400).json({ error:"유효한 부서가 필요합니다" });
    const urls=(Array.isArray(b.urls)?b.urls:String(b.urls||"").split("\n"))
      .map(u=>String(u).trim()).filter(u=>/youtube\.com\/watch\?v=|youtu\.be\//.test(u)).slice(0,3);
    if(!urls.length) return res.status(400).json({ error:"유효한 유튜브 URL이 없어요 (youtube.com/watch?v=… 또는 youtu.be/… 형식)" });
    if(gatePaidLearn(res, "youtube", { dept, urls }, b.appUrl)) return;   // 유료면 먼저 승인
    DB.learnJobs = DB.learnJobs || [];
    const job={ id:String(Date.now())+"_"+Math.floor(Math.random()*1000), kind:"youtube", dept, urls, status:"queued", at:Date.now() };
    DB.learnJobs.push(job); trimLearnJobs(); saveDB();
    res.json({ ok:true, jobId:job.id, status:"queued", count:urls.length });
    setImmediate(()=>{ runLearnJob(job.id, String(b.appUrl||"")).catch(e=>logError("runLearnJob", e)); });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 웹 벤치마크 학습도 백그라운드로 접수
app.post("/api/learn/benchmark-start", (req,res)=>{
  try{
    let dept=String((req.body&&req.body.dept)||"auto");
    if(dept==="auto"){   // 팀장이 순번대로 부서를 정함
      const order=Object.keys(DEPT_BENCHMARK);
      dept = order[((DB.benchmarkTurn||0)) % order.length];
      DB.benchmarkTurn = ((DB.benchmarkTurn||0)+1) % order.length;
    }
    if(!AGENTS[dept]) return res.status(400).json({ error:"유효한 부서가 필요합니다" });
    if(gatePaidLearn(res, "benchmark", { dept }, (req.body&&req.body.appUrl))) return;   // 유료면 먼저 승인
    DB.learnJobs = DB.learnJobs || [];
    const job={ id:String(Date.now())+"_"+Math.floor(Math.random()*1000), kind:"benchmark", dept, status:"queued", at:Date.now() };
    DB.learnJobs.push(job); trimLearnJobs(); saveDB();
    res.json({ ok:true, jobId:job.id, status:"queued" });
    setImmediate(()=>{ runLearnJob(job.id, String((req.body&&req.body.appUrl)||"")).catch(e=>logError("runLearnJob", e)); });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.get("/api/learn/status", (req,res)=>{
  const j=(DB.learnJobs||[]).find(x=>String(x.id)===String(req.query.id||""));
  if(!j) return res.status(404).json({ error:"작업을 찾을 수 없어요" });
  res.json({ ok:true, id:j.id, kind:j.kind, dept:j.dept, status:j.status, step:j.step||"", error:j.error||"",
    analyzed:j.analyzed||0, autoDepts:j.autoDepts||null, knowledge:(j.status==="done"?(j.knowledge||""):""),
    result:(j.status==="done" && j.kind==="deep") ? (j.result||null) : null });
});
// ===== 학습 인프라 진단: 구글검색·유튜브·웹수집이 '왜' 실패하는지 진짜 에러를 그대로 보여준다 =====
async function probeGeminiSearch(){
  const key = geminiKey();
  if(!key) return { ok:false, reason:"GEMINI_API_KEY(또는 GOOGLE_API_KEY) 미설정 — Render 환경변수 확인" };
  const models = ["gemini-3.5-flash","gemini-2.5-flash"];
  const tried = [];
  for(const mdl of models){
    try{
      const url = "https://generativelanguage.googleapis.com/v1beta/models/"+mdl+":generateContent";
      const body = { contents:[{ parts:[{ text:"한국 전라남도 고흥의 특산물을 한 문장으로 알려줘." }] }], tools:[{ google_search:{} }], generationConfig:{ maxOutputTokens:200 } };
      const r = await fetch(url, { method:"POST", headers:{ "Content-Type":"application/json", "x-goog-api-key":key }, body: JSON.stringify(body) });
      const raw = await r.text(); let dj={}; try{ dj=JSON.parse(raw); }catch(_){}
      if(dj.error){ tried.push({ model:mdl, http:r.status, ok:false, code:String(dj.error.status||dj.error.code||""), reason:String(dj.error.message||"").slice(0,280) }); continue; }
      const cand=(dj.candidates||[])[0]||{}; const parts=(cand.content||{}).parts||[];
      const t=parts.map(p=>p.text||"").join("").trim();
      const grounded = !!(((cand.groundingMetadata||{}).groundingChunks)||[]).length;
      tried.push({ model:mdl, http:r.status, ok:!!t, grounded, sample:t.slice(0,120) });
      if(t) return { ok:true, tried };
    }catch(e){ tried.push({ model:mdl, ok:false, reason:String(e.message||e).slice(0,280) }); }
  }
  return { ok:false, tried };
}
async function probeYouTube(u){
  const key = geminiKey(); if(!key) return { ok:false, reason:"키 없음" };
  if(!/youtube\.com\/watch\?v=|youtu\.be\//.test(String(u||""))) return { ok:false, reason:"유효한 유튜브 URL 아님" };
  try{
    const url="https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent";
    const body={ contents:[{ parts:[ {fileData:{fileUri:u}}, {text:"이 영상의 주제를 한 문장으로 요약해."} ] }], generationConfig:{maxOutputTokens:150} };
    const r=await fetch(url,{method:"POST",headers:{"Content-Type":"application/json","x-goog-api-key":key},body:JSON.stringify(body)});
    const raw=await r.text(); let dj={}; try{dj=JSON.parse(raw);}catch(_){}
    if(dj.error) return { ok:false, http:r.status, code:String(dj.error.status||dj.error.code||""), reason:String(dj.error.message||"").slice(0,280) };
    const t=(((dj.candidates||[])[0]||{}).content||{}).parts||[]; const s=t.map(p=>p.text||"").join("").trim();
    return { ok:!!s, http:r.status, sample:s.slice(0,120) };
  }catch(e){ return { ok:false, reason:String(e.message||e).slice(0,280) }; }
}
async function probeWeb(u){
  try{ const h=await fetchPageText(u); return { ok:!!(h&&h.length>200), len:(h||"").length }; }
  catch(e){ return { ok:false, reason:String(e.message||e).slice(0,280) }; }
}
// GET /api/learn/diagnose?yt=<유튜브URL>&web=<웹URL>  — 셋 다 실제로 때려보고 진짜 원인 반환
app.get("/api/learn/diagnose", async (req,res)=>{
  const ytUrl = String((req.query&&req.query.yt)||"").trim();
  const webUrl = String((req.query&&req.query.web)||"https://www.goheung.go.kr").trim();
  const out = {
    at: Date.now(),
    key: { present: !!geminiKey(),
           source: process.env.GEMINI_API_KEY ? "GEMINI_API_KEY" : (process.env.GOOGLE_API_KEY ? "GOOGLE_API_KEY" : "없음"),
           paidAvailable: paidKeyAvailable(), paidModeOn: paidModeOn() },
    cooldown: { active: Date.now()<geminiCooldownUntil, untilSec: Math.max(0,Math.ceil((geminiCooldownUntil-Date.now())/1000)), streak: gemini429Streak },
    searchesToday: (typeof searchesToday==="function") ? searchesToday() : 0
  };
  try{ out.search = await probeGeminiSearch(); }catch(e){ out.search={ ok:false, reason:String(e.message||e).slice(0,280) }; }
  try{ const u = await freeSearchUrls("고흥 특산물 유자 마케팅", 5); out.freeSearch = { ok:u.length>0, count:u.length, urls:u.slice(0,3) }; }catch(e){ out.freeSearch={ ok:false, reason:String(e.message||e).slice(0,200) }; }
  if(ytUrl){ try{ out.youtube = await probeYouTube(ytUrl); }catch(e){ out.youtube={ ok:false, reason:String(e.message||e).slice(0,280) }; } }
  try{ out.web = await probeWeb(webUrl); }catch(e){ out.web={ ok:false, reason:String(e.message||e).slice(0,280) }; }
  res.json({ ok:true, diagnose: out });
});
// 유료(grounding) 웹조사 예상 비용 — 소비 토큰·구글검색 요금을 원화로 산출해 보여준다
app.get("/api/research/cost", (req,res)=>{
  const rates = geminiRates();                 // per 1M USD (in/out)
  const fx = geminiUsdKrw();
  const GQ_USD = +(process.env.GROUNDING_PER_QUERY_USD || 0.035);   // 구글검색 grounding 1건 $0.035
  const FREE_GQ = +(process.env.GROUNDING_FREE_PER_DAY || 1500);    // 결제 프로젝트 무료 1,500건/일
  const today = todayStr();
  const gd = (DB.geminiDaily && DB.geminiDaily.date===today) ? DB.geminiDaily : { in:0, out:0, calls:0 };
  const gSearch = searchesToday();
  const tokKrw = (tin,tout)=> Math.round(((tin/1e6)*rates.inM + (tout/1e6)*rates.outM) * fx);
  const calls = gd.calls||0;
  const avgIn = calls ? Math.round(gd.in/calls) : 400;    // 조사 1회 추정 입력 토큰
  const avgOut = calls ? Math.round(gd.out/calls) : 1400; // 조사 1회 추정 출력 토큰
  const perCallTokenKrw = tokKrw(avgIn, avgOut);
  const perCallGroundKrw = Math.round(GQ_USD * fx);
  const todayTokenKrw = tokKrw(gd.in, gd.out);
  const paidGround = Math.max(0, gSearch - FREE_GQ);
  const todayGroundKrw = Math.round(paidGround * GQ_USD * fx);
  res.json({ ok:true,
    rates:{ inPerM:rates.inM, outPerM:rates.outM, usdKrw:fx, groundingPerQueryUsd:GQ_USD, freeGroundingPerDay:FREE_GQ },
    today:{ groundingSearches:gSearch, tokIn:gd.in, tokOut:gd.out, calls },
    perCall:{ avgIn, avgOut, tokenKrw:perCallTokenKrw, groundingKrw:perCallGroundKrw, totalKrw:perCallTokenKrw+perCallGroundKrw },
    todayCost:{ tokenKrw:todayTokenKrw, groundingKrw:todayGroundKrw, totalKrw:todayTokenKrw+todayGroundKrw, freeGroundingLeft:Math.max(0, FREE_GQ-gSearch) }
  });
});
// 학습 이력 조회: 언제·어떤 영상/페이지로·무슨 결과를 냈는지 (유튜브는 영상별 상세 포함)
app.get("/api/learn/history", (req,res)=>{
  const dept = String((req.query&&req.query.dept)||"").trim();
  let list = (DB.learnLog||[]);
  if(dept && dept!=="auto" && dept!=="ops") list = list.filter(x=>x.dept===dept);
  res.json({ ok:true, rows: list.slice(-20).reverse() });
});
// 현재 부서 지식(품질 공식) 조회
app.get("/api/learn/knowledge", (req,res)=>{
  const dept=String(req.query.dept||"");
  if(dept){ const k=(DB.deptKnowledge||{})[dept]; return res.json({ ok:true, dept, knowledge:(k&&k.text)||"", at:(k&&k.at)||0, source:(k&&k.benchmark)||"" }); }
  res.json({ ok:true, all: Object.keys(DB.deptKnowledge||{}).map(d=>({ dept:d, source:(DB.deptKnowledge[d].benchmark||""), at:DB.deptKnowledge[d].at||0 })) });
});
// ===== 웹페이지(홈페이지·상세페이지) URL을 직접 학습시켜 부서 전문성에 반영 =====
async function fetchDirect(url){
  const ctrl = new AbortController();
  const to = setTimeout(()=>{ try{ctrl.abort();}catch(_){}}, 22000);
  try{
    const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36";
    const r = await fetch(url, { signal:ctrl.signal, redirect:"follow", headers:{
      "User-Agent": UA,
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8"
    }});
    if(!r.ok) throw new Error("HTTP "+r.status);
    let html = await r.text();
    // 분석에 방해되는 것 제거(스크립트/주석/base64), 구조·카피·스타일 힌트는 보존
    html = html.replace(/<script[\s\S]*?<\/script>/gi, " ")
               .replace(/<noscript[\s\S]*?<\/noscript>/gi, " ")
               .replace(/<!--[\s\S]*?-->/g, " ")
               .replace(/data:image\/[a-z]+;base64,[A-Za-z0-9+/=]+/gi, "[이미지]")
               .replace(/\s{2,}/g, " ");
    return html.slice(0, 60000);   // 토큰 보호
  } finally { clearTimeout(to); }
}
// 리더 프록시(Jina Reader) — 봇 차단·JS 전용 페이지도 대신 렌더해 깨끗한 텍스트로 반환(무료, 키 선택)
async function fetchViaReader(url){
  const ctrl = new AbortController();
  const to = setTimeout(()=>{ try{ctrl.abort();}catch(_){}}, 30000);
  try{
    const headers = { "Accept":"text/plain", "X-Return-Format":"text", "X-Timeout":"25" };
    if(process.env.JINA_API_KEY) headers["Authorization"] = "Bearer "+process.env.JINA_API_KEY;   // 있으면 한도 상향(없어도 무료 동작)
    const r = await fetch("https://r.jina.ai/"+url, { signal:ctrl.signal, redirect:"follow", headers });
    if(!r.ok) throw new Error("reader HTTP "+r.status);
    const t = await r.text();
    return String(t||"").replace(/\s{3,}/g," ").slice(0, 60000);
  } finally { clearTimeout(to); }
}
// 페이지 본문 가져오기: ① 직접 → 부족/차단 시 ② 리더 프록시로 폴백(봇 차단 커머스 사이트 대응)
async function fetchPageText(url){
  let direct = "";
  try{ direct = await fetchDirect(url); }catch(e){ direct = ""; }
  if(direct && direct.length > 500) return direct;              // 직접 본문이 충분하면 사용
  try{ const rd = await fetchViaReader(url); if(rd && rd.length > 200) return rd; }catch(_){}   // 차단·JS전용 → 리더
  if(direct && direct.length > 200) return direct;              // 리더도 실패 시 직접본문이라도
  throw new Error("페이지를 가져오지 못했어요 (봇 차단·JS 전용·로그인 필요일 수 있어요)");
}
// ===== 무료 웹조사 계층 =====
// DuckDuckGo 결과 HTML에서 결과 URL 추출(리다이렉트 uddg 디코딩 + 직접링크 폴백)
function extractDdgUrls(html){
  const out=[], seen={};
  const re=/[?&]uddg=([^&"']+)/g; let m;
  while((m=re.exec(html))){ try{ const u=decodeURIComponent(m[1]); if(/^https?:\/\//.test(u)&&!/duckduckgo\.com/.test(u)&&!seen[u]){ seen[u]=1; out.push(u);} }catch(_){} }
  if(out.length<3){ const re2=/href="(https?:\/\/[^"]+)"/g; let m2; while((m2=re2.exec(html))){ const u=m2[1]; if(!/duckduckgo\.com|w3\.org/.test(u)&&!seen[u]){ seen[u]=1; out.push(u);} } }
  return out;
}
// 무료 웹검색: 검색어 → 결과 URL 목록. 여러 무료 제공자를 순차 시도, 실패 시 [].
async function freeSearchUrls(query, n){
  const want = n||5;
  const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36";
  const providers = [
    "https://lite.duckduckgo.com/lite/?q="+encodeURIComponent(query),
    "https://html.duckduckgo.com/html/?q="+encodeURIComponent(query)
  ];
  for(const purl of providers){
    const ctrl=new AbortController(); const to=setTimeout(()=>{try{ctrl.abort();}catch(_){}}, 12000);
    try{
      const r = await fetch(purl, { signal:ctrl.signal, redirect:"follow", headers:{ "User-Agent":UA, "Accept":"text/html,application/xhtml+xml", "Accept-Language":"ko-KR,ko;q=0.9,en;q=0.8" }});
      if(!r.ok){ clearTimeout(to); continue; }
      const html = await r.text(); clearTimeout(to);
      const urls = extractDdgUrls(html).slice(0, want);
      if(urls.length) return urls;
    }catch(e){ clearTimeout(to); }
  }
  return [];
}
// 통합 웹조사: 모드에 따라 ①(유료)grounding 먼저 → ②무료검색+본문+종합 → ③모델기억. 하드 실패 없음.
//  mode: "paid"=grounding 시도(결제/유료키 필요, 진짜 구글검색) / "free"=grounding 건너뛰고 바로 무료검색(비용·429 없음)
async function researchWeb(query, maxTok, mode){
  const want = maxTok || 1400;
  const m = mode || ((DB.state && DB.state.researchMode) || "free");
  let groundedText = "", groundErr = "";
  // ① 유료 모드에서만 grounding 시도 (무료 모드는 429 낭비·비용 없이 곧장 무료검색)
  if (m === "paid"){
    try{
      const g = await geminiSearch(query, want);
      const txt = String((g&&g.text)||"").trim();
      const src = (g&&g.sources)||[];
      if(txt && src.length) return { text:txt, sources:src, via:"grounding" };
      groundedText = txt;   // 근거 없이 텍스트만 = 모델기억 → 무료검색 먼저 시도 후 폴백
    }catch(e){ groundErr = String(e.message||e).slice(0,120); }
  }
  // ② 무료 웹검색 → 상위 페이지 본문 읽기 → Claude가 사실 위주로 종합
  try{
    const urls = await freeSearchUrls(query, 5);
    if(urls.length){
      const picked = urls.slice(0,3), pages=[];
      for(const u of picked){ try{ const h=await fetchPageText(u); if(h && h.length>200) pages.push("[출처] "+u+"\n"+h.slice(0,12000)); }catch(_){} }
      if(pages.length){
        const sys = "아래는 '"+query+"'로 실제 웹에서 찾은 페이지들이다. 핵심 사실만 근거와 함께 한국어로 정리하라. 특히 '무엇이 이것을 고품질로 만드는가'의 구체 요소(구조·구성·디자인·카피·수치 기준)를 뽑아라. 추측 금지, 페이지에 있는 사실만.";
        const summary = await anthropic(sys, pages.join("\n\n---\n\n").slice(0,24000), want);
        if(summary && summary.trim()) return { text:summary.trim(), sources:picked.map(u=>({ title:u, uri:u })), via:"free-web" };
      }
    }
  }catch(e){ /* 무료검색 실패 → 모델기억 폴백 */ }
  // ③ (유료 모드에서) grounding이 준 모델기억 텍스트라도 있으면 사용
  if(groundedText) return { text:groundedText, sources:[], via:"model-grounding" };
  // ④ 최종: 순수 모델기억
  try{ const t = await geminiText("아래 주제를 아는 범위에서 핵심만 정리하라(지금은 웹조사가 불가한 상태): "+query, want); if(t && t.trim()) return { text:t.trim(), sources:[], via:"model" }; }catch(_){}
  throw new Error("web-research 실패"+(groundErr?(": "+groundErr):""));
}
async function learnFromWebUrls(dept, urls, setStep){
  const _step = (typeof setStep==="function")?setStep:function(){};
  const a = AGENTS[dept];
  if(!a) throw new Error("유효한 부서가 아닙니다");
  const list = (urls||[]).slice(0,3);
  if(!list.length) throw new Error("분석할 웹페이지 URL이 없습니다");
  const notes = [];
  let _i=0;
  for (const u of list){
    _i++; _step("웹페이지 "+_i+"/"+list.length+" 읽는 중");
    try{
      const html = await fetchPageText(u);
      if(html && html.length>200) notes.push("[페이지] "+u+"\n"+html);
    }catch(e){ /* 실패한 URL은 건너뜀 */ }
  }
  if(!notes.length) throw new Error("페이지를 가져오지 못했어요 (접속 차단·로그인 필요·잘못된 주소)");
  _step("웹페이지 품질 공식 추출 중");
  const prior = knowledgeText(dept);
  const sys = "너는 민앤팜(고흥 특산물)의 '"+a.no+" "+a.kr+"' 부서 수석 전문가다. 아래는 실제 웹페이지의 HTML이다. "
    + "이 페이지가 '무엇 때문에 고품질인지'를 분석해, 우리가 결과물을 만들 때 그대로 적용할 구체적 품질 공식·체크리스트를 뽑아라. "
    + "특히 관찰하라: 섹션 구성과 순서, 첫 화면(히어로)의 구조, 카피의 어조·문장 길이, CTA 위치·문구, 이미지와 텍스트 비율, 여백·타이포그래피 힌트(클래스명·인라인 스타일), 스크롤 흐름. "
    + "HTML 코드를 그대로 베끼지 말고, '왜 좋은지'의 원리와 재사용 가능한 기준으로 바꿔 써라. "
    + "기존 축적 전문성과 통합·발전시켜라. 형식(이 형식만, 한국어):\n핵심 품질 공식: (5~8개 체크리스트)\n왜 통하나(원리): (위 공식이 왜 통하는지 12살도 이해하게 2~3줄로 — 이유를 알아야 응용 가능)\n반드시 지킬 것: (3~5개)\n피해야 할 것: (2~4개)\n다음 학습 과제: (조사하다 아직 확실치 않은 빈틈 1~2개)";
  const ctx = "[기존 축적 전문성]\n"+(prior||"(아직 없음)")+"\n\n[분석할 실제 웹페이지]\n"+notes.join("\n\n---\n\n");
  const formula = await anthropic(sys, ctx, 1800);
  commitKnowledge(dept, formula, "웹페이지 직접 학습("+notes.length+"개)");
  DB.deptMemory = DB.deptMemory || {}; DB.deptMemory[dept] = DB.deptMemory[dept]||[];
  DB.deptMemory[dept].push({ at:Date.now(), instruction:"[웹페이지 학습]", note:notes.length+"개 분석 → 품질 공식 갱신" });
  if(DB.deptMemory[dept].length>40) DB.deptMemory[dept]=DB.deptMemory[dept].slice(-40);
  DB.exp = DB.exp || {}; DB.exp[dept] = (DB.exp[dept]||0) + 3;
  saveDB();
  await absorbToLeader([dept], "웹페이지");   // 팀장이 이 부서 학습을 흡수
  recordLearnLog({ dept, deptKr:(AGENTS[dept]?AGENTS[dept].kr:dept), kind:"web", ok:true, analyzed:notes.length, urls:(urls||[]).slice(0,4), note:notes.length+"개 웹페이지 분석 → 품질 공식 갱신" });
  return { ok:true, dept, analyzed:notes.length, knowledge:formula };
}
app.post("/api/learn/web-apply", (req,res)=>{
  try{
    const b=req.body||{};
    const dept=String(b.dept||"auto").trim();
    if(dept!=="auto" && !AGENTS[dept]) return res.status(400).json({ error:"유효한 부서가 필요합니다" });
    const urls=(Array.isArray(b.urls)?b.urls:String(b.urls||"").split("\n"))
      .map(u=>String(u).trim()).filter(u=>/^https?:\/\/.+/.test(u)).slice(0,3);
    if(!urls.length) return res.status(400).json({ error:"유효한 웹페이지 URL이 없어요 (https:// 로 시작)" });
    if(gatePaidLearn(res, "web", { dept, urls }, b.appUrl)) return;   // 유료면 먼저 승인
    DB.learnJobs = DB.learnJobs || [];
    const job={ id:String(Date.now())+"_"+Math.floor(Math.random()*1000), kind:"web", dept, urls, status:"queued", at:Date.now() };
    DB.learnJobs.push(job); trimLearnJobs(); saveDB();
    res.json({ ok:true, jobId:job.id, status:"queued", count:urls.length });
    setImmediate(()=>{ runLearnJob(job.id, String(b.appUrl||"")).catch(e=>logError("runLearnJob", e)); });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 학습 작업 목록(학습 탭에서 이력 표시)
app.get("/api/learn/list", (req,res)=>{
  res.json({ ok:true, jobs:(DB.learnJobs||[]).slice(-12).map(j=>({
    id:j.id, kind:j.kind, dept:j.dept, status:j.status, at:j.at, doneAt:j.doneAt||0, startedAt:j.startedAt||0, step:j.step||"",
    error:j.error||"", analyzed:j.analyzed||0, urls:(j.urls||[]).length })) });
});
// ===== 🏢 전 부서 협업 심화 학습 =====
// 팀장 배정 → 부서별 가이드라인 → 제작부 예제 → 검수부 채점 → (부족하면 2라운드) → 부서 지식 갱신
function stripFences(t){ return String(t||"").replace(/^```[a-z]*\s*/i,"").replace(/```\s*$/,"").trim(); }
function safeJson(t, fallback){
  try{ return JSON.parse(stripFences(t)); }
  catch(e){
    const m = String(t||"").match(/\{[\s\S]*\}/);
    if(m){ try{ return JSON.parse(m[0]); }catch(_){} }
    return fallback;
  }
}
// 채점 텍스트에서 0~100 점수를 안정적으로 추출(JSON이 아니어도 '72/100','점수: 72','72점','Score: 72' 등 커버)
function extractScore(t){
  const s = String(t||"");
  if(!s) return null;
  const pats = [
    /(\d{1,3})\s*\/\s*100\b/,                 // 72/100
    /점수[^\d]{0,8}(\d{1,3})/,                 // 점수: 72 / 점수는 72
    /score[^\d]{0,8}(\d{1,3})/i,              // Score: 72
    /(\d{1,3})\s*점(?!\s*(?:이하|이상|만점))/,   // 72점 (‘100점 만점’ 류 제외)
    /(\d{1,3})\s*(?:out of|\/)\s*100/i
  ];
  for(const p of pats){ const m=s.match(p); if(m){ const n=+m[1]; if(n>=0&&n<=100) return n; } }
  // 라벨이 없을 때 마지막 수단: 리스트 개수(가지/개/편 등)로 오인되지 않는 0~100 숫자
  const m2 = s.match(/\b(\d{1,3})\b(?!\s*(?:가지|개|편|명|위|번|라운드|round|%|퍼센트))/i);
  if(m2){ const n=+m2[1]; if(n>=0&&n<=100) return n; }
  return null;
}
// 검수부 채점 파싱: JSON 우선 → 실패 시 텍스트에서 점수 추출. strengths/gaps는 가능한 만큼 보존.
// 채점 텍스트에서 '부족 항목(gaps)'을 추출 — JSON이 깨졌을 때도 2라운드 방향을 잡을 수 있게
function extractGaps(t){
  const s = String(t||"").replace(/\r/g,"");
  const m = s.match(/(?:부족한?\s*점?|부족분|개선점?|보완|gaps?)\s*[:：]?\s*([\s\S]*?)(?:\n\s*(?:강점|점수|score|코칭|총평|strengths)\s*[:：\s]|$)/i);
  const seg = m ? m[1] : "";
  const out = [];
  seg.split(/\n|·|•|;|、|,|\d+[.)]\s*|(?:^|\s)-\s+/m)
     .map(x=>x.replace(/^[\s"'\-–—•·]+|[\s"']+$/g,"").trim())
     .filter(x=>x && x.length>=4 && !/^\d+$/.test(x))
     .slice(0,6).forEach(x=>out.push(x.slice(0,140)));
  return out;
}
function parseReviewScore(raw, fallback){
  const clamp = n => Math.max(0, Math.min(100, Math.round(+n||0)));
  const j = safeJson(raw, null);
  const strengths = (j && Array.isArray(j.strengths)) ? j.strengths : [];
  let gaps = (j && Array.isArray(j.gaps)) ? j.gaps.filter(Boolean) : [];
  if (!gaps.length){ const eg = extractGaps(raw); if (eg.length) gaps = eg; }   // JSON에 gaps 없으면 텍스트에서 추출
  if (j && j.score!=null && !isNaN(+j.score)) return { score:clamp(j.score), strengths, gaps, parsed:"json" };
  const s = extractScore(raw);
  if (s!=null) return { score:clamp(s), strengths, gaps, parsed:"regex" };
  return fallback || { score:0, strengths:[], gaps:["채점 결과를 해석하지 못했어요"], parsed:"fail" };
}
async function gatherMaterial(urls){
  const isYt = (u)=>/youtube\.com\/watch\?v=|youtu\.be\//.test(u);
  const yts = urls.filter(isYt).slice(0,2), webs = urls.filter(u=>!isYt(u)).slice(0,2);
  const parts=[], errs=[];
  for(const u of yts){
    try{ const a=await analyzeYouTube(u, "이 영상의 화면 구성·편집 리듬·자막 스타일·도입 후킹·마무리 CTA를 관찰한 사실 위주로 상세히 분석하라.", 1400);
      if(a&&a.text) parts.push("[유튜브 영상] "+u+"\n"+a.text);
      else errs.push(u+" → 분석 결과 없음"); }
    catch(e){ errs.push(u+" → "+String(e.message||e).slice(0,90)); }
  }
  for(const u of webs){
    try{ const h=await fetchPageText(u);
      if(h&&h.length>200) parts.push("[웹페이지] "+u+"\n"+h.slice(0,40000));
      else errs.push(u+" → 본문이 너무 짧음(차단 페이지일 수 있음)"); }
    catch(e){ errs.push(u+" → "+String(e.message||e).slice(0,90)); }
  }
  if(!parts.length) throw new Error("자료를 가져오지 못했어요 — "+(errs.join(" | ").slice(0,320)||"원인 미상"));
  const kind = yts.length ? (webs.length ? "혼합" : "영상") : "웹페이지";
  return { text: parts.join("\n\n---\n\n"), kind };
}
const DEEP_DEPTS = ["strategy","creation","publishing","analytics","monetization","advisory","scout","editing","graphic"];
async function deepLearnPipeline(job){
  const setStep = (s)=>{ try{ job.step=s; saveDB(); }catch(_){} };
  setStep("자료 수집·분석 중");
  const mat = await gatherMaterial(job.urls||[]);
  const material = mat.text.slice(0, 24000);   // 이후 여러 부서가 참조하므로 입력 절약

  // 1) 팀장 오세라: 학습 목표 + 참여 부서 배정
  setStep("팀장(오세라)이 학습 목표·부서 배정 중");
  const ops = AGENTS.ops;
  const sysA = "너는 민앤팜(고흥 특산물)의 '"+ops.no+" "+ops.kr+"' 팀장 오세라다."+ADDRESS
    + " 아래 참고자료("+mat.kind+")를 보고 팀 전체의 학습을 설계하라. "
    + "반드시 JSON만 출력(설명·머리말·마크다운 금지). 형식:\n"
    + '{"goal":"이 자료에서 우리가 배워야 할 핵심 한 줄","depts":[{"dept":"strategy","focus":"이 부서가 집중해서 볼 관점 한 줄"}]}\n'
    + "dept는 반드시 다음 중에서 2~3개만 고르되, 자료 성격에 실제로 기여할 부서만: strategy, publishing, analytics, monetization, scout, editing, graphic. "
    + "(제작·검수 부서는 뒤 단계에서 자동 참여하므로 여기 넣지 마라.)";
  const rawA = await anthropic(sysA, "[참고자료]\n"+material, 900);
  const defA = { goal:"참고자료의 고품질 요소 흡수", depts:[{dept:"strategy",focus:"구성과 메시지 전략"},{dept:"monetization",focus:"전환·설득 구조"}] };
  const assign = safeJson(rawA, defA);
  let picked = (Array.isArray(assign.depts)?assign.depts:[])
    .filter(x=>x&&DEEP_DEPTS.indexOf(x.dept)>=0 && x.dept!=="creation" && x.dept!=="advisory").slice(0,3);
  if(!picked.length) picked = defA.depts;
  const goal = String(assign.goal||defA.goal).slice(0,200);

  // 2) 배정 부서별 가이드라인 도출
  const guides = {};
  for (const p of picked){
    const a = AGENTS[p.dept]; if(!a) continue;
    setStep(a.kr+" 부서가 가이드라인 도출 중");
    const prior = knowledgeText(p.dept);
    const sys = "너는 민앤팜의 '"+a.no+" "+a.kr+"' 부서 전문가다. 역할: "+a.role+ADDRESS+personaLine(p.dept)
      + (prior?("\n\n[기존 축적 전문성]\n"+prior):"")
      + "\n\n[팀장 지시] 학습 목표: "+goal+" / 우리 부서 집중 관점: "+(p.focus||"")
      + "\n아래 참고자료를 우리 부서 관점에서 분석해, 앞으로 결과물에 적용할 '실행 가이드라인'을 5~7개 항목으로 뽑아라. 추상론 금지, 구체적·검증 가능하게. 한국어로 목록만.";
    guides[p.dept] = await genText(sys, "[참고자료]\n"+material.slice(0,16000), 900, workEngine());
  }

  // 3) 제작부: 배운 걸 즉시 적용한 '예제' 제작
  const cre = AGENTS.creation;
  const guideBlock = Object.keys(guides).map(d=>"["+AGENTS[d].kr+" 가이드라인]\n"+guides[d]).join("\n\n");
  const genreHint = (mat.kind==="영상")
    ? "출처가 영상이므로 '30초 영상 기획안 + 컷별 콘티(3~5컷)'를 예제로 만들어라."
    : "출처가 웹페이지이므로 '핵심 섹션만 담은 간결한 HTML 상품/소개 페이지 예제'를 만들어라(완전한 <!DOCTYPE html> 단일 문서, CSS 인라인).";
  const sysC = "너는 민앤팜의 '"+cre.no+" "+cre.kr+"' 부서다."+ADDRESS+personaLine("creation")
    + (knowledgeText("creation")?("\n\n[기존 전문성]\n"+knowledgeText("creation")):"")
    + "\n\n[학습 목표] "+goal+"\n\n"+guideBlock
    + "\n\n위 가이드라인을 실제로 적용해 고흥 특산물 소재의 '예제 결과물'을 만들어라. "+genreHint
    + " 설명 없이 결과물만 출력."+profileContext();
  let round = 1;
  setStep("제작부가 예제 결과물 만드는 중 (1라운드)");
  let example = await anthropic(sysC, "[참고자료 요약]\n"+material.slice(0,8000), 3000);
  example = stripFences(example);

  // 4) 검수부: 원본 기준 채점 + 부족분
  const adv = AGENTS.advisory;
  const sysR = "너는 민앤팜의 '"+adv.no+" "+adv.kr+"' 부서 서다은이다."+ADDRESS
    + " 아래 '참고자료(벤치마크)'의 수준을 기준으로 '우리 예제'를 냉정하게 채점하라. 후하게 주지 마라. "
    + "반드시 JSON만 출력: {\"score\":0~100, \"strengths\":[\"...\"], \"gaps\":[\"부족한 점을 고칠 수 있게 구체적으로\"]}. "
    + "점수가 90 미만이면 gaps를 반드시 1개 이상, '무엇을 어떻게 고쳐야 하는지' 실행 가능하게 채워라(빈 배열 금지).";
  setStep("검수부(서다은)가 채점 중 (1라운드)");
  const rawR = await anthropic(sysR, "[참고자료(벤치마크)]\n"+material.slice(0,10000)+"\n\n[우리 예제]\n"+example.slice(0,10000), 900);
  let review = parseReviewScore(rawR, { score:0, strengths:[], gaps:["채점 결과를 해석하지 못했어요"] });
  let score = review.score;

  // 5) 점수가 낮으면 2라운드: 지적사항 반영 → 재제작 → 재채점 (진짜 자가개선)
  if (score < 80){
    round = 2;
    setStep("제작부가 지적사항 반영해 재제작 중 (2라운드)");
    const gapsList = (review.gaps||[]).filter(Boolean);
    const gapsTxt = gapsList.length
      ? gapsList.map((g,i)=>(i+1)+". "+g).join("\n")
      : "· 참고자료(벤치마크) 대비 우리 예제에서 약한 부분을 스스로 찾아 구체적으로 개선하라: 구성·정보 위계, 여백·타이포, 카피의 설득력, CTA 명확성, 디테일·완성도. 현재 "+score+"점 → 85점 이상을 목표로 확실히 끌어올려라.";
    // 파서가 라벨 없는 산문 지적('0~2초 훅 부재' 등)을 놓칠 수 있어, 검수 원문도 함께 주입(중복은 무해)
    const rawFeedback = String(rawR||"").replace(/```[a-z]*|```/g,"").replace(/\s{2,}/g," ").trim().slice(0,1200);
    const fixBlock = "[검수부 지적사항 — 아래를 하나도 빠짐없이 반영해 고쳐라]\n"+gapsTxt
      + (rawFeedback ? ("\n\n[검수부 원문 피드백(구체 지시 포함) — 여기 적힌 훅 문구·수치 노출 타이밍·UI 가시화 등 모든 지시를 실제로 반영]\n"+rawFeedback) : "");
    const example2 = stripFences(await anthropic(
      sysC + "\n\n"+fixBlock,
      "[이전 예제]\n"+example.slice(0,9000)+"\n\n위 예제를 위 지적사항대로 구체적으로 고쳐 더 높은 수준으로 다시 만들어라. 지적된 항목은 반드시 결과물에서 확인 가능해야 한다.", 3000));
    setStep("검수부가 재채점 중 (2라운드)");
    const rawR2 = await anthropic(sysR, "[참고자료(벤치마크)]\n"+material.slice(0,10000)+"\n\n[우리 예제]\n"+example2.slice(0,10000), 900);
    const review2 = parseReviewScore(rawR2, review);
    const score2 = review2.score;
    if (score2 >= score){ example = example2; review = review2; score = score2; }
  }

  // 6) 팀장 종합 → 참여 부서별 '품질 공식'을 지식에 갱신 (부족분은 다음 학습 과제로)
  const scored = (review.parsed !== "fail");   // 채점을 실제로 읽었는가(파싱 실패=false)
  setStep("팀장이 종합해 각 부서 지식 갱신 중");
  const parts = picked.map(p=>p.dept).concat(["creation","advisory"]);
  const uniq = parts.filter((v,i)=>parts.indexOf(v)===i);
  const priorBlock = uniq.map(d=>"["+AGENTS[d].kr+" 기존 전문성]\n"+(knowledgeText(d)||"(없음)")).join("\n\n");
  const sysF = "너는 민앤팜 팀장 오세라다."+ADDRESS
    + " 이번 학습(자료 분석 → 가이드라인 → 예제 제작 → 검수 채점)의 결과를 바탕으로, 참여 부서 각각의 '품질 공식'을 갱신하라. "
    + "검수에서 지적된 부족분은 반드시 각 부서의 '다음 학습 과제'에 반영하라. "
    + "각 부서 블록을 정확히 아래 형식으로만 출력(다른 말 금지):\n"
    + uniq.map(d=>"===dept:"+d+"===\n핵심 품질 공식: (5~8개 체크리스트)\n반드시 지킬 것: (3~5개)\n피해야 할 것: (2~4개)\n다음 학습 과제: (1~2개)").join("\n\n");
  const reviewBlock = scored
    ? ("[검수 채점] 점수 "+score+"/100\n강점: "+((review.strengths||[]).join(" / "))+"\n부족: "+((review.gaps||[]).join(" / ")))
    : "[검수 채점] 이번엔 채점 형식 오류로 점수를 산출하지 못했다. 점수·부족분에 의존하지 말고, 위 '부서 가이드라인'의 품질을 그대로 통합·발전시켜라. 없는 부족분을 지어내지 마라.";
  const rawF = await anthropic(sysF,
    "[학습 목표] "+goal+"\n\n"+priorBlock+"\n\n[부서 가이드라인]\n"+guideBlock.slice(0,6000)
    +"\n\n"+reviewBlock, 2600);

  // 블록 파싱 → 각 부서 지식 저장
  const updated = [];
  for (const d of uniq){
    const re = new RegExp("===dept:"+d+"===([\\s\\S]*?)(?====dept:|$)");
    const m = String(rawF).match(re);
    const text = m ? m[1].trim() : "";
    if(!text) continue;
    DB.deptKnowledge = DB.deptKnowledge || {};
    DB.deptKnowledge[d] = { text, at:Date.now(), basis:(DB.deptMemory[d]||[]).length,
      exp:(DB.exp&&DB.exp[d])||0, benchmark:"협업 심화학습("+mat.kind+", "+round+"라운드, "+score+"점)", learned:true };
    DB.deptMemory = DB.deptMemory||{}; DB.deptMemory[d] = DB.deptMemory[d]||[];
    DB.deptMemory[d].push({ at:Date.now(), instruction:"[협업 심화학습]", note:goal.slice(0,80)+" → "+score+"점" });
    if(DB.deptMemory[d].length>40) DB.deptMemory[d]=DB.deptMemory[d].slice(-40);
    DB.exp = DB.exp||{}; DB.exp[d] = (DB.exp[d]||0) + 4;   // 협업 심화학습은 +4
    updated.push(d);
  }
  saveDB();
  setStep("팀장이 전 부서 학습을 흡수해 총괄 기준 갱신 중");
  await absorbToLeader(updated, "협업 심화학습");
  recordLearnLog({ dept:"ops", deptKr:"전 부서(팀장)", kind:"deep", ok:true, score:(scored?score:null),
    analyzed:(picked||[]).length, note:"협업 심화학습("+mat.kind+") — "+(scored?(score+"점 · "):"")+updated.length+"개 부서 갱신", goal:String(goal||"").slice(0,80) });
  saveDB();
  return { ok:true, goal, kind:mat.kind, round, score, scored,
    depts: picked.map(p=>({ dept:p.dept, kr:AGENTS[p.dept].kr, focus:p.focus||"" })),
    guides, example, review, updated: updated.map(d=>AGENTS[d].kr) };
}
app.post("/api/learn/deep-start", (req,res)=>{
  try{
    const b=req.body||{};
    const urls=(Array.isArray(b.urls)?b.urls:String(b.urls||"").split("\n"))
      .map(u=>String(u).trim()).filter(u=>/^https?:\/\/.+/.test(u)).slice(0,4);
    if(!urls.length) return res.status(400).json({ error:"유효한 URL이 없어요 (유튜브 또는 웹페이지 주소)" });
    if(gatePaidLearn(res, "deep", { urls }, b.appUrl)) return;   // 유료면 먼저 승인
    DB.learnJobs = DB.learnJobs || [];
    const job={ id:String(Date.now())+"_"+Math.floor(Math.random()*1000), kind:"deep", dept:"ops", urls, status:"queued", step:"대기 중", at:Date.now() };
    DB.learnJobs.push(job); trimLearnJobs(); saveDB();
    res.json({ ok:true, jobId:job.id, status:"queued" });
    setImmediate(()=>{ runLearnJob(job.id, String(b.appUrl||"")).catch(e=>logError("deep-learn", e)); });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// ===== 팀장(오세라)은 모든 부서의 학습을 흡수해 '총괄 품질 기준'으로 승격한다 =====
function allDeptKnowledgeDigest(limit){
  const k = DB.deptKnowledge || {};
  return Object.keys(k)
    .filter(d => d!=="ops" && AGENTS[d] && k[d] && k[d].text)
    .map(d => "· ["+AGENTS[d].kr+"] "+String(k[d].text).replace(/\s+/g," ").slice(0, limit||420))
    .join("\n");
}
// 부서가 새로 배울 때마다 팀장이 그 지식을 통합(상위 원칙으로 승격 + 부서별 특수 기준 보존)
async function absorbToLeader(depts, sourceLabel){
  try{
    const list = (Array.isArray(depts)?depts:[depts]).filter(d=> d && d!=="ops" && AGENTS[d]);
    if(!list.length) return false;
    DB.deptKnowledge = DB.deptKnowledge || {};
    const newBlocks = list
      .map(d => "["+AGENTS[d].kr+" 부서가 새로 배운 품질 공식]\n"+((DB.deptKnowledge[d]&&DB.deptKnowledge[d].text)||""))
      .filter(b => b.split("\n")[1] && b.split("\n")[1].trim())
      .join("\n\n");
    if(!newBlocks.trim()) return false;
    const ops = AGENTS.ops;
    const prior = knowledgeText("ops");
    const sys = "너는 민앤팜(고흥 특산물)의 팀장 오세라('"+ops.no+" "+ops.kr+"')다."+ADDRESS
      + " 팀장은 모든 부서의 전문성을 흡수해 팀 전체에서 가장 높은 판단 기준을 갖는다. "
      + "아래 [팀장 기존 종합 기준]에 각 부서가 새로 배운 품질 공식을 통합하라. "
      + "단순 나열 금지 — (1) 여러 부서에 공통되는 원리는 '총괄 품질 기준'으로 승격하고, (2) 부서 간 충돌하는 지침은 팀장이 판단해 정리하고, (3) 부서 고유의 특수 기준은 부서별 항목으로 보존하라. "
      + "형식(이 형식만, 한국어):\n"
      + "총괄 품질 기준: (팀 전체 결과물에 적용할 상위 원칙 5~8개)\n"
      + "부서별 핵심: (부서명 — 그 부서가 반드시 지킬 1~2줄씩)\n"
      + "검수 체크리스트: (팀장이 결과물을 볼 때 확인할 5~7개)\n"
      + "다음 팀 학습 과제: (1~2개)";
    const text = await anthropic(sys, "[팀장 기존 종합 기준]\n"+(prior||"(아직 없음)")+"\n\n"+newBlocks, 2200);
    DB.deptKnowledge.ops = { text, at:Date.now(), basis:((DB.deptMemory&&DB.deptMemory.ops)||[]).length,
      exp:(DB.exp&&DB.exp.ops)||0, benchmark:"부서 학습 흡수("+(sourceLabel||"")+")", learned:true };
    DB.deptMemory = DB.deptMemory || {}; DB.deptMemory.ops = DB.deptMemory.ops || [];
    DB.deptMemory.ops.push({ at:Date.now(), instruction:"[팀장 흡수 학습]",
      note: list.map(d=>AGENTS[d].kr).join(", ")+" 지식 통합 → 총괄 기준 갱신" });
    if(DB.deptMemory.ops.length>40) DB.deptMemory.ops = DB.deptMemory.ops.slice(-40);
    DB.exp = DB.exp || {}; DB.exp.ops = (DB.exp.ops||0) + 2;
    saveDB();
    console.log("팀장 흡수 학습 완료: "+list.join(","));
    return true;
  }catch(e){ logError("absorbToLeader", e); return false; }
}
// ===== 팀장의 '플랫폼 변경 제안' — 승인 없이는 절대 적용되지 않음 =====
// 학습으로 새 기준을 얻었을 때, 그걸 제대로 쓰려면 앱 자체를 바꿔야 하는 경우 팀장이 승인을 요청한다.
async function proposePlatformChange(ctx, opts){
  try{
    opts = opts || {};
    DB.patchProposals = DB.patchProposals || [];
    if (DB.patchProposals.filter(p=>p.status==="pending").length >= 3) return null;   // 승인 적체 방지
    const ops = AGENTS.ops;
    const sys = "너는 이 SNS 자동화 플랫폼의 팀장 오세라('"+ops.no+" "+ops.kr+"')다."+ADDRESS
      + " 너는 가장 높은 자유도와 판단 권한을 가진다.\n\n"+APP_ENV_DOC+"\n\n"+BACKEND_DOC
      + "\n\n[너의 판단 과제]\n아래 내용을 근거로, '플랫폼 자체를 바꾸면 팀의 성능·품질이 실제로 좋아지는가'를 판단하라. "
      + "불가능한 경우에만 제안하는 게 아니라, **더 좋아질 수 있다고 판단되면 먼저 제안**하라.\n\n"
      + "[제안 종류]\n"
      + "· 앱 화면(입력칸·표시·워크플로) 개선 → kind='frontend-patch' : 실제 js/css 코드를 작성하라(승인 시 즉시 적용).\n"
      + "· 서버 로직·프롬프트·파이프라인 개선 → kind='backend-change' : 패치로 못 바꾸므로 코드 대신 **개발 가이드라인**을 작성하라. "
      + "위 [백엔드 구조]의 실제 함수명을 정확히 지목하고, 개발자가 그대로 작업할 수 있게 써라.\n"
      + "· 프롬프트·지식만으로 이미 해결되는 것, 사소한 색상 변경은 제안하지 마라(needed:false).\n\n"
      + "반드시 아래 JSON 하나만 출력(마크다운 펜스·설명 금지):\n"
      + '{"needed":false} 또는\n'
      + '{"needed":true,"kind":"frontend-patch","title":"짧은 제목","desc":"1~2문장","reason":"근거 연결 1문장","impact":"기대 효과 1문장","js":"IIFE, 내부 try/catch","css":"없으면 빈 문자열"}\n'
      + '또는\n'
      + '{"needed":true,"kind":"backend-change","title":"짧은 제목","desc":"1~2문장","reason":"근거 연결 1문장","impact":"기대 효과 1문장",'
      + '"guideline":{'
      + '"target":"수정 대상 — 예: server.js > work() 의 maxTok 설정부",'
      + '"current":"현재 동작(코드 근거)",'
      + '"change":"무엇을 어떻게 바꾸는지 단계별로",'
      + '"codeSketch":"바꿀 코드의 핵심 스니펫 또는 의사코드",'
      + '"verify":"바꾼 뒤 무엇을 보면 개선을 확인할 수 있는지",'
      + '"risk":"부작용·주의점",'
      + '"rollback":"문제 시 되돌리는 방법"}}'
      + "\n주의: js는 STATE를 파괴하지 않고 try/catch로 감싼다. guideline은 추측 말고 위 [백엔드 구조]에 있는 실제 이름만 써라."
      + (guidelineLogText() ? ("\n\n[내가 과거에 낸 제안과 운영자의 결정 — 승인된 유형은 따르고, 거절된 유형은 반복하지 마라. 이미 제안한 것을 또 내지 마라]\n"+guidelineLogText()) : "");
    const out = await anthropic(sys, "["+(opts.label||"판단 근거")+"]\n"+String(ctx||"").slice(0,5000), 3000);
    const p = safeJson(out, { needed:false });
    if (!p || !p.needed) return null;
    const kind = (p.kind==="backend-change") ? "backend-change" : "frontend-patch";
    let guideline = null, backendRequest = "";
    if (kind==="frontend-patch"){
      if (!p.js || typeof p.js !== "string") return null;
      try { new Function(p.js); } catch(e){ logError("proposePatch:문법오류", e); return null; }   // 깨진 코드는 제안 자체를 버림
      if (p.js.length > 60000) return null;
    } else {
      const g = p.guideline || {};
      const need = (v)=>String(v||"").trim().length >= 10;
      if (!need(g.target) || !need(g.change)) return null;   // 실행 불가능한 두루뭉술한 제안은 버림
      guideline = {
        target:String(g.target||"").slice(0,300),
        current:String(g.current||"").slice(0,800),
        change:String(g.change||"").slice(0,1500),
        codeSketch:String(g.codeSketch||"").slice(0,2000),
        verify:String(g.verify||"").slice(0,500),
        risk:String(g.risk||"").slice(0,500),
        rollback:String(g.rollback||"").slice(0,400)
      };
      // 그대로 복사해 개발에 넘길 수 있는 요청서 텍스트 생성
      backendRequest =
        "【백엔드 개발 가이드라인】 "+String(p.title||"")+"\n\n"
        +"■ 배경\n"+String(p.reason||"")+"\n"+String(p.desc||"")+"\n\n"
        +"■ 수정 대상\n"+guideline.target+"\n\n"
        +"■ 현재 동작\n"+(guideline.current||"(기재 없음)")+"\n\n"
        +"■ 변경 내용\n"+guideline.change+"\n\n"
        +(guideline.codeSketch ? ("■ 코드 스케치\n"+guideline.codeSketch+"\n\n") : "")
        +"■ 검증 방법\n"+(guideline.verify||"(기재 없음)")+"\n\n"
        +"■ 위험·주의\n"+(guideline.risk||"(기재 없음)")+"\n\n"
        +"■ 롤백\n"+(guideline.rollback||"(기재 없음)")+"\n\n"
        +"■ 기대 효과\n"+String(p.impact||"");
    }
    const proposal = {
      id: Date.now(), code: crypto.randomBytes(6).toString("hex"), kind,
      title: String(p.title||"이름 없는 변경").slice(0,60),
      desc: String(p.desc||"").slice(0,300),
      reason: String(p.reason||"").slice(0,200),
      impact: String(p.impact||"").slice(0,200),
      js: kind==="frontend-patch" ? p.js : "",
      css: kind==="frontend-patch" ? String(p.css||"").slice(0,20000) : "",
      guideline: guideline,
      backendRequest: backendRequest,
      status:"pending", at: Date.now(), source: String(opts.label||"").slice(0,60)
    };
    DB.patchProposals.push(proposal);
    if (DB.patchProposals.length>15) DB.patchProposals = DB.patchProposals.slice(-15);
    DB.deptMemory = DB.deptMemory||{}; DB.deptMemory.ops = DB.deptMemory.ops||[];
    DB.deptMemory.ops.push({ at:Date.now(), instruction:"[플랫폼 변경 제안]", note:proposal.title+" ("+kind+") — 승인 대기" });
    DB.exp = DB.exp||{}; DB.exp.ops = (DB.exp.ops||0)+1;
    pushGuidelineLog(proposal);   // 판단 이력에 남겨 다음 판단의 재료로
    saveDB();
    return proposal;
  }catch(e){ logError("proposePlatformChange", e); return null; }
}
function notifyProposal(pr, appUrl){
  const base = String(process.env.SELF_URL || process.env.RENDER_EXTERNAL_URL || "").replace(/\/+$/,"");
  const links = base
    ? ("\n\n승인: "+base+"/api/patch/approve?id="+pr.id+"&code="+pr.code
      +"\n거절: "+base+"/api/patch/reject?id="+pr.id+"&code="+pr.code)
    : "\n\n앱의 📚 학습 탭에서 승인/거절할 수 있어요.";
  const kindTxt = (pr.kind==="backend-change") ? "📝 백엔드 변경 요청(자동 적용 불가)" : "🛠 앱 화면 변경(승인 시 즉시 적용)";
  kakaoNotify("팀장이 플랫폼 변경 승인을 요청했어요\n"+kindTxt+"\n\n["+pr.title+"]\n"+pr.desc
    +"\n\n근거: "+pr.reason+(pr.impact?("\n기대 효과: "+pr.impact):"")
    +"\n\n⚠️ 승인하기 전에는 적용되지 않습니다."+links, String(appUrl||"")).catch(()=>{});
}
// 승인/거절 — 카톡 링크(코드 검증) 방식
app.get("/api/patch/approve", (req,res)=>{
  const pr=(DB.patchProposals||[]).find(x=>String(x.id)===String(req.query.id) && x.code===req.query.code && x.status==="pending");
  if(!pr) return res.send("<meta charset=utf-8>유효하지 않거나 이미 처리된 요청입니다.");
  const applied = applyProposal(pr);
  res.send("<meta charset=utf-8><pre>"+(applied? ("✅ 승인되어 적용했습니다.\n\n"+pr.title+"\n"+pr.desc+"\n\n앱을 새로고침하면 반영됩니다.") : "적용에 실패했어요.")+"</pre>");
});
app.get("/api/patch/reject", (req,res)=>{
  const pr=(DB.patchProposals||[]).find(x=>String(x.id)===String(req.query.id) && x.code===req.query.code && x.status==="pending");
  if(!pr) return res.send("<meta charset=utf-8>유효하지 않거나 이미 처리된 요청입니다.");
  pr.status="rejected"; pr.decidedAt=Date.now(); markGuidelineDecided(pr.id,"rejected"); saveDB();
  res.send("<meta charset=utf-8><pre>거절했습니다. 플랫폼은 변경되지 않았어요.\n(팀장이 이 결정을 학습해 다음 제안이 더 좋아집니다)</pre>");
});
function applyProposal(pr){
  try{
    if (pr.kind === "backend-change"){
      // 백엔드 변경은 패치로 자동 적용 불가 — '승인됨(반영 대기)'으로 표시하고 요청서를 남긴다
      pr.status="approved"; pr.decidedAt=Date.now();
      DB.exp = DB.exp||{}; DB.exp.ops=(DB.exp.ops||0)+1;
      markGuidelineDecided(pr.id, "approved"); saveDB();
      kakaoNotify("📝 백엔드 변경 요청 승인됨 — "+pr.title+"\n⚠️ 자동 적용은 불가합니다. 아래 요청서를 개발에 반영해 주세요.\n\n"+String(pr.backendRequest||"").slice(0,700)).catch(()=>{});
      return true;
    }
    DB.patches = DB.patches || [];
    if (DB.patches.length >= 20) return false;
    DB.patches.push({ id:Date.now(), title:pr.title, desc:pr.desc, js:pr.js, css:pr.css||"",
      enabled:true, at:Date.now(), request:"[팀장 제안] "+pr.reason });
    pr.status="approved"; pr.decidedAt=Date.now();
    DB.exp = DB.exp||{}; DB.exp.ops = (DB.exp.ops||0)+2;
    markGuidelineDecided(pr.id, "approved"); saveDB();
    kakaoNotify("✅ 플랫폼 변경 적용됨 — "+pr.title+"\n앱을 새로고침하면 반영됩니다.").catch(()=>{});
    return true;
  }catch(e){ logError("applyProposal", e); return false; }
}
// 앱에서 조회·결정
app.get("/api/patch/proposals", (req,res)=>{
  res.json({ ok:true, proposals:(DB.patchProposals||[])
    .filter(p=>p.status==="pending")
    .map(p=>({ id:p.id, kind:p.kind||"frontend-patch", title:p.title, desc:p.desc, reason:p.reason,
      impact:p.impact||"", backendRequest:p.backendRequest||"", guideline:p.guideline||null, at:p.at, source:p.source||"" })) });
});
// 팀장의 자유 제안(또는 운영자 요청)을 '승인 대기 변경 제안'으로 만든다 (승인 전 적용 안 됨)
app.post("/api/patch/propose", async (req,res)=>{
  try{
    const request = String((req.body&&req.body.request)||"").slice(0,3000);
    if(!request.trim()) return res.status(400).json({ error:"제안 내용을 적어주세요" });
    const pr = await proposePlatformChange(request, { label:"운영자·팀장 제안" });
    if(!pr) return res.json({ ok:true, needed:false, note:"팀장 판단: 지금은 플랫폼 변경이 필요하지 않아요(프롬프트·지식으로 충분)." });
    notifyProposal(pr, String((req.body&&req.body.appUrl)||""));
    res.json({ ok:true, needed:true, proposal:{ id:pr.id, kind:pr.kind, title:pr.title, desc:pr.desc, impact:pr.impact } });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.post("/api/patch/decide", (req,res)=>{
  try{
    const b=req.body||{};
    const pr=(DB.patchProposals||[]).find(x=>String(x.id)===String(b.id) && x.status==="pending");
    if(!pr) return res.status(404).json({ error:"대기 중인 요청을 찾을 수 없어요" });
    if(b.approve===true){
      const ok = applyProposal(pr);
      return res.json({ ok, applied:ok, title:pr.title });
    }
    pr.status="rejected"; pr.decidedAt=Date.now(); markGuidelineDecided(pr.id,"rejected"); saveDB();
    res.json({ ok:true, applied:false, title:pr.title });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// ===== 💸 유료 자원 사전 비용 산정 · 승인 · 무료 우회 =====
function claudeRates(){ return { inM:+(process.env.PRICE_IN_PER_M||3), outM:+(process.env.PRICE_OUT_PER_M||15) }; }
function usdKrwRate(){ const st=DB.state||{}; return +(st.usdKrw)||+(process.env.PRICE_USD_KRW||1540); }
function claudeCostKrw(tin, tout){ const r=claudeRates(); return Math.round(((tin/1e6)*r.inM + (tout/1e6)*r.outM)*usdKrwRate()); }
// 작업 종류별 예상 토큰·비용(₩) — 실제 호출 전에 미리 계산
function estimateJob(kind, opts){
  opts = opts || {};
  const rows = [];
  const add = (label, tin, tout, note) => rows.push({ 항목:label, 입력토큰:tin, 출력토큰:tout, 원화:claudeCostKrw(tin,tout), 비고:note||"" });
  if (kind === "product-page"){
    add("상품페이지 HTML 생성", 3000, 8000);
    if (opts.aiImages) rows.push({ 항목:"AI 이미지 생성("+(opts.imageCount||3)+"장)", 입력토큰:0, 출력토큰:0,
      원화: Math.round((opts.imageCount||3) * (+(process.env.PRICE_IMAGE_KRW||60))), 비고:"OpenAI 유료" });
  } else if (kind === "deep-learn"){
    add("팀장 배정", 6000, 900); add("부서 가이드라인 ×3", 16000*3/3, 900*3);
    add("예제 제작", 9000, 3000); add("검수 채점", 12000, 900);
    add("2라운드(점수 낮을 때)", 12000, 3900, "조건부");
    add("팀장 종합·흡수", 8000, 4800);
  } else if (kind === "learn-youtube" || kind === "learn-web"){
    add("자료 분석", 0, 0, kind==="learn-youtube" ? "Gemini 영상분석(무료)" : "웹 수집(무료)");
    add("품질 공식 추출", 12000, 1800);
    add("팀장 흡수", 6000, 2200);
  } else if (kind === "video"){
    add("영상 기획안", 3000, 2200);
    add("Veo 프롬프트 변환", 2000, 1100, "Gemini(무료)");
  } else { add("일반 작업", 2000, 2000); }
  const estKrw = rows.reduce((a,r)=>a+(r.원화||0), 0);
  const paid = [];
  if (opts.aiImages && process.env.OPENAI_API_KEY) paid.push("OpenAI 이미지 생성");
  if (paidModeOn()) paid.push("유료 Gemini 키");
  return { kind, estKrw, rows, paid, freeAlternative: paid.length ? "AI 이미지 없이 [사진 자리]로 표시 / 무료 Gemini 사용" : "" };
}
app.get("/api/cost/estimate", (req,res)=>{
  const kind = String(req.query.kind||"product-page");
  const opts = { aiImages: req.query.aiImages==="1", imageCount:+(req.query.imageCount||3) };
  res.json({ ok:true, ...estimateJob(kind, opts) });
});
// 유료 자원이 필요한 작업은 실행 전 승인 요청 (승인 / 무료 우회 / 취소)
function createCostApproval(kind, payload, est){
  DB.costApprovals = DB.costApprovals || [];
  const ap = { id:Date.now(), code:crypto.randomBytes(6).toString("hex"),
    kind, payload, estKrw:est.estKrw, paid:est.paid, rows:est.rows,
    status:"pending", at:Date.now() };
  DB.costApprovals.push(ap);
  if(DB.costApprovals.length>15) DB.costApprovals = DB.costApprovals.slice(-15);
  saveDB();
  const base = String(process.env.SELF_URL || process.env.RENDER_EXTERNAL_URL || "").replace(/\/+$/,"");
  const links = base ? ("\n\n승인(유료 진행): "+base+"/api/cost/approve?id="+ap.id+"&code="+ap.code
    +"\n무료로 우회: "+base+"/api/cost/bypass?id="+ap.id+"&code="+ap.code
    +"\n취소: "+base+"/api/cost/cancel?id="+ap.id+"&code="+ap.code) : "\n\n앱의 📚 학습 탭에서 결정할 수 있어요.";
  kakaoNotify("💸 유료 자원이 필요한 작업이에요\n\n작업: "+kind+"\n예상 비용: 약 ₩"+ap.estKrw.toLocaleString()
    +"\n유료 항목: "+(ap.paid.join(", ")||"-")
    +"\n\n무료 우회: "+(est.freeAlternative||"-")
    +"\n\n⚠️ 승인 전에는 실행되지 않습니다."+links).catch(()=>{});
  return ap;
}
function runApprovedCost(ap, freeMode){
  try{
    if (String(ap.kind).indexOf("learn-")===0){
      const b = ap.payload || {};
      const kind = String(ap.kind).slice(6);   // youtube | web | deep | benchmark
      DB.learnJobs = DB.learnJobs || [];
      // benchmark는 반드시 실제 부서여야 함(auto면 순번으로 결정)
      let jdept = (kind==="deep") ? "ops" : (b.dept || "auto");
      if (kind==="benchmark" && (!AGENTS[jdept] || jdept==="auto")){
        const order = Object.keys(DEPT_BENCHMARK);
        jdept = order[((DB.benchmarkTurn||0)) % order.length];
        DB.benchmarkTurn = ((DB.benchmarkTurn||0)+1) % order.length;
      }
      const job = { id:String(Date.now())+"_"+Math.floor(Math.random()*1000),
        kind: kind, dept: jdept,
        urls: b.urls||[], status:"queued", step:"대기 중", at:Date.now(),
        forceFree: !!freeMode };   // 무료 우회 선택 시 이 작업만 무료 키 사용
      DB.learnJobs.push(job); trimLearnJobs(); saveDB();
      setImmediate(()=>{ runLearnJob(job.id, ap.appUrl||"").catch(e=>logError("runLearnJob", e)); });
      return job.id;
    }
    if (ap.kind === "product-page"){
      const b = Object.assign({}, ap.payload, freeMode ? { aiImages:false } : {});
      DB.pageJobs = DB.pageJobs || [];
      const job = { id:String(Date.now())+"_"+Math.floor(Math.random()*1000), status:"queued", kind:"create",
        product:b.product, brand:b.brand||"", price:b.price||"", info:b.info||"", photos:b.photos||[],
        input:b, at:Date.now() };
      DB.pageJobs.push(job); trimPageJobs(); saveDB();
      setImmediate(()=>{ runPageJob(job.id, ap.appUrl||"").catch(e=>logError("runPageJob", e)); });
      return job.id;
    }
    return null;
  }catch(e){ logError("runApprovedCost", e); return null; }
}
function decideCost(ap, mode){   // mode: approve | bypass | cancel
  if(mode==="cancel"){ ap.status="cancelled"; ap.decidedAt=Date.now(); saveDB(); return { ok:true, started:false }; }
  const free = (mode==="bypass");
  ap.status = free ? "bypassed" : "approved"; ap.decidedAt=Date.now(); saveDB();
  const jobId = runApprovedCost(ap, free);
  kakaoNotify((free?"🆓 무료로 우회해 진행합니다":"✅ 승인되어 유료로 진행합니다")+" — "+ap.kind).catch(()=>{});
  return { ok:true, started:!!jobId, jobId, free };
}
app.get("/api/cost/approve", (req,res)=>{
  const ap=(DB.costApprovals||[]).find(x=>String(x.id)===String(req.query.id)&&x.code===req.query.code&&x.status==="pending");
  if(!ap) return res.send("<meta charset=utf-8>유효하지 않거나 이미 처리된 요청입니다.");
  const r=decideCost(ap,"approve");
  res.send("<meta charset=utf-8><pre>✅ 승인되어 실행합니다 (약 ₩"+ap.estKrw.toLocaleString()+")\n완료되면 카카오톡으로 알려드려요.</pre>");
});
app.get("/api/cost/bypass", (req,res)=>{
  const ap=(DB.costApprovals||[]).find(x=>String(x.id)===String(req.query.id)&&x.code===req.query.code&&x.status==="pending");
  if(!ap) return res.send("<meta charset=utf-8>유효하지 않거나 이미 처리된 요청입니다.");
  decideCost(ap,"bypass");
  res.send("<meta charset=utf-8><pre>🆓 무료로 우회해 실행합니다.\n(AI 이미지 대신 [사진 자리]로 표시됩니다)</pre>");
});
app.get("/api/cost/cancel", (req,res)=>{
  const ap=(DB.costApprovals||[]).find(x=>String(x.id)===String(req.query.id)&&x.code===req.query.code&&x.status==="pending");
  if(!ap) return res.send("<meta charset=utf-8>유효하지 않거나 이미 처리된 요청입니다.");
  decideCost(ap,"cancel");
  res.send("<meta charset=utf-8><pre>취소했습니다. 비용이 발생하지 않았어요.</pre>");
});
app.get("/api/cost/pending", (req,res)=>{
  res.json({ ok:true, approvals:(DB.costApprovals||[]).filter(a=>a.status==="pending")
    .map(a=>({ id:a.id, kind:a.kind, estKrw:a.estKrw, paid:a.paid, rows:a.rows, at:a.at })) });
});
app.post("/api/cost/decide", (req,res)=>{
  const b=req.body||{};
  const ap=(DB.costApprovals||[]).find(x=>String(x.id)===String(b.id)&&x.status==="pending");
  if(!ap) return res.status(404).json({ error:"대기 중인 요청을 찾을 수 없어요" });
  const mode = (b.mode==="approve"||b.mode==="bypass"||b.mode==="cancel") ? b.mode : "cancel";
  res.json(Object.assign({ kind:ap.kind }, decideCost(ap, mode)));
});
// ===== 👑 팀장의 자유 제안 (높은 자유도) — 플랫폼 전반을 스스로 살펴보고 개선안을 제시 =====
app.post("/api/ops/suggest", async (req,res)=>{
  try{
    const focus = String((req.body&&req.body.focus)||"").slice(0,200);
    const ops = AGENTS.ops;
    const knowN = Object.keys(DB.deptKnowledge||{});
    const expTxt = Object.keys(DB.exp||{}).map(d=>(AGENTS[d]?AGENTS[d].kr:d)+":"+DB.exp[d]).join(", ");
    const recent = (DB.pageJobs||[]).slice(-3).map(j=>"· 상품페이지 "+j.product+" ("+j.status+")").join("\n");
    const learns = (DB.learnJobs||[]).slice(-3).map(j=>"· 학습 "+j.kind+" ("+j.status+")").join("\n");
    const digest = allDeptKnowledgeDigest(220);
    const pendingP = (DB.patchProposals||[]).filter(p=>p.status==="pending").length;
    const sys = "너는 이 플랫폼의 팀장 오세라('"+ops.no+" "+ops.kr+"')다."+ADDRESS
      + " 너는 가장 높은 자유도와 판단 권한을 가진다. 아래 플랫폼 현황을 보고, 운영자에게 '먼저 제안'하라. "
      + "요청받은 것만 하지 말고, 지금 이 팀이 더 좋아지려면 무엇을 해야 하는지 스스로 판단해 제시하라.\n"
      + "제안은 실행 가능해야 한다. 각 제안마다: 무엇을 / 왜(근거는 현황에서) / 예상 효과 / 비용이 드는지 여부.\n"
      + "형식(이 형식만, 한국어):\n"
      + "1) [분류] 제목\n   무엇: …\n   왜: …\n   효과: …\n   비용: 무료 또는 유료(대략)\n"
      + "(3~5개. 우선순위 높은 순. 뻔한 소리·일반론 금지, 이 플랫폼의 실제 현황에 근거할 것.)"
      + (focus ? ("\n\n[운영자가 특별히 봐달라는 부분] "+focus) : "");
    const user = "[부서 경험치] "+(expTxt||"없음")
      +"\n[학습된 부서] "+(knowN.join(", ")||"없음")
      +"\n[대기 중 플랫폼 변경 제안] "+pendingP+"건"
      +"\n\n[최근 작업]\n"+(recent||"없음")
      +"\n\n[최근 학습]\n"+(learns||"없음")
      +"\n\n[전 부서 축적 전문성 요약]\n"+(digest||"아직 없음");
    const text = await anthropic(sys, user, 2200);
    DB.deptMemory = DB.deptMemory||{}; DB.deptMemory.ops = DB.deptMemory.ops||[];
    DB.deptMemory.ops.push({ at:Date.now(), instruction:"[팀장 자유 제안]", note:String(text).slice(0,200) });
    if(DB.deptMemory.ops.length>40) DB.deptMemory.ops=DB.deptMemory.ops.slice(-40);
    DB.exp = DB.exp||{}; DB.exp.ops=(DB.exp.ops||0)+1; saveDB();
    res.json({ ok:true, suggestions:text });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 팀장이 '정확한' 백엔드 개선 가이드라인을 쓸 수 있도록, 실제 코드 구조를 알려주는 문서
const BACKEND_DOC = `[백엔드 구조 — server.js 단일 파일, Node.js + Express, Render 무료 플랜, Supabase(agent_state 1행)에 전체 상태 저장]

■ AI 호출
· anthropic(system, user, maxTokens=1500, images) — Claude(claude-sonnet-4-6) 호출. 타임아웃은 maxTokens에 비례(최소 90초, 최대 8분). 429/5xx는 재시도(작업 중엔 40회까지, 평소 4회).
· genText(system, user, maxTok, engine) — engine="claude"|"gemini"
· geminiSearch(prompt, maxTok) — 구글검색 grounding(무료), geminiText(prompt, maxTok)
· analyzeYouTube(url, question, maxTok) — 유튜브 영상 화면+음성 분석(Gemini 멀티모달)
· fetchPageText(url) — 웹페이지 HTML 수집(스크립트 제거, 6만자 제한)

■ 부서 작업
· AGENTS — 8부서 정의 {no, kr, role}. ops가 팀장. MEMBERS/CREW(제작부 3인)
· work(dept, instruction, context, images, teamLog) — 부서 실행. maxTok 2000(ops만 4000). 시스템 프롬프트에 ADDRESS+STYLE+personaLine+profileContext+knowledgeText(dept)+최근기억+타부서기억을 주입
· deptLevel(dept) = floor(exp/5)+1, DB.exp[dept]
· knowledgeText(dept) — 그 부서의 축적 품질 공식(DB.deptKnowledge[dept].text)
· distillKnowledge(dept) — 자기 작업 기록을 압축(외부 학습 아님)

■ 학습 (품질의 핵심)
· DEPT_BENCHMARK — 부서별 벤치마크 주제·검색어·유튜브 검색어
· learnFromBenchmark(dept) — 웹 조사 → 품질 공식 추출 → DB.deptKnowledge[dept] 갱신 (+exp 3)
· learnFromYouTubeUrls(dept, urls) / learnFromWebUrls(dept, urls) — URL 직접 학습
· deepLearnPipeline(job) — 팀장 배정 → 부서 가이드라인 → 제작부 예제 → 검수부 채점 → 80점 미만이면 2라운드 → 부서 지식 갱신
· absorbToLeader(depts, label) — 팀장이 부서 학습을 흡수해 총괄 기준으로 승격
· allDeptKnowledgeDigest(limit) — 팀장 프롬프트에 주입되는 전 부서 지식 요약

■ 결과물 생성
· productPagePipeline(body) — 상품페이지 HTML 생성(maxTok 8000). body.baseHtml+feedback이면 수정 모드
· runPageJob / runLearnJob — 백그라운드 작업 큐. beginJobMode()~endJobMode() 사이에는 한도로 중단되지 않음
· 작업 상태는 DB.pageJobs / DB.learnJobs, 완료 시 kakaoNotify(text, url)

■ 데이터·안전
· loadDB() — Supabase 접속 성공(reached)과 데이터 존재(loaded)를 구분. 접속 실패 시 저장 보류(빈 데이터 덮어쓰기 방지)
· saveDB() — 로컬 파일 + Supabase 이중 저장, dbHasContent(DB)로 빈 데이터 저장 차단
· 스케줄러(setInterval) — 아침/저녁 브리핑, 하루 1회 벤치마크 학습, 성과 수집

■ 비용
· estimateJob(kind, opts) → 예상 ₩, claudeCostKrw(tin,tout)
· 유료 자원(OpenAI 이미지·유료 Gemini)은 createCostApproval()로 승인/무료우회/취소

■ 바꿀 수 없는 것(패치로는 불가) — 위 함수·프롬프트·파이프라인은 모두 server.js 안에 있으며, 프론트엔드 패치로는 수정 불가. 운영자가 server.js를 수정·재배포해야 한다.`;
// ===== 팀장의 '판단 이력' — 가이드라인을 쓰고, 승인/거절 결과로 배운다 =====
function guidelineLogText(){
  const lg = (DB.guidelineLog||[]).slice(-8);
  if(!lg.length) return "";
  return lg.map(g=>{
    const r = g.decided==="approved" ? "✅승인됨" : (g.decided==="rejected" ? "❌거절됨" : "⏳대기");
    return "· ["+r+"] ("+(g.kind==="backend-change"?"백엔드":"앱화면")+") "+g.title+(g.target?(" — 대상: "+g.target):"");
  }).join("\n");
}
function pushGuidelineLog(pr){
  DB.guidelineLog = DB.guidelineLog || [];
  DB.guidelineLog.push({ id:pr.id, at:Date.now(), kind:pr.kind, title:pr.title,
    target:(pr.guideline&&pr.guideline.target)||"", impact:pr.impact||"", source:pr.source||"", decided:null });
  if(DB.guidelineLog.length>20) DB.guidelineLog = DB.guidelineLog.slice(-20);
}
function markGuidelineDecided(id, decided){
  const g = (DB.guidelineLog||[]).find(x=>String(x.id)===String(id));
  if(g){ g.decided = decided; g.decidedAt = Date.now(); }
  // 결정이 3건 쌓일 때마다, 팀장이 '무엇이 좋은 개선 판단인가'를 스스로 정리해 지식으로 승격
  const decidedN = (DB.guidelineLog||[]).filter(x=>x.decided).length;
  if (decidedN > 0 && decidedN % 3 === 0) distillGuidelineKnowledge().catch(e=>logError("distillGuideline", e));
}
// 가이드라인 작성·승인/거절 경험을 '개선 판단 기준'으로 압축해 팀장 지식에 통합 (= 가이드라인 쓰는 것 자체가 학습)
async function distillGuidelineKnowledge(){
  try{
    const lg = (DB.guidelineLog||[]).filter(x=>x.decided);
    if(lg.length < 3) return false;
    const ops = AGENTS.ops;
    const prior = knowledgeText("ops");
    const hist = lg.slice(-10).map(g=>
      "· "+(g.decided==="approved"?"승인":"거절")+" | "+(g.kind==="backend-change"?"백엔드":"앱화면")+" | "+g.title
      +(g.target?(" | 대상:"+g.target):"")+(g.impact?(" | 기대효과:"+g.impact):"")).join("\n");
    const sys = "너는 이 플랫폼의 팀장 오세라('"+ops.no+" "+ops.kr+"')다."+ADDRESS
      + " 너는 지금까지 여러 번 '플랫폼 개선 가이드라인'을 작성했고, 운영자가 승인하거나 거절했다. "
      + "그 결과에서 '무엇이 좋은 개선 판단인가'를 스스로 배워라. 승인된 제안의 공통점과 거절된 제안의 공통점을 찾아, 앞으로의 판단 기준으로 만들어라.\n"
      + "기존 [팀장 종합 기준]에 이 판단 기준을 '플랫폼 개선 판단 기준' 항목으로 통합하되, 기존 내용을 지우지 말고 발전시켜라.\n"
      + "형식(이 형식만, 한국어):\n총괄 품질 기준: (기존 것 유지·정리)\n부서별 핵심: (기존 유지)\n검수 체크리스트: (기존 유지)\n플랫폼 개선 판단 기준: (이번에 배운 것 4~6개 — 언제 제안하고 언제 하지 말아야 하는가)\n다음 팀 학습 과제: (1~2개)";
    const text = await anthropic(sys, "[팀장 기존 종합 기준]\n"+(prior||"(아직 없음)")+"\n\n[내가 낸 제안과 운영자의 결정]\n"+hist, 2400);
    DB.deptKnowledge = DB.deptKnowledge || {};
    DB.deptKnowledge.ops = { text, at:Date.now(), basis:((DB.deptMemory&&DB.deptMemory.ops)||[]).length,
      exp:(DB.exp&&DB.exp.ops)||0, benchmark:"개선 판단 학습("+lg.length+"건 결정)", learned:true };
    DB.deptMemory = DB.deptMemory||{}; DB.deptMemory.ops = DB.deptMemory.ops||[];
    DB.deptMemory.ops.push({ at:Date.now(), instruction:"[개선 판단 학습]", note:lg.length+"건의 승인/거절에서 판단 기준 도출" });
    if(DB.deptMemory.ops.length>40) DB.deptMemory.ops=DB.deptMemory.ops.slice(-40);
    DB.exp = DB.exp||{}; DB.exp.ops=(DB.exp.ops||0)+3;
    saveDB();
    kakaoNotify("🧠 팀장이 '플랫폼 개선 판단 기준'을 스스로 정리했어요\n지금까지의 승인·거절 결과에서 배웠습니다.").catch(()=>{});
    return true;
  }catch(e){ logError("distillGuidelineKnowledge", e); return false; }
}
// 팀장 자가진단 — 시키지 않아도 스스로 플랫폼을 살펴보고 개선 가이드라인을 만든다
async function selfReviewAndPropose(appUrl){
  try{
    if ((DB.patchProposals||[]).filter(p=>p.status==="pending").length >= 2) return null;   // 적체 시 쉬어감
    const expTxt = Object.keys(DB.exp||{}).map(d=>(AGENTS[d]?AGENTS[d].kr:d)+":"+DB.exp[d]).join(", ");
    const knows = Object.keys(DB.deptKnowledge||{});
    const jobs = DB.pageJobs||[], lj = DB.learnJobs||[];
    const fails = jobs.filter(j=>j.status==="error").slice(-3).map(j=>"· 상품페이지 실패: "+(j.error||"")).join("\n");
    const lfails = lj.filter(j=>j.status==="error").slice(-3).map(j=>"· 학습 실패: "+(j.error||"")).join("\n");
    const ctx = "[플랫폼 자가진단 시점] "+new Date().toLocaleString("ko-KR",{timeZone:"Asia/Seoul"})
      +"\n[부서 경험치] "+(expTxt||"없음")
      +"\n[학습된 부서] "+(knows.join(", ")||"없음")
      +"\n[상품페이지 작업] 총 "+jobs.length+"건 (실패 "+jobs.filter(j=>j.status==="error").length+")"
      +"\n[학습 작업] 총 "+lj.length+"건 (실패 "+lj.filter(j=>j.status==="error").length+")"
      +(fails?("\n\n[최근 실패]\n"+fails):"")+(lfails?("\n"+lfails):"")
      +"\n\n[전 부서 축적 전문성 요약]\n"+(allDeptKnowledgeDigest(260)||"아직 없음")
      +"\n\n위 현황에서 '플랫폼을 어떻게 고치면 팀 성능이 좋아지는가'를 스스로 판단하라. 개선이 필요 없으면 needed:false.";
    const pr = await proposePlatformChange(ctx, { label:"팀장 자가진단" });
    if (pr) notifyProposal(pr, appUrl||"");
    return pr;
  }catch(e){ logError("selfReview", e); return null; }
}
app.post("/api/ops/self-review", async (req,res)=>{
  try{
    const pr = await selfReviewAndPropose(String((req.body&&req.body.appUrl)||""));
    if(!pr) return res.json({ ok:true, needed:false, note:"팀장 판단: 지금은 플랫폼 변경이 필요하지 않아요." });
    res.json({ ok:true, needed:true, proposal:{ id:pr.id, kind:pr.kind, title:pr.title, desc:pr.desc, impact:pr.impact } });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 팀장이 지금까지 내린 판단 이력(학습 근거) 조회
app.get("/api/ops/guideline-log", (req,res)=>{
  res.json({ ok:true, log:(DB.guidelineLog||[]).slice(-12).map(g=>({
    title:g.title, kind:g.kind, target:g.target||"", decided:g.decided||"pending", at:g.at, source:g.source||"" })) });
});
// ===== 🤖 자동 배정 학습: URL만 주면 팀장이 부서를 정하고, 각 부서가 자기 관점으로 배운다 =====
// 자료는 한 번만 분석(비용 절약) → 팀장 배정 → 부서별 품질 공식 추출 → 팀장이 한 번에 흡수
async function autoRouteLearn(urls, srcKind, onStep){
  const step = (s)=>{ try{ if(onStep) onStep(s); }catch(_){} };
  step("자료 수집·분석 중");
  const mat = await gatherMaterial(urls||[]);              // 유튜브=화면분석(무료), 웹=HTML수집(무료)
  const material = mat.text.slice(0, 22000);

  // 1) 팀장이 '어느 부서가 무엇을 배워야 하는가' 배정
  step("팀장(오세라)이 배울 부서 배정 중");
  const ops = AGENTS.ops;
  const pool = ["strategy","creation","publishing","analytics","monetization","advisory","scout"];
  const sysA = "너는 민앤팜(고흥 특산물)의 팀장 오세라('"+ops.no+" "+ops.kr+"')다."+ADDRESS
    + " 아래 참고자료("+mat.kind+")를 보고, 우리 팀에서 '어느 부서가 무엇을 배워야 하는지' 정하라. "
    + "자료 성격에 실제로 기여할 부서만 1~3개 고르라(억지로 채우지 마라).\n"
    + "부서: strategy(기획·전략), creation(콘텐츠 제작), publishing(채널 발행), analytics(데이터 분석), monetization(커머스·그로스), advisory(콘텐츠 검수), scout(트렌드 기획)\n"
    + "반드시 JSON만 출력(설명·마크다운 금지):\n"
    + '{"reason":"이 자료에서 배울 핵심 한 줄","depts":[{"dept":"creation","focus":"이 부서가 집중해서 볼 관점 한 줄"}]}';
  const rawA = await anthropic(sysA, "[참고자료]\n"+material.slice(0,12000), 800);
  const defA = { reason:"참고자료의 고품질 요소 흡수", depts:[{dept:(srcKind==="youtube"?"creation":"monetization"), focus:"핵심 구성과 표현"}] };
  const assign = safeJson(rawA, defA);
  let picked = (Array.isArray(assign.depts)?assign.depts:[]).filter(x=>x&&pool.indexOf(x.dept)>=0).slice(0,3);
  if(!picked.length) picked = defA.depts;

  // 2) 배정된 부서가 각자 관점으로 품질 공식 추출 (같은 자료 재사용 → 자료 분석 비용 1회)
  const learned = [];
  for (const p of picked){
    const a = AGENTS[p.dept]; if(!a) continue;
    step(a.kr+" 부서가 학습 중");
    const prior = knowledgeText(p.dept);
    const sys = "너는 민앤팜의 '"+a.no+" "+a.kr+"' 부서 수석 전문가다. 역할: "+a.role+ADDRESS+personaLine(p.dept)
      + " 방금 "+(mat.kind==="영상"?"실제 유튜브 영상을 화면까지 직접 분석":"실제 웹페이지를 분석")+"했다."
      + "\n[팀장 배정] 우리 부서가 집중할 관점: "+(p.focus||"")
      + "\n이 자료에서 '우리 부서가 결과물을 만들 때 그대로 적용할 구체적 품질 공식·체크리스트'를 뽑아, 기존 전문성에 통합·발전시켜라. "
      + "추상적 조언 금지, 바로 실행 가능한 구체 기준으로.\n"
      + "형식(이 형식만, 한국어):\n핵심 품질 공식: (5~8개 체크리스트)\n반드시 지킬 것: (3~5개)\n피해야 할 것: (2~4개)\n다음 학습 과제: (1~2개)";
    const formula = await anthropic(sys, "[기존 축적 전문성]\n"+(prior||"(아직 없음)")+"\n\n[분석한 자료]\n"+material.slice(0,16000), 1800);
    commitKnowledge(p.dept, formula, "자동배정 학습("+mat.kind+")");
    DB.deptMemory = DB.deptMemory||{}; DB.deptMemory[p.dept] = DB.deptMemory[p.dept]||[];
    DB.deptMemory[p.dept].push({ at:Date.now(), instruction:"[자동배정 학습]", note:(p.focus||"").slice(0,80) });
    if(DB.deptMemory[p.dept].length>40) DB.deptMemory[p.dept]=DB.deptMemory[p.dept].slice(-40);
    DB.exp = DB.exp||{}; DB.exp[p.dept] = (DB.exp[p.dept]||0)+3;
    learned.push({ dept:p.dept, kr:a.kr, focus:p.focus||"", knowledge:formula });
    saveDB();
  }
  if(!learned.length) throw new Error("배정된 부서가 없어 학습하지 못했어요");

  // 3) 팀장이 한 번에 흡수 (부서마다 흡수하면 호출 낭비)
  step("팀장이 전 부서 학습을 흡수하는 중");
  await absorbToLeader(learned.map(l=>l.dept), "자동배정 "+mat.kind+" 학습");

  return { ok:true, auto:true, kind:mat.kind, reason:String(assign.reason||"").slice(0,200),
    analyzed:(urls||[]).length, depts:learned.map(l=>({ dept:l.dept, kr:l.kr, focus:l.focus })),
    knowledge: learned.map(l=>"["+l.kr+"]\n"+l.knowledge).join("\n\n———\n\n") };
}
// ===== 🧾 학습 적용 기록: 배운 품질 공식이 '실제로 어떤 결과물에 쓰였는지' 남긴다 (추가 API 호출 없음) =====
function logKnowledgeApplied(dept, what, note){
  try{
    const k = (DB.deptKnowledge||{})[dept];
    if(!k || !k.text) return;                       // 배운 게 없으면 적용 기록도 없음
    DB.appliedLog = DB.appliedLog || [];
    DB.appliedLog.push({
      at: Date.now(), dept,
      deptKr: (AGENTS[dept]?AGENTS[dept].kr:dept),
      what: String(what||"").slice(0,60),           // 무슨 작업에
      note: String(note||"").slice(0,120),          // 무엇을 만들었는지
      source: k.benchmark || "학습",                 // 어떤 학습에서 온 공식인지
      learnedAt: k.at || 0                          // 그 공식이 언제 갱신됐는지
    });
    if(DB.appliedLog.length>200) DB.appliedLog = DB.appliedLog.slice(-200);
  }catch(e){}
}
app.get("/api/learn/applied", (req,res)=>{
  const dept = String(req.query.dept||"");
  let list = (DB.appliedLog||[]);
  if(dept && dept!=="auto") list = list.filter(x=>x.dept===dept);
  const rows = list.slice(-30).reverse();
  // 부서별 요약: 학습 이후 몇 번 적용됐는가
  const summary = {};
  (DB.appliedLog||[]).forEach(x=>{
    const k=(DB.deptKnowledge||{})[x.dept];
    if(!k) return;
    summary[x.dept] = summary[x.dept] || { deptKr:x.deptKr, applied:0, sinceLearn:0, source:k.benchmark||"" };
    summary[x.dept].applied++;
    if(x.at >= (k.at||0)) summary[x.dept].sinceLearn++;   // 최신 공식 이후 적용 횟수
  });
  res.json({ ok:true, rows, summary });
});
// 부서가 쌓은 기록을 압축·갱신해 '전문성'으로 발전시킴(지식이 버려지지 않고 누적·정교화)
async function distillKnowledge(dept, forceDeep){
  try{
    const a=AGENTS[dept]; if(!a) return;
    const arr=DB.deptMemory[dept]||[];
    if(arr.length<3) return;
    const exp=(DB.exp&&DB.exp[dept])||0;
    const deep = forceDeep || (exp>0 && exp%10===0); // 10활동마다 또는 강제 시 '심화 정리'(전체 재검토)
    const prior=knowledgeText(dept);
    const win = deep ? 26 : 16;
    const recent=arr.slice(-win).map(x=>"· "+(x.instruction?("["+x.instruction+"] "):"")+String(x.note||"").slice(0,240)).join("\n");
    // 피드백(회의 평가·팀장 평가)을 우선 학습 신호로 강조
    const fb=arr.filter(x=>/\[(회의 피드백|팀장 평가|콘텐츠 의견)\]/.test(x.instruction||"")).slice(-6)
      .map(x=>"· "+(x.instruction||"")+" → "+String(x.note||"").slice(0,160)).join("\n");
    const sys="너는 SNS 자동화 회사 '"+a.no+" "+a.kr+"' 부서의 지식 관리자다. 역할: "+a.role
      +" 이 부서가 수행·학습·피드백받은 기록을 통합해 '이 부서의 축적 전문성'을 갱신하라. 기존 전문성에 새 기록을 합치되 중복은 통합하고, 실제로 통한 방식은 원칙으로 굳히고, 실패·낮은 평가는 '피해야 할 것'으로 명확히 새겨라. 앞으로 모든 작업에 즉시 적용할, 다른 작업에도 전이 가능한 핵심 노하우로 만들라."
      +(deep?" [심화 정리] 지금은 전체 전문성을 처음부터 재검토하는 시간이다. 낡거나 약한 항목은 과감히 버리고, 가장 가치 있는 노하우만 더 날카롭게 다듬어라.":"")
      +" 한국어로만, 아래 형식으로 "+(deep?16:14)+"줄 이내로 압축 출력(설명·서론 금지):\n핵심 전문성: (이 부서가 잘하게 된 것 3~5개)\n검증된 원칙·노하우: (반복해서 통한 방식 3~5개, 가능하면 재사용 공식/템플릿으로)\n피해야 할 것: (실패·비효율·낮은 평가에서 배운 것 1~3개)\n타깃·시장 인사이트: (고객/시장에 대해 알게 된 것 2~4개)\n최근 성장: (예전 대비 나아진 점 1줄)"
      +profileContext();
    const ctx="[기존 축적 전문성]\n"+(prior||"(아직 없음)")+(fb?"\n\n[중요: 받은 피드백 — 반드시 반영]\n"+fb:"")+"\n\n[새로 쌓인 최근 기록]\n"+recent;
    const out=await genText(sys, ctx, deep?1500:1100, "gemini"); // 학습은 자동·무료(Gemini)
    DB.deptKnowledge=DB.deptKnowledge||{};
    DB.deptKnowledge[dept]={ text:out, at:Date.now(), basis:arr.length, exp, deep: !!deep };
    saveDB();
  }catch(e){ logError("distill:"+dept, e); }
}
function crossDeptMemory(exceptDept){
  const lines = [];
  for (const id of Object.keys(AGENTS)){
    if (id === exceptDept) continue;
    const m = (DB.deptMemory && DB.deptMemory[id]) || [];
    if (!m.length) continue;
    const a = AGENTS[id];
    const recent = m.slice(-2).map(x => "· " + (x.note||"")).join("\n");
    lines.push("[" + a.no + " " + a.kr + (MEMBERS[id]?" "+MEMBERS[id]:"") + "]\n" + recent);
  }
  let out = lines.join("\n\n");
  if (out.length > 2200) out = out.slice(0, 2200) + "…";
  return out;
}

async function work(dept, instruction, context, images, teamLog){
  const a = AGENTS[dept];
  const isQuestion = /[?？]|어때|어떨까|할까요|할까|좋을까|어떤|어떻게|추천|의견|방법|왜|뭐가|무엇|가능|괜찮/.test(instruction||"");
  let sys = "너는 SNS 자동화 회사의 '"+a.no+" "+a.kr+"' 부서 AI 에이전트다. 역할: "+a.role+ADDRESS+STYLE;
  if (MEMBERS[dept] || PERSONA[dept]) sys += " 너의 담당 매니저는 '"+(MEMBERS[dept]||"")+"'이며 성격은 ["+(PERSONA[dept]||"")+"] 이 성격과 말투를 응답 톤에 자연스럽게 녹이되, 전문성과 결과물 품질은 항상 유지하라.";
  const exp = (DB.exp && DB.exp[dept]) || 0;
  if (exp) sys += " 너는 지금까지 "+exp+"회의 자율 학습·업무 경험을 쌓았다.";
  sys += " 이 회사의 모든 부서와 운영자는 하나의 공용 콘솔에서 함께 대화한다. 아래에 최근 팀 전체 대화와 각 부서가 기록한 내용이 그대로 공유되므로, '다른 부서 대화는 볼 수 없다/공유되지 않는다'거나 '캡처해서 붙여달라'고 말하지 마라. 공유된 맥락을 그대로 활용해 이어서 답하라.";
  sys += clientBlock();
  sys += " 다른 부서가 기록·작성한 내용에서 문제·빈틈·리스크가 보이면, 네 전문 영역의 관점에서 적극적으로 의견을 제시하고 구체적으로 보완·개선하라. 단, 월권하여 다른 부서 고유 업무를 통째로 대신하지는 말고, 네 전문성으로 더할 수 있는 부분을 명확히 짚어 보태라.";
  if (isQuestion){
    sys += " 이번 입력은 질문·상담 성격이다. 단정적인 결과물 생산보다, 네 경험과 수집한 자료를 근거로 유연하게 의견·선택지·추천을 제시하라. 모르면 솔직히 말하고 확인 방법을 제안하라. 대화하듯 자연스럽게.";
  } else {
    sys += " 너의 전문 영역 안에서 지시를 실제로 끝까지 수행해 구체적이고 완성된 결과물을 한국어로 내라.";
  }
  sys += " 능동적으로 사고하라: 지시에 드러나지 않은 필요까지 스스로 파악해 먼저 제안하고, 네 전문 영역에서 할 수 있는 것은 끝까지 직접 처리하라. '다른 부서에 전달하세요'처럼 떠넘기지 말고, 존재하지 않는 부서를 지어내지 마라. 본론부터 쓴다.";
  sys += meetingFeedbackInsights();
  sys += profileContext();
  if (!isQuestion) sys += " 지시가 다소 모호하더라도 운영 프로필과 상식으로 합리적으로 가정해 바로 완성된 결과물을 만들고, 사용자에게 무엇을 만들지 되묻지 마라. 정말 불가피할 때만 짧게 1가지만 확인하라.";
  const _kb = knowledgeText(dept); const _lv = deptLevel(dept);
  sys += " 너는 현재 Lv"+_lv+" 숙련도의 부서다." + (_lv>=3 ? " 그동안 축적한 전문성을 바탕으로 더 깊고 차별화된 결과를 내라." : " 기본기를 지키며 일관되게 수행하라.");
  if (_kb) sys += "\n\n[이 부서가 축적한 전문성·노하우(지식 베이스) — 모든 판단·작성의 기반으로 반드시 적용하라]\n" + _kb;
  const _rel = await relevantSmart(dept, instruction, 2);
  if (_rel) sys += "\n\n[이번 지시와 비슷한 과거 작업·수집 자료 — 처음부터 다시 조사하지 말고 이걸 재활용·갱신해 더 빠르고 정확하게 처리하라]\n" + _rel;
  const mem = DB.deptMemory[dept] || [];
  if (mem.length) sys += "\n\n[최근 작업·학습(단기 기억)]\n" + mem.slice(-4).map(x=>"· "+x.note).join("\n");
  const crossMem = crossDeptMemory(dept);
  // 트렌드 소재화(10 탐색·발상): 팀장이 조사해온 최신 트렌드를 직접 이어받아 소재로 가공
  if (dept === "scout"){
    const nr = DB.nightResearch || {};
    const feed = Object.keys(nr).map(k=>{
      const r=nr[k]; if(!r||!r.text) return "";
      return "· ["+(AGENTS[k]?AGENTS[k].kr:k)+"] "+String(r.text).slice(0,160)+(r.searched?" (웹검색)":"");
    }).filter(Boolean).slice(0,6).join("\n");
    if (feed) sys += "\n\n[팀장이 조사해온 최신 트렌드·자료 — 네 임무는 이걸 우리 특산물 콘텐츠에 바로 쓸 '실전 소재'(콘텐츠 아이디어·후킹 문구·릴스 포맷·시즌 앵글)로 가공하는 것이다. 조사를 반복하지 말고 이 재료를 가공하라]\n" + feed;
  }
  if (crossMem) sys += "\n\n[다른 부서들이 최근 기록·작성한 내용 — 팀 전체가 공유한다. 관련되면 참조하고, 문제·빈틈이 보이면 보완하라]\n" + crossMem;
  if (teamLog) sys += "\n\n[최근 팀 전체 대화 — 운영자와 다른 부서들이 이 공용 콘솔에서 나눈 내용. 이미 너에게 공유된 맥락이다. 여기에 이어서 답하고, 관련 있으면 앞 대화를 구체적으로 참조하라]\n" + teamLog;
  if (context) sys += "\n\n[동료 부서들이 지금까지 진행한 협업 맥락 — 반드시 읽고 이어받아라]\n" + context + "\n앞 부서 결과를 중복하지 말고, 거기에 네 전문성을 더해 발전시키거나 빈 부분을 채워라. 필요하면 앞 부서 결과를 구체적으로 언급하며 연결하라.";
  if (images && images.length) sys += "\n\n[운영자가 첨부한 참고 사진/영상 장면이 함께 제공된다. 반드시 그 이미지를 분석해 작업에 반영하라.]";
  let maxTok = 2000;
  if (dept === "ops"){
    sys += "\n\n[너는 이 플랫폼의 실질적 팀장이며, 모든 부서 중 최고 수준의 지식과 가장 넓은 자유도를 가진다. (1) 어느 부서의 산출물이든 검토하고 직접 수정·보완·재작성할 수 있다. (2) 각 부서의 자율수행 성과와 학습 수준(메모리·경험치·기록 품질)을 평가하고, 부서별 자율수행 지시의 조정안을 제안·결정할 수 있다. (3) 플랫폼 기능 자체의 추가·변경(패치 개발)을 설계·생성할 수 있다. (4) 정책·발행·콘텐츠 전반의 방향을 조율한다. 클로드(이 플랫폼의 텍스트·추론 엔진)로 분석·작성·수정을 직접 수행하고, 제미나이로 만들 영상·이미지는 구체적 제작 프롬프트로 핸드오프하라. \'수정할 수 없다/권한이 없다\'고 하지 말고 끝까지 직접 처리하되, 실제 시스템 설정을 임의로 바꿨다고 거짓말하지는 마라.]";
    const _digest = allDeptKnowledgeDigest(420);
    if (_digest) sys += "\n\n[전 부서가 축적한 전문성 요약 — 팀장은 이를 모두 흡수하고 있다. 검토·지시·작성 시 각 부서의 기준을 함께 적용하라]\n" + _digest;
    maxTok = 4000;
  }
  const text = await anthropic(sys, instruction, maxTok, images);
  if (_kb) logKnowledgeApplied(dept, "부서 지시 수행", String(instruction||"").slice(0,80));   // 배운 공식이 이 결과물에 적용됨
  if (!DB.deptMemory[dept]) DB.deptMemory[dept] = [];
  DB.deptMemory[dept].push({ at:Date.now(), instruction, note: text.length>500 ? text.slice(0,500)+"…" : text });
  if (DB.deptMemory[dept].length > 40) DB.deptMemory[dept] = DB.deptMemory[dept].slice(-40);
  DB.exp = DB.exp || {}; DB.exp[dept] = (DB.exp[dept]||0) + 1;
  saveDB();
  if (DB.exp[dept] % 3 === 0) { try{ await distillKnowledge(dept); }catch(e){} } // 4회 활동마다 전문성 압축·발전
  return text;
}

// ===== 토큰 헬퍼 (OAuth refresh 자동 갱신) =====
async function getGoogleToken(){
  if (process.env.YT_ACCESS_TOKEN) return process.env.YT_ACCESS_TOKEN;
  if (!process.env.YT_REFRESH_TOKEN) throw new Error("YT_REFRESH_TOKEN 미설정");
  const r = await fetch("https://oauth2.googleapis.com/token", {
    method:"POST", headers:{ "Content-Type":"application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id: process.env.GOOGLE_CLIENT_ID, client_secret: process.env.GOOGLE_CLIENT_SECRET,
      refresh_token: process.env.YT_REFRESH_TOKEN, grant_type: "refresh_token"
    })
  });
  const d = await r.json();
  if (!d.access_token) throw new Error("구글 토큰 갱신 실패: " + JSON.stringify(d).slice(0,160));
  return d.access_token;
}
let _kakaoCache = { token:"", at:0 };
async function getKakaoToken(){
  if (process.env.KAKAO_ACCESS_TOKEN) return process.env.KAKAO_ACCESS_TOKEN;
  if (_kakaoCache.token && Date.now()-_kakaoCache.at < 5*60*60*1000) return _kakaoCache.token;
  if (!process.env.KAKAO_REFRESH_TOKEN) throw new Error("KAKAO_ACCESS_TOKEN/REFRESH_TOKEN 미설정");
  const _body = {
    grant_type:"refresh_token", client_id:process.env.KAKAO_REST_KEY, refresh_token:process.env.KAKAO_REFRESH_TOKEN
  };
  // 카카오 REST 키의 클라이언트 시크릿이 "사용"이면 반드시 포함해야 함(2025-12 개편으로 기본 활성).
  if (process.env.KAKAO_CLIENT_SECRET) _body.client_secret = process.env.KAKAO_CLIENT_SECRET;
  const r = await fetch("https://kauth.kakao.com/oauth/token", {
    method:"POST", headers:{ "Content-Type":"application/x-www-form-urlencoded" },
    body: new URLSearchParams(_body)
  });
  const d = await r.json();
  if (!d.access_token) throw new Error("카카오 토큰 갱신 실패: " + (d.error_description || d.error || JSON.stringify(d)));
  _kakaoCache = { token:d.access_token, at:Date.now() };
  return d.access_token;
}

// ===== AI 이미지 생성 + 미디어 저장소 =====
async function uploadToDrive(base64, mime, name){
  // 구글 드라이브(구글 원) 업로드 — YT_REFRESH_TOKEN(드라이브 권한 포함) 재사용
  const token = await getGoogleToken();
  const meta = { name: name || ("agent-"+Date.now()+(String(mime).startsWith("video")?".mp4":".png")) };
  if (process.env.GDRIVE_FOLDER_ID) meta.parents=[process.env.GDRIVE_FOLDER_ID];
  const boundary = "agentbnd"+Date.now();
  const pre = "--"+boundary+"\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"+JSON.stringify(meta)
    +"\r\n--"+boundary+"\r\nContent-Type: "+(mime||"application/octet-stream")+"\r\nContent-Transfer-Encoding: base64\r\n\r\n";
  const post = "\r\n--"+boundary+"--";
  const body = Buffer.concat([Buffer.from(pre,"utf8"), Buffer.from(base64,"utf8"), Buffer.from(post,"utf8")]);
  const r = await fetch("https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id", {
    method:"POST",
    headers:{ "Authorization":"Bearer "+token, "Content-Type":"multipart/related; boundary="+boundary },
    body
  });
  const d = await r.json();
  if (!d.id) throw new Error("드라이브 업로드 실패: "+JSON.stringify(d).slice(0,180));
  // 링크 가진 사람 보기 권한
  await fetch("https://www.googleapis.com/drive/v3/files/"+d.id+"/permissions", {
    method:"POST", headers:{ "Authorization":"Bearer "+token, "Content-Type":"application/json" },
    body: JSON.stringify({ role:"reader", type:"anyone" })
  }).catch(()=>{});
  return String(mime||"").startsWith("video")
    ? ("https://drive.google.com/file/d/"+d.id+"/view")
    : ("https://drive.google.com/uc?export=view&id="+d.id);
}
async function uploadMedia(base64, mime, name){
  const store = String(process.env.MEDIA_STORE||"").toLowerCase();
  const driveReady = (process.env.YT_REFRESH_TOKEN || process.env.YT_ACCESS_TOKEN) && process.env.GOOGLE_CLIENT_ID;
  // 1순위: 구글 드라이브(구글 원) — store=drive 이거나, cloudinary로 강제하지 않았고 드라이브 연결됨
  if (store==="drive" || (store!=="cloudinary" && driveReady)) {
    try { return await uploadToDrive(base64, mime, name); }
    catch(e){ if (store==="drive") throw e; /* 실패 시 Cloudinary로 폴백 */ }
  }
  // 2순위: Cloudinary unsigned 업로드 (CLOUDINARY_CLOUD_NAME + CLOUDINARY_UPLOAD_PRESET)
  const cloud = process.env.CLOUDINARY_CLOUD_NAME, preset = process.env.CLOUDINARY_UPLOAD_PRESET;
  if (cloud && preset) {
    const kind = String(mime||"").startsWith("video") ? "video" : "image";
    const form = new URLSearchParams();
    form.set("file", "data:"+(mime||"image/png")+";base64,"+base64);
    form.set("upload_preset", preset);
    const r = await fetch("https://api.cloudinary.com/v1_1/"+cloud+"/"+kind+"/upload", { method:"POST", body:form });
    const d = await r.json();
    if (d.secure_url) return d.secure_url;
    throw new Error("Cloudinary 업로드 실패: "+JSON.stringify(d).slice(0,160));
  }
  throw new Error("미디어 저장소 미설정(구글 드라이브 또는 CLOUDINARY) — 공개 URL을 얻을 수 없습니다");
}
async function generateVideo(prompt){
  // 1순위: Google Gemini(Veo) API (GEMINI_API_KEY) — 서버가 스스로 자동 생성
  const gkey = geminiKey();
  if (gkey) return await generateVideoVeo(prompt, gkey);
  // 2순위: Replicate
  const token = process.env.REPLICATE_API_TOKEN;
  if (token) return await generateVideoReplicate(prompt, token);
  throw new Error("영상 API 미설정 (GEMINI_API_KEY 권장, 또는 REPLICATE_API_TOKEN)");
}
async function generateVideoVeo(prompt, key){
  const model = process.env.GEMINI_VIDEO_MODEL || "veo-3.1-generate-preview";
  const base = "https://generativelanguage.googleapis.com/v1beta";
  const ar = process.env.VIDEO_ASPECT || "9:16";
  // 1) 장시간 작업 시작
  const init = await fetch(base+"/models/"+model+":predictLongRunning?key="+key, {
    method:"POST", headers:{ "Content-Type":"application/json" },
    body: JSON.stringify({ instances:[{ prompt }], parameters:{ aspectRatio:ar } })
  });
  let d = await init.json();
  if (d.error) throw new Error("Veo: "+String(d.error.message||JSON.stringify(d.error)).slice(0,180));
  const opName = d.name;
  if (!opName) throw new Error("Veo: 작업 시작 실패(응답에 name 없음)");
  // 2) 폴링 (최대 약 5분)
  let done=false, resp=null;
  for (let i=0; i<60 && !done; i++){
    await new Promise(r=>setTimeout(r,5000));
    const pr = await fetch(base+"/"+opName+"?key="+key);
    const pd = await pr.json();
    if (pd.error) throw new Error("Veo: "+String(pd.error.message||"").slice(0,180));
    if (pd.done){ done=true; resp=pd.response||pd; }
  }
  if (!done) throw new Error("Veo: 생성 시간초과(5분)");
  // 3) 결과 영상 URI 추출 (응답 형태 방어적으로 처리)
  let uri=null;
  try{
    const gv = resp.generateVideoResponse || resp;
    const arr = gv.generatedSamples || gv.generatedVideos || gv.videos || [];
    const s0 = arr[0] || {};
    uri = (s0.video && (s0.video.uri || s0.video.fileUri)) || s0.uri || null;
  }catch(_){}
  if (!uri) throw new Error("Veo: 결과 영상 URI를 찾지 못함");
  // 4) 파일 다운로드(키 필요) → 공개 URL로 업로드
  const dl = await fetch(uri + (uri.indexOf("?")>=0?"&":"?") + "key="+key);
  if (!dl.ok) throw new Error("Veo: 영상 다운로드 실패("+dl.status+")");
  const b64 = Buffer.from(await dl.arrayBuffer()).toString("base64");
  return await uploadMedia(b64, "video/mp4"); // 공개 URL 반환(Cloudinary 필요)
}
async function generateVideoReplicate(prompt, token){
  const version = process.env.VIDEO_MODEL_VERSION;
  if (!version) throw new Error("VIDEO_MODEL_VERSION 미설정 (Replicate 영상 모델 버전 해시)");
  const init = await fetch("https://api.replicate.com/v1/predictions", {
    method:"POST",
    headers:{ "Authorization":"Bearer "+token, "Content-Type":"application/json" },
    body: JSON.stringify({ version, input:{ prompt } })
  });
  let d = await init.json();
  if (d.error) throw new Error(typeof d.error==="string"?d.error:JSON.stringify(d.error).slice(0,160));
  const getUrl = d.urls && d.urls.get;
  let status = d.status;
  for (let i=0; i<55 && status!=="succeeded" && status!=="failed" && status!=="canceled"; i++){
    await new Promise(r=>setTimeout(r,2000));
    const pr = await fetch(getUrl, { headers:{ "Authorization":"Bearer "+token } });
    d = await pr.json(); status = d.status;
  }
  if (status!=="succeeded") throw new Error("영상 생성 실패/시간초과 ("+status+")");
  let out = d.output;
  if (Array.isArray(out)) out = out[out.length-1];
  if (typeof out!=="string") throw new Error("영상 URL을 받지 못했습니다");
  return out;
}
// ===== Gemini TTS (진짜 성우 음성) =====
const TTS_MODELS = [
  process.env.GEMINI_TTS_MODEL,                 // 사용자가 지정하면 1순위
  "gemini-3.1-flash-tts-preview",               // 2026 현행 문서 기준
  "gemini-2.5-flash-preview-tts",               // 구 명칭(호환)
  "gemini-2.5-flash-tts"
].filter(Boolean);
let TTS_MODEL = TTS_MODELS[0]; // 성공한 모델을 기억해 다음부터 바로 사용
// 전원 여성 캐릭터 — 공식 여성 보이스 중 성격에 맞게 배정
const DEPT_VOICE = {
  strategy:"Kore",         // 한지우 — 침착·단단한 리더 (firm, confident)
  creation:"Leda",         // 이서연 — 발랄·에너지 (youthful, energetic)
  publishing:"Aoede",      // 박하늘 — 시원시원·추진력, 또렷한 여성톤 (breezy, natural)
  analytics:"Erinome",     // 강민서 — 냉철·또렷 (clear, precise)
  monetization:"Despina",  // 윤소희 — 매끄러운 협상가 (smooth, flowing)
  ops:"Gacrux",            // 오세라 — 팀장, 연륜·무게감 (mature, experienced)
  advisory:"Achernar",     // 서다은 — 차분·단정한 서기 (soft, gentle)
  scout:"Laomedeia",       // 서다은 — 톡톡 튀는 발상 (upbeat, lively)
  editing:"Autonoe",       // 노아라 — 빠르고 감각적 (bright, quick)
  graphic:"Callirrhoe"     // 오하늘 — 부드럽고 깔끔 (easy-going, clean)
};
const _ttsCache = new Map(); // key: voice|text → base64 wav (재생성 비용 절감)
function pcmToWavBase64(pcmB64, rate, ch, bits){
  rate=rate||24000; ch=ch||1; bits=bits||16;
  const pcm = Buffer.from(pcmB64, "base64");
  const blockAlign = ch*bits/8, byteRate = rate*blockAlign, dataLen = pcm.length;
  const buf = Buffer.alloc(44+dataLen);
  buf.write("RIFF",0); buf.writeUInt32LE(36+dataLen,4); buf.write("WAVE",8);
  buf.write("fmt ",12); buf.writeUInt32LE(16,16); buf.writeUInt16LE(1,20); buf.writeUInt16LE(ch,22);
  buf.writeUInt32LE(rate,24); buf.writeUInt32LE(byteRate,28); buf.writeUInt16LE(blockAlign,32); buf.writeUInt16LE(bits,34);
  buf.write("data",36); buf.writeUInt32LE(dataLen,40); pcm.copy(buf,44);
  return buf.toString("base64");
}
// Gemini 텍스트 생성(영상 프롬프트 등 협력용)
// 무료 등급(429 한도) 보호: 모든 모델이 한도에 걸리면 잠깐 쉬었다 재개 (자동작업은 멈추지 않고 다음 주기에 재시도)
let geminiCooldownUntil = 0;
let gemini429Streak = 0;
function geminiRateInfo(){ const now=Date.now(); return { cooling: now < geminiCooldownUntil, cooldownUntil: geminiCooldownUntil, secondsLeft: Math.max(0, Math.ceil((geminiCooldownUntil-now)/1000)), streak: gemini429Streak }; }
function isRateErr(msg, code){ return code===429 || /quota|exhaust|RESOURCE_EXHAUSTED|rate limit|too many request|429/i.test(String(msg||"")); }
// ===== Gemini API 사용량·비용·월예산(₩) 추적 (24시간 운용을 예산 안에서) =====
// 무료↔유료 키 전환: 평소 무료(0원), 필요할 때만 유료 키로 (앱 토글 paidGeminiOn)
function geminiKey(){
  const st = DB.state || {};
  const free = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY || "";
  const paid = process.env.GEMINI_API_KEY_PAID || "";
  if (_forceFree > 0) return free;              // 이 작업은 '무료로 우회' 선택됨
  if (st.paidGeminiOn && paid) return paid;
  return free;
}
function paidKeyAvailable(){ return !!process.env.GEMINI_API_KEY_PAID; }
// 특정 작업만 '무료로 우회' 실행할 때 사용 (전역 유료 모드를 건드리지 않음)
let _forceFree = 0;
function beginForceFree(){ _forceFree++; }
function endForceFree(){ _forceFree = Math.max(0, _forceFree-1); }
function paidModeOn(){ const st=DB.state||{}; return !!(st.paidGeminiOn && paidKeyAvailable()); }
function geminiRates(){ return { inM:+(process.env.GEMINI_IN_PER_M||0.3), outM:+(process.env.GEMINI_OUT_PER_M||2.5) }; } // per 1M 토큰(USD, flash 추정)
function geminiUsdKrw(){ const st=DB.state||{}; return +(st.usdKrw)||+(process.env.PRICE_USD_KRW||1540); }
function geminiCostKrw(tin, tout){ const r=geminiRates(); return Math.round(((tin/1e6)*r.inM + (tout/1e6)*r.outM)*geminiUsdKrw()); }
function recordGeminiUsage(um){
  if(!um) return;
  const tin = (um.promptTokenCount||0);
  const tout = (um.candidatesTokenCount||0) + (um.thoughtsTokenCount||0); // 사고 토큰도 출력 과금
  const today = todayStr(), month = today.slice(0,7);
  DB.geminiUsage = DB.geminiUsage || { in:0, out:0, calls:0 };
  DB.geminiUsage.in += tin; DB.geminiUsage.out += tout; DB.geminiUsage.calls += 1;
  if(!DB.geminiDaily || DB.geminiDaily.date!==today) DB.geminiDaily = { date:today, in:0, out:0, calls:0 };
  DB.geminiDaily.in += tin; DB.geminiDaily.out += tout; DB.geminiDaily.calls += 1;
  if(!DB.geminiMonthly || DB.geminiMonthly.month!==month) DB.geminiMonthly = { month, in:0, out:0, calls:0, alerted:"", dayAlerted:"" };
  DB.geminiMonthly.in += tin; DB.geminiMonthly.out += tout; DB.geminiMonthly.calls += 1;
}
function daysInMonth(){ const d=new Date(); return new Date(d.getFullYear(), d.getMonth()+1, 0).getDate(); }
function geminiBudget(){
  const st = DB.state || {};
  const monthLimit = (st.geminiMonthLimitKrw!=null) ? +st.geminiMonthLimitKrw : 100000; // 기본 월 ₩100,000
  const today = todayStr(), month = today.slice(0,7);
  const gm = (DB.geminiMonthly && DB.geminiMonthly.month===month) ? DB.geminiMonthly : { in:0, out:0, calls:0 };
  const gd = (DB.geminiDaily && DB.geminiDaily.date===today) ? DB.geminiDaily : { in:0, out:0, calls:0 };
  const monthKrw = geminiCostKrw(gm.in, gm.out);
  const dayKrw = geminiCostKrw(gd.in, gd.out);
  // 하루 예산 = 월예산을 이 달 일수로 균등 분배 + 그날 미사용분 약간 이월(최대 2배까지 허용)
  const dayAllow = monthLimit>0 ? Math.round(monthLimit / daysInMonth()) : 0;
  const monthOver = monthLimit>0 && monthKrw >= monthLimit;
  const dayOver = dayAllow>0 && dayKrw >= dayAllow*2;   // 그날 평소의 2배까지는 허용(버스트), 그 이상이면 다음날로 페이싱
  return { monthLimit, monthKrw, dayKrw, dayAllow, monthOver, dayOver, calls:(gm.calls||0) };
}
// 자동작업이 Gemini를 호출해도 되는가? (월 상한 초과 OR 하루 페이싱 초과면 잠시 멈춤 — 다음 날/다음 달 자동 재개)
function geminiAutoAllowed(){
  if (!paidModeOn()) return true; // 무료 키 모드: 비용 0원 → 예산 가드 불필요(429 보호만 작동)
  const b = geminiBudget();
  if(b.monthLimit<=0) return true; // 0=무제한
  if(b.monthOver){
    const m = todayStr().slice(0,7);
    if(DB.geminiMonthly && DB.geminiMonthly.alerted!==m){ DB.geminiMonthly.alerted=m; saveDB();
      kakaoNotify("🛑 Gemini 자동작업 월 예산(₩"+b.monthLimit.toLocaleString()+") 도달 — 이번 달 자동작업을 잠시 멈췄어요(추가 과금 0). 다음 달 1일 자동 재개되며, 예산을 올리면 바로 재개돼요.").catch(()=>{});
    }
    return false;
  }
  if(b.dayOver){
    const today = todayStr();
    if(DB.geminiMonthly && DB.geminiMonthly.dayAlerted!==today){ DB.geminiMonthly.dayAlerted=today; saveDB();
      kakaoNotify("⏳ 오늘 Gemini 예산 분배분(약 ₩"+(b.dayAllow*2).toLocaleString()+")을 다 써서, 예산을 고르게 쓰려고 잠시 천천히 돌려요. 내일 자동으로 평소 속도 재개돼요.").catch(()=>{});
    }
    return false;
  }
  return true;
}
// ===== 팀장 전용: 실시간 구글검색(grounding)으로 전 세계 공개 웹 자료 수집 =====
function searchesToday(){ const t=todayStr(); return (DB.geminiSearchDaily && DB.geminiSearchDaily.date===t) ? DB.geminiSearchDaily.n : 0; }
function noteSearch(){ const t=todayStr(); if(!DB.geminiSearchDaily || DB.geminiSearchDaily.date!==t) DB.geminiSearchDaily={date:t,n:0}; DB.geminiSearchDaily.n++; DB.geminiSearchTotal=(DB.geminiSearchTotal||0)+1; }
// 하루 검색 상한(폭주·과금 방지): 무료 5회 / 유료 40회
function searchDailyCap(){
  // 무료 범위에선 우리가 한도를 두지 않는다(구글 무료 한도에 걸리면 쿨다운 후 자동 재시도).
  // 유료 모드에서만 과금 폭주 방지를 위해 상한을 둔다. SEARCH_DAILY_CAP로 강제 지정 가능.
  if (process.env.SEARCH_DAILY_CAP) return +process.env.SEARCH_DAILY_CAP;
  if (paidModeOn()) return +(process.env.SEARCH_DAILY_CAP_PAID || 150);
  return 0;   // 0 = 무제한(무료)
}
function searchAllowedNow(){ const cap = searchDailyCap(); return cap <= 0 ? true : (searchesToday() < cap); }
// 작업(백그라운드 job) 중이면 Gemini 쿨다운을 기다렸다 진행 — 한도 때문에 학습이 죽지 않게
let _cooldownNotifiedUntil = 0;
async function waitGeminiCooldown(){
  if (Date.now() >= geminiCooldownUntil) return;
  const leftSec = Math.ceil((geminiCooldownUntil - Date.now())/1000);
  if (!inJobMode()) throw new Error("무료 한도 대기 중 — 약 "+leftSec+"초 뒤 다시 시도하세요");
  // 작업 중이면 중단하지 않고 기다렸다 자동 재시도. 지연 사실은 한 번만 알림.
  if (geminiCooldownUntil > _cooldownNotifiedUntil){
    _cooldownNotifiedUntil = geminiCooldownUntil;
    const paidHint = (paidKeyAvailable() && !paidModeOn())
      ? "\n\n유료 키가 있어요. 즉시 진행하려면 설정에서 유료 모드를 켜세요(비용 발생)."
      : "";
    kakaoNotify("⏳ 무료 한도에 걸려 잠시 대기합니다\n약 "+Math.max(1,Math.ceil(leftSec/60))+"분 뒤 자동으로 다시 시도해요.\n작업은 취소되지 않았습니다."+paidHint).catch(()=>{});
  }
  const waitMs = Math.min(11*60000, geminiCooldownUntil - Date.now() + 1000);
  console.log("Gemini 무료 한도 대기 "+Math.ceil(waitMs/1000)+"초 → 자동 재시도");
  await _sleep(waitMs);
}
// fetch + 타임아웃(무한 대기 방지). Gemini/외부 호출이 응답 없이 멈추는 걸 막는다.
async function fetchTimeout(url, opts, ms){
  const c = new AbortController();
  const to = setTimeout(()=>{ try{ c.abort(); }catch(_){}}, ms||120000);
  try{ return await fetch(url, Object.assign({}, opts||{}, { signal:c.signal })); }
  catch(e){ if(e && e.name==="AbortError") throw new Error("응답 시간 초과("+Math.round((ms||120000)/1000)+"초)"); throw e; }
  finally{ clearTimeout(to); }
}
// Gemini 무료 쿼터(RESOURCE_EXHAUSTED/429) 소진 시 Claude로 폴백 (실시간 웹검색은 없음 — 모델 지식 기반 best-effort)
async function claudeResearchFallback(prompt, maxTok){
  const sys = "너는 리서치 어시스턴트다. 실시간 웹 접근은 없으니 네가 아는 지식으로 최대한 정확하고 구체적으로 한국어로 답하라. 최신·불확실한 사실은 '추정' 또는 '확인 필요'라고 명시하라. 특히 한우·특산물·SNS 콘텐츠 맥락이면 실무적으로 바로 쓸 수 있게.";
  const t = await anthropic(sys, String(prompt||""), Math.min(maxTok||1400, 1800));
  return String(t||"").trim();
}
async function geminiSearch(prompt, maxTok){
  const key = geminiKey();
  if (!key) throw new Error("GEMINI_API_KEY 미설정");
  await waitGeminiCooldown();
  if (!searchAllowedNow()) throw new Error("오늘 웹검색 한도("+searchDailyCap()+"회) 도달 — 내일 재개하거나 SEARCH_DAILY_CAP를 올리세요");
  const models = ["gemini-2.5-flash","gemini-3.5-flash"]; // 무료티어에선 2.5가 응답, 3.5는 429 잦음 → 2.5 우선
  const want = maxTok || 1400;
  let lastErr=null, rateHits=0;
  for (const mdl of models){
    try{
      const url = "https://generativelanguage.googleapis.com/v1beta/models/"+mdl+":generateContent";
      const body = { contents:[{ parts:[{ text: prompt }] }], tools:[{ google_search:{} }], generationConfig:{ maxOutputTokens: want } };
      const r = await fetchTimeout(url, { method:"POST", headers:{ "Content-Type":"application/json", "x-goog-api-key":key }, body: JSON.stringify(body) }, 150000);
      const dj = await r.json();
      if (dj.error){ lastErr="["+mdl+"] "+(dj.error.status||dj.error.code||r.status)+": "+String(dj.error.message||""); if(isRateErr(dj.error.message, dj.error.code||r.status)) rateHits++; continue; }
      const cand = (dj.candidates||[])[0]||{};
      const parts = (cand.content||{}).parts||[];
      const text = parts.map(p=>p.text||"").join("").trim();
      const gmeta = cand.groundingMetadata || {};
      const chunks = gmeta.groundingChunks || [];
      const sources = chunks.map(c=> (c && c.web) ? { title:String(c.web.title||"").slice(0,80), uri:c.web.uri } : null).filter(Boolean).slice(0,8);
      if (text){ gemini429Streak=0; noteSearch(); try{ recordGeminiUsage(dj.usageMetadata); }catch(_){} return { text, sources }; }
      lastErr = "["+mdl+"] 빈 응답";
    }catch(e){ lastErr=String(e.message||e); if(isRateErr(lastErr)) rateHits++; }
  }
  // 모든 모델이 무료 쿼터(429/RESOURCE_EXHAUSTED)로 실패 → 폴백 체인(둘 다 시도)
  if (rateHits>0 && rateHits>=models.length){
    gemini429Streak++; geminiCooldownUntil=Date.now()+Math.min(10,gemini429Streak)*60000;
    // ① 무료 웹검색(DuckDuckGo+Jina)로 실제 근거 확보 → Claude가 사실 위주로 종합
    try{
      const rw = await researchWeb(prompt, want, "free");
      if(rw && rw.text){ console.log("gemini 쿼터 소진 → 무료웹+Claude 폴백(search, via="+(rw.via||"?")+")"); return { text: rw.text, sources: rw.sources||[], via: "free+"+(rw.via||"web") }; }
    }catch(we){ lastErr = (lastErr||"") + " / 무료웹 폴백 실패: "+String(we.message||we).slice(0,90); }
    // ② NVIDIA 무료 LLM(키 있으면) — 지식 기반(무료, Claude 비용 절약)
    if(nvidiaAvailable()){
      try{
        const nt = await nvidiaChat("너는 리서치 어시스턴트다. 실시간 웹은 없으니 아는 지식으로 한국어로 정확·구체적으로 답하고, 불확실하면 '추정/확인 필요'로 표기하라. 한우·특산물·SNS 콘텐츠 맥락이면 실무적으로.", prompt, want);
        if(nt){ console.log("gemini 쿼터 소진 → NVIDIA 무료 폴백(search)"); return { text: nt, sources: [], via:"nvidia-fallback" }; }
      }catch(ne){ lastErr = (lastErr||"") + " / NVIDIA 폴백 실패: "+String(ne.message||ne).slice(0,80); }
    }
    // ③ 그래도 안 되면 Claude 지식 기반 최종 폴백
    try{
      const ctext = await claudeResearchFallback(prompt, want);
      if(ctext){ console.log("gemini 쿼터 소진 → Claude 지식 폴백(search)"); return { text: ctext, sources: [], via:"claude-fallback" }; }
    }catch(fe){ lastErr = (lastErr||"") + " / Claude 폴백도 실패: "+String(fe.message||fe); }
  }
  throw new Error("gemini-search 실패: "+(lastErr||"원인 미상"));
}

// 유튜브에서 검색어로 인기 영상 URL 찾기 (YouTube Data API, Google API 키 재사용). 실패 시 [] 반환(학습은 웹검색으로 폴백).
async function ytSearchTop(query, n){
  try{
    // YouTube Data API는 Gemini 키와 별개로 활성화가 필요할 수 있음 → 전용 키(YT_API_KEY) 우선
    const key = process.env.YT_API_KEY || process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY || "";
    if(!key) return [];
    const url = "https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&maxResults="+(n||3)
      +"&order=viewCount&q="+encodeURIComponent(query)+"&key="+key;
    const r = await fetch(url);
    const d = await r.json();
    if(d.error || !d.items) return [];
    return d.items.filter(it=>it.id&&it.id.videoId).map(it=>({
      url:"https://www.youtube.com/watch?v="+it.id.videoId,
      title:(it.snippet&&it.snippet.title)||""
    }));
  }catch(e){ return []; }
}
// 유튜브 영상을 화면(프레임)+음성까지 실제로 분석 (Gemini 멀티모달, 공개 영상만, 무료 프리뷰)
async function analyzeYouTube(url, question, maxTok){
  const key = geminiKey();
  if (!key) throw new Error("GEMINI_API_KEY 미설정");
  await waitGeminiCooldown();
  const u = String(url||"").trim();
  if (!/youtube\.com\/watch\?v=|youtu\.be\//.test(u)) throw new Error("유효한 유튜브 URL이 아님");
  const models = ["gemini-2.5-flash","gemini-3.5-flash"];
  const want = maxTok || 1500;
  const q = question || "이 영상을 화면·구성·편집·자막까지 분석해, 무엇이 이 영상을 잘 만들어진(또는 인기 있는) 콘텐츠로 만드는지 구체적으로 정리하라. 도입 후킹, 장면 전환, 자막·카피, 길이·리듬, 썸네일급 첫 장면을 짚어라.";
  let lastErr=null, rateHits=0;
  for (const mdl of models){
    try{
      const apiUrl = "https://generativelanguage.googleapis.com/v1beta/models/"+mdl+":generateContent";
      const body = { contents:[{ parts:[ { fileData:{ fileUri:u } }, { text:q } ] }], generationConfig:{ maxOutputTokens: want } };
      const r = await fetchTimeout(apiUrl, { method:"POST", headers:{ "Content-Type":"application/json", "x-goog-api-key":key }, body: JSON.stringify(body) }, 180000);
      const dj = await r.json();
      if (dj.error){ lastErr="["+mdl+"] "+(dj.error.status||dj.error.code||r.status)+": "+String(dj.error.message||""); if(isRateErr(dj.error.message, dj.error.code||r.status)) rateHits++; continue; }
      const cand = (dj.candidates||[])[0]||{};
      const parts = (cand.content||{}).parts||[];
      const text = parts.map(p=>p.text||"").join("").trim();
      if (text){ gemini429Streak=0; noteSearch(); try{ recordGeminiUsage(dj.usageMetadata); }catch(_){} return { text, url:u }; }
      lastErr = "["+mdl+"] 빈 응답(영상이 비공개/미등록이거나 분석 불가)";
    }catch(e){ lastErr=String(e.message||e); if(isRateErr(lastErr)) rateHits++; }
  }
  if (rateHits>0 && rateHits>=models.length){
    gemini429Streak++; geminiCooldownUntil=Date.now()+Math.min(10,gemini429Streak)*60000;
    // ① 무료로 영상 페이지 본문(제목·설명 등) 읽기: 직접 → Jina 리더 폴백
    let pageInfo = "";
    try{ const pt = await fetchPageText(u); if(pt && pt.length>120) pageInfo = pt.slice(0,6000); }catch(_){}
    // ② 가져온 실제 텍스트 + 일반 원리를 Claude가 종합 (화면 직접 시청은 불가 — 정직하게 표기)
    try{
      const base = pageInfo
        ? ("아래는 유튜브 영상 페이지에서 무료로 가져온 실제 텍스트(제목·설명 등)다. 영상 화면 자체는 볼 수 없으니 이 텍스트와 일반 인기영상 원리로 분석하라. 맨 앞 줄에 '⚠️ 영상 화면 직접 분석 불가 — 페이지 정보+일반 원리 기반'이라고 밝혀라.\n[페이지 텍스트]\n"+pageInfo+"\n\n[질문]\n"+q)
        : ("유튜브 영상 URL이다. 화면도 못 보고 페이지 정보도 못 가져온 상태다. URL과 일반 인기 쇼츠·영상 원리로 best-effort 분석하되, 맨 앞 줄에 '⚠️ 영상 직접 분석 불가(무료 쿼터 소진) — 일반 원리 기반 추정'이라고 밝혀라.\nURL: "+u+"\n질문: "+q);
      let out=null, viaTag="";
      if(nvidiaAvailable()){   // NVIDIA 무료 우선(비용 절약)
        try{ out = await nvidiaChat("너는 인기 영상·쇼츠 분석가다. 한국어로 구체적이고 실무적으로 정리하라.", base, want); viaTag = pageInfo?"free+nvidia-fallback":"nvidia-fallback"; }
        catch(ne){ lastErr = (lastErr||"") + " / NVIDIA 폴백 실패: "+String(ne.message||ne).slice(0,80); }
      }
      if(!out){ out = await claudeResearchFallback(base, want); viaTag = pageInfo?"free+claude-fallback":"claude-fallback"; }
      if(out){ console.log("gemini 쿼터 소진 → "+viaTag+"(youtube, 영상 미시청)"); return { text: out, url:u, via:viaTag }; }
    }catch(fe){ lastErr = (lastErr||"") + " / 폴백 종합 실패: "+String(fe.message||fe); }
  }
  throw new Error("유튜브 분석 실패: "+(lastErr||"원인 미상"));
}
// 유튜브 URL 분석 엔드포인트(수동 테스트/학습용)
app.post("/api/learn/youtube", async (req,res)=>{
  try{
    const b=req.body||{};
    const out = await analyzeYouTube(b.url, b.question, b.maxTok);
    res.json({ ok:true, url:out.url, analysis:out.text });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

async function geminiText(prompt, maxTok){
  const key = geminiKey();
  if (!key) throw new Error("GEMINI_API_KEY 미설정");
  // 무료 한도 쿨다운: 작업 중이면 기다렸다 진행(끊지 않음), 평소엔 즉시 스킵(하머링 방지)
  await waitGeminiCooldown();
  // 살아있는 모델만 (gemini-2.0/1.5는 2026년 종료=404). 1순위 3.5 Flash → 2.5 Flash → 2.5 Flash-Lite
  const models = ["gemini-3.5-flash","gemini-2.5-flash","gemini-2.5-flash-lite"];
  const want = maxTok || 1200;
  let lastErr=null, rateHits=0, modelsTried=0;
  for (const mdl of models){
    modelsTried++;
    // 모델별 '확장 사고' 설정 (3.x: thinkingLevel, 2.5: thinkingBudget 동적)
    let thinkCfg=null;
    if (/gemini-3/.test(mdl)) thinkCfg = { thinkingLevel: "high" };  // 3.x: 확장 사고
    else if (/2\.5/.test(mdl)) thinkCfg = { thinkingBudget: -1 };     // 2.5: 동적(확장) 사고
    for (let attempt=0; attempt<2; attempt++){   // 0: 사고설정 포함 / 1: 필드 거부 대비 설정 빼고 재시도
      try{
        const url = "https://generativelanguage.googleapis.com/v1beta/models/"+mdl+":generateContent";
        // 확장 사고는 출력 토큰을 많이 먹어 답변이 비거나 잘릴 수 있어 여유를 크게(3.x high=+6000, 2.5=+3000)
        const headroom = thinkCfg ? (/gemini-3/.test(mdl) ? 6000 : 3000) : 0;
        const genCfg = { maxOutputTokens: want + headroom };
        if (thinkCfg && attempt===0) genCfg.thinkingConfig = thinkCfg;
        const r = await fetchTimeout(url, { method:"POST", headers:{ "Content-Type":"application/json", "x-goog-api-key":key },
          body: JSON.stringify({ contents:[{ parts:[{ text: prompt }] }], generationConfig: genCfg }) }, 150000);
        const dj = await r.json();
        if (dj.error){
          lastErr=String(dj.error.message||"");
          const code = dj.error.code || r.status;
          if (isRateErr(lastErr, code)){ rateHits++; break; }          // 이 모델 한도(429) → 다음 모델(별도 쿼터)로
          if (attempt===0 && thinkCfg && /think/i.test(lastErr)) continue; // 사고설정 거부 → 같은 모델, 설정 빼고 재시도
          break;                                                            // 그 외 오류 → 다음 모델
        }
        const parts = (((dj.candidates||[])[0]||{}).content||{}).parts||[];
        const text = parts.map(p=>p.text||"").join("").trim();
        if (text){ gemini429Streak=0; try{ recordGeminiUsage(dj.usageMetadata); }catch(_){} return text; }
        lastErr = "빈 응답"; break;
      }catch(e){
        lastErr=String(e.message||e);
        if (isRateErr(lastErr)){ rateHits++; break; }
        if (attempt===0 && thinkCfg && /think/i.test(lastErr)) continue;
        break;
      }
    }
  }
  // 모든 모델이 한도(429)에 걸렸으면 잠깐 쿨다운 (무료 등급 보호 — 자동작업은 멈추지 않고 다음 주기에 재개)
  if (rateHits>0 && rateHits>=modelsTried){
    gemini429Streak++;
    const mins = Math.min(10, gemini429Streak); // 연속 실패할수록 1→2→…→최대 10분
    geminiCooldownUntil = Date.now() + mins*60000;
    throw new Error("gemini-rate-limited(무료한도) — "+mins+"분 쉼: "+(lastErr||""));
  }
  throw new Error("Gemini 텍스트 생성 실패: "+(lastErr||"원인 미상"));
}

// 기획안 → Veo 3.1 프롬프트(10초 구간별로 분리). Gemini 협력, 실패 시 Claude 대체.
// 회의 등에서 엔진 선택: gemini면 Gemini(무료)로 → 기본은 유료(Claude) 폴백 안 함(무료 보호).
// ===== NVIDIA 무료 LLM (OpenAI 호환, build.nvidia.com nvapi- 키, 약 40 RPM) =====
// 키는 보안상 env(NVIDIA_API_KEY) 우선. 모델은 앱에서 바꾸도록 DB.state.nvidiaModel 허용(비밀 아님).
function nvidiaKey(){ return process.env.NVIDIA_API_KEY || (DB.state && DB.state.nvidiaKey) || ""; }
function nvidiaModel(){ return (DB.state && DB.state.nvidiaModel) || process.env.NVIDIA_MODEL || "meta/llama-3.1-70b-instruct"; }
function nvidiaAvailable(){ return !!nvidiaKey(); }
async function nvidiaChat(system, user, maxTok, model){
  const key = nvidiaKey();
  if(!key) throw new Error("NVIDIA_API_KEY 미설정");
  const mdl = model || nvidiaModel();
  const body = {
    model: mdl,
    messages: [ system?{ role:"system", content:String(system) }:null, { role:"user", content:String(user||"") } ].filter(Boolean),
    max_tokens: Math.min(maxTok||1400, 4000),
    temperature: 0.3
  };
  const r = await fetchTimeout("https://integrate.api.nvidia.com/v1/chat/completions",
    { method:"POST", headers:{ "Content-Type":"application/json", "Accept":"application/json", "Authorization":"Bearer "+key }, body: JSON.stringify(body) },
    (_jobMode>0?180000:90000));
  let dj; try{ dj = await r.json(); }catch(e){ throw new Error("NVIDIA 응답 파싱 실패(HTTP "+r.status+")"); }
  if(!r.ok || dj.error){ const em=(dj&&dj.error&&(dj.error.message||dj.error))||("HTTP "+r.status); throw new Error("NVIDIA["+mdl+"]: "+String(em).slice(0,180)); }
  const ch = (dj.choices||[])[0]||{};
  const txt = String((ch.message&&ch.message.content)||"").trim();
  if(!txt) throw new Error("NVIDIA 빈 응답["+mdl+"]");
  return txt;
}
async function genText(system, user, maxTok, engine){
  const prompt = String(system||"") + "\n\n" + String(user||"");
  // NVIDIA 무료 LLM 우선 엔진: 키 있으면 NVIDIA로, 실패 시 Claude 폴백(끊기지 않게)
  if (engine === "nvidia" || engine === "free") {
    if (nvidiaAvailable()){
      try { return await nvidiaChat(system, user, maxTok); }
      catch(en){ logError("genText-nvidia→claude(폴백)", en); return await anthropic(system, user, maxTok); }
    }
    return await anthropic(system, user, maxTok);   // 키 없으면 Claude
  }
  if (engine === "gemini") {
    try { return await geminiText(prompt, maxTok); }
    catch(e1){
      const allowPaid = !!(DB.state && DB.state.allowPaidFallback);
      // 쿨다운이 아니면 무료로 한 번 더 재시도
      if (Date.now() >= geminiCooldownUntil) {
        try { await new Promise(r=>setTimeout(r,1500)); return await geminiText(prompt, maxTok); }
        catch(e2){
          if (allowPaid) { logError("genText-gemini→claude(유료폴백 허용됨)", e2); return await anthropic(system, user, maxTok); }
          logError("genText-gemini(무료보호: 유료폴백 안 함, 다음 주기 재시도)", e2);
          throw e2;
        }
      }
      // 무료 한도 쿨다운 중: 기본은 스킵(0원). 명시적으로 켰을 때만 유료 폴백.
      if (allowPaid) { logError("genText-gemini→claude(쿨다운, 유료폴백 허용됨)", e1); return await anthropic(system, user, maxTok); }
      throw e1;
    }
  }
  return await anthropic(system, user, maxTok);
}

function workEngine(){
  if (DB.state && DB.state.freeLLMFirst && nvidiaAvailable()) return "free";   // 무료 우선 모드: NVIDIA 무료 → 실패 시 Claude 폴백
  return (DB.state&&DB.state.workEngine==="gemini")?"gemini":"claude";
} // 자율수행·콘텐츠·영상 엔진
// 콘텐츠/영상 엔드포인트 엔진 결정: 무료 우선 모드면 프론트가 보낸 engine보다 '무료(free)'를 우선한다.
function pickEngine(bodyEngine){
  if (DB.state && DB.state.freeLLMFirst && nvidiaAvailable()) return "free";
  return bodyEngine || workEngine();
}
async function veoPromptFromPlan(plan, charBlock){
  const consist = charBlock ? ("\n\n[등장인물 일관성 — 모든 구간에서 아래 인물의 외모·의상을 동일하게 유지하고, 영어 프롬프트에 그 특징을 매 구간 반복해 넣어라]"+charBlock) : "";
  const fmt = " 영상을 10초 단위 구간으로 끊어, 각 구간마다 아래 형식으로 출력하라.\n"
    + "◆ 구간 N (0-10초)\n<그 10초 구간의 영어 Veo 프롬프트 — 카메라 무빙·조명·분위기·피사체·동작·색감을 영화적으로>\n자막/나레이션: <해당 구간 자막·나레이션(한국어 가능)>\n"
    + "구간 사이에는 빈 줄 하나. 마지막 구간은 10초 미만이어도 된다. 설명·머리말 없이 구간들만 출력.";
  const gp = "다음 한국어 영상 기획안을 Google Veo 3.1로 생성 가능한 영어 영상 프롬프트로 변환하라."+fmt+consist+"\n\n[기획안]\n"+plan;
  try{ return { veoPrompt: await geminiText(gp, 1100), by:"Gemini" }; }
  catch(e){
    const cp = "너는 영상 생성 프롬프트 전문가다. 아래 기획안을 Veo 3.1용으로 변환하라."+fmt+consist;
    return { veoPrompt: await anthropic(cp, "[기획안]\n"+plan, 1000), by:"Claude(Gemini 미설정/실패 대체)" };
  }
}

async function ttsGemini(text, voiceName){
  const key = geminiKey();
  if (!key) throw new Error("GEMINI_API_KEY 미설정 — 설정에서 등록하세요");
  const voice = voiceName || "Charon";
  const clean = String(text||"").replace(/[#*>_`~|]/g," ").replace(/\s+/g," ").trim().slice(0,1200);
  if (!clean) throw new Error("빈 텍스트");
  const ck = voice+"|"+clean;
  if (_ttsCache.has(ck)) return _ttsCache.get(ck);
  // 성공했던 모델 우선, 실패 시 후보 순회
  const tryList = [TTS_MODEL].concat(TTS_MODELS.filter(m=>m!==TTS_MODEL));
  let d=null, lastErr=null, usedModel=null;
  for (const mdl of tryList){
    try {
      const url = "https://generativelanguage.googleapis.com/v1beta/models/"+mdl+":generateContent";
      const r = await fetch(url, {
        method:"POST",
        headers:{ "Content-Type":"application/json", "x-goog-api-key":key },
        body: JSON.stringify({
          contents:[{ parts:[{ text: clean }] }],
          generationConfig:{ responseModalities:["AUDIO"], speechConfig:{ voiceConfig:{ prebuiltVoiceConfig:{ voiceName:voice } } } }
        })
      });
      const dj = await r.json();
      if (dj.error){
        lastErr = mdl+": "+String(dj.error.message||JSON.stringify(dj.error)).slice(0,160);
        // 모델 없음/권한 오류면 다음 후보, 그 외(키·쿼터 등)는 즉시 중단
        if (/not found|not supported|permission|invalid model|404/i.test(String(dj.error.message||""))) continue;
        throw new Error("Gemini TTS: "+lastErr);
      }
      d = dj; usedModel = mdl; break;
    } catch(e){ lastErr = mdl+": "+String(e.message||e).slice(0,160); if(!/not found|not supported|404/i.test(lastErr)) { logError("tts", e); throw e; } }
  }
  if (!d) { const err=new Error("Gemini TTS 모델 호출 실패 — "+(lastErr||"원인 미상")); logError("tts", err); throw err; }
  if (usedModel && usedModel !== TTS_MODEL) TTS_MODEL = usedModel; // 성공 모델 기억
  const part = (((d.candidates||[])[0]||{}).content||{}).parts||[];
  const inline = part.map(p=>p.inlineData||p.inline_data).filter(Boolean)[0];
  if (!inline || !inline.data){ const err=new Error("Gemini TTS: 오디오 응답 없음 (모델 "+usedModel+")"); logError("tts", err); throw err; }
  // 출력 mime 예: audio/L16;rate=24000 → rate 파싱
  const mt = inline.mimeType || inline.mime_type || "audio/L16;rate=24000";
  const mr = /rate=(\d+)/.exec(mt); const rate = mr ? +mr[1] : 24000;
  const wav = pcmToWavBase64(inline.data, rate, 1, 16);
  if (_ttsCache.size > 200){ const k0=_ttsCache.keys().next().value; _ttsCache.delete(k0); }
  _ttsCache.set(ck, wav);
  return wav;
}

async function generateImage(prompt){
  // OpenAI 이미지 생성 (OPENAI_API_KEY) → base64 → 저장소 업로드 → 공개 URL
  if (!process.env.OPENAI_API_KEY) throw new Error("OPENAI_API_KEY 미설정");
  const r = await fetch("https://api.openai.com/v1/images/generations", {
    method:"POST",
    headers:{ "Content-Type":"application/json", "Authorization":"Bearer "+process.env.OPENAI_API_KEY },
    body: JSON.stringify({ model: process.env.IMAGE_MODEL || "gpt-image-1", prompt, size:"1024x1024", n:1 })
  });
  const d = await r.json();
  if (d.error) throw new Error(d.error.message || "이미지 생성 실패");
  const b64 = d.data && d.data[0] && d.data[0].b64_json;
  if (!b64) throw new Error("이미지 데이터 없음");
  return await uploadMedia(b64, "image/png");
}


// content 예시: { title, description, caption, tags:[], mediaUrl, mediaType:"video"|"image", privacy }
async function publishYouTube(content){
  if (!content || !content.mediaUrl) return { ok:false, note:"영상 mediaUrl 필요" };
  const token = await getGoogleToken();
  // 1) 영상 내려받기
  const vres = await fetch(content.mediaUrl);
  if (!vres.ok) return { ok:false, note:"영상 다운로드 실패" };
  const videoBuf = Buffer.from(await vres.arrayBuffer());
  // 2) resumable 업로드 세션 시작 (메타데이터)
  const meta = {
    snippet: { title: content.title || "무제", description: content.description || "", tags: content.tags || [] },
    status: { privacyStatus: content.privacy || "private" }
  };
  const init = await fetch("https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status", {
    method:"POST",
    headers:{ "Authorization":"Bearer "+token, "Content-Type":"application/json; charset=UTF-8", "X-Upload-Content-Type":"video/*" },
    body: JSON.stringify(meta)
  });
  const uploadUrl = init.headers.get("location");
  if (!uploadUrl) return { ok:false, note:"업로드 세션 실패: "+(await init.text()).slice(0,160) };
  // 3) 영상 업로드
  const up = await fetch(uploadUrl, { method:"PUT", headers:{ "Content-Type":"video/*" }, body: videoBuf });
  if (!up.ok) return { ok:false, note:"업로드 실패("+up.status+"): "+(await up.text().catch(()=>"")).slice(0,160) };
  let d; try { d = await up.json(); } catch(e){ return { ok:false, note:"업로드 응답 파싱 실패: "+(await up.text().catch(()=>"")).slice(0,160) }; }
  return d && d.id ? { ok:true, url:"https://youtu.be/"+d.id } : { ok:false, note:JSON.stringify(d).slice(0,200) };
}
async function publishInstagram(content){
  const token = process.env.IG_ACCESS_TOKEN, user = process.env.IG_USER_ID;
  if (!token || !user) return { ok:false, note:"IG_ACCESS_TOKEN/IG_USER_ID 필요" };
  if (!content || !content.mediaUrl) return { ok:false, note:"공개 mediaUrl 필요(인스타는 공개 URL 필수)" };
  const GV = "https://graph.facebook.com/v21.0/";
  const isVideo = content.mediaType==="video" || /\.(mp4|mov)(\?|$)/i.test(content.mediaUrl);
  // 1) 미디어 컨테이너 생성
  const p1 = new URLSearchParams({ caption: content.caption || content.description || "", access_token: token });
  if (isVideo) { p1.set("media_type","REELS"); p1.set("video_url", content.mediaUrl); }
  else p1.set("image_url", content.mediaUrl);
  const c = await fetch(GV+user+"/media", { method:"POST", body:p1 });
  const cd = await c.json();
  if (!cd.id) return { ok:false, note:JSON.stringify(cd).slice(0,200) };
  // 영상(릴스)은 인코딩이 끝나야 게시 가능 — status_code=FINISHED 될 때까지 폴링
  if (isVideo) {
    const deadline = Date.now() + 5*60*1000; // 최대 5분 대기
    while (Date.now() < deadline) {
      await new Promise(r=>setTimeout(r, 5000));
      let st;
      try {
        const sres = await fetch(GV+cd.id+"?fields=status_code,status&access_token="+encodeURIComponent(token));
        st = await sres.json();
      } catch(e){ continue; }
      const code = st && st.status_code;
      if (code === "FINISHED") break;
      if (code === "ERROR" || code === "EXPIRED") return { ok:false, note:"인스타 영상 인코딩 실패: "+(st.status||code) };
      // IN_PROGRESS 또는 PUBLISHED가 아니면 계속 대기
    }
  }
  // 2) 게시
  const p2 = new URLSearchParams({ creation_id: cd.id, access_token: token });
  const pub = await fetch(GV+user+"/media_publish", { method:"POST", body:p2 });
  const pd = await pub.json();
  return pd.id ? { ok:true, id:pd.id } : { ok:false, note:JSON.stringify(pd).slice(0,200) };
}
async function publishWordpress(content){
  const base = process.env.WP_BASE, user = process.env.WP_USER, pw = process.env.WP_APP_PASSWORD;
  if (!base || !user || !pw) return { ok:false, note:"WP_BASE/WP_USER/WP_APP_PASSWORD 필요" };
  const r = await fetch(base.replace(/\/$/,"")+"/wp-json/wp/v2/posts", {
    method:"POST",
    headers:{ "Content-Type":"application/json", "Authorization":"Basic "+Buffer.from(user+":"+pw).toString("base64") },
    body: JSON.stringify({ title: content.title || "무제", content: content.description || content.body || "", status:"publish" })
  });
  const d = await r.json();
  if (d.id) { try { archiveBlogPost({ title: content.title||"무제", url: d.link, topic: content.topic||content.title||"", tags: content.tags||[] }); } catch(e){} return { ok:true, url:d.link }; }
  return { ok:false, note:JSON.stringify(d).slice(0,200) };
}
async function publishWebhook(content){
  const url = process.env.SITE_WEBHOOK_URL;
  if (!url) return { ok:false, note:"SITE_WEBHOOK_URL 필요" };
  const r = await fetch(url, { method:"POST", headers:{ "Content-Type":"application/json" }, body: JSON.stringify(content) });
  return { ok:r.ok, note: r.ok ? "홈페이지 웹훅 전송" : "웹훅 실패("+r.status+")" };
}
// ===== 페이스북 페이지 발행 (Graph API /{page-id}/feed) =====
// 필요: FB_PAGE_ID, FB_PAGE_TOKEN(만료 없는 페이지 토큰). 영상은 /videos, 사진은 /photos, 텍스트/링크는 /feed.
async function publishFacebook(content){
  const pageId = process.env.FB_PAGE_ID, token = process.env.FB_PAGE_TOKEN;
  if (!pageId || !token) return { ok:false, note:"FB_PAGE_ID/FB_PAGE_TOKEN 필요" };
  const GV = "https://graph.facebook.com/v21.0/";
  const msg = content.caption || content.description || content.body || content.title || "";
  const media = content.mediaUrl;
  const isVideo = content.mediaType==="video" || (media && /\.(mp4|mov)(\?|$)/i.test(media));
  try {
    let endpoint, params;
    if (media && isVideo) {
      endpoint = GV+pageId+"/videos";
      params = new URLSearchParams({ file_url: media, description: msg, access_token: token });
    } else if (media) {
      endpoint = GV+pageId+"/photos";
      params = new URLSearchParams({ url: media, caption: msg, access_token: token });
    } else {
      if (!msg) return { ok:false, note:"페이스북: 내용(text) 또는 mediaUrl 필요" };
      endpoint = GV+pageId+"/feed";
      params = new URLSearchParams({ message: msg, access_token: token });
    }
    const r = await fetch(endpoint, { method:"POST", body: params });
    if (!r.ok) return { ok:false, note:"페이스북 실패("+r.status+"): "+(await r.text().catch(()=>"")).slice(0,160) };
    let d; try { d = await r.json(); } catch(e){ return { ok:false, note:"페이스북 응답 파싱 실패" }; }
    const id = d.id || d.post_id;
    return id ? { ok:true, id, url:"https://facebook.com/"+id } : { ok:false, note:JSON.stringify(d).slice(0,200) };
  } catch(e){ return { ok:false, note:"페이스북 오류: "+String(e.message||e) }; }
}
// ===== 복사용 발행함으로 라우팅하는 발행기 (API 막힌 채널) =====
// 자동발행 대신 발행함(Publish Inbox)에 완성 글을 담는다 → 사용자가 폰에서 복사·붙여넣기.
function inboxPublisher(channel){
  return async function(content){
    try {
      const it = addToInbox({
        channel,
        title: content.title || content.topic || "",
        body: content.body || content.description || content.caption || "",
        tags: content.tags || content.hashtags || [],
        meta: content.meta || content.metaDescription || "",
        topic: content.topic || content.title || ""
      });
      const nm = (COPY_CHANNELS[channel] && COPY_CHANNELS[channel].name) || channel;
      return { ok:true, note:"발행함에 담김("+nm+") — 앱에서 복사·붙여넣기", inbox:true, inboxId: it.id };
    } catch(e){ return { ok:false, note:"발행함 담기 실패: "+String(e.message||e) }; }
  };
}
const publishers = {
  "유튜브": publishYouTube,
  "인스타그램": publishInstagram,
  "페이스북": publishFacebook,
  "블로그": publishWordpress,
  "홈페이지": publishWebhook,
  "티스토리": inboxPublisher("tistory"),
  "네이버블로그": inboxPublisher("naver_blog"),
  "네이버카페": inboxPublisher("naver_cafe"),
  "다음카페": inboxPublisher("daum_cafe")
};
// ===== 발행 전 감사(08) + 한도 =====
function randCode(){ return Math.random().toString(36).slice(2,6).toUpperCase(); }
function publicBase(){ return process.env.PUBLIC_BASE || process.env.RENDER_EXTERNAL_URL || process.env.SELF_URL || ""; }
async function kakaoApprovalRequest(c, platforms, id, code){
  let token; try { token = await getKakaoToken(); } catch(e){ return false; }
  const base = publicBase();
  const text = "📢 발행 승인 요청\n플랫폼: "+platforms.join(", ")+"\n내용: "+((c.description||c.caption||"").slice(0,120));
  const tpl = {
    object_type:"text", text, link:{ web_url:"", mobile_web_url:"" },
    buttons:[
      { title:"✅ 승인", link:{ web_url: base+"/api/approve?id="+id+"&code="+code, mobile_web_url: base+"/api/approve?id="+id+"&code="+code } },
      { title:"❌ 거절", link:{ web_url: base+"/api/reject?id="+id+"&code="+code, mobile_web_url: base+"/api/reject?id="+id+"&code="+code } }
    ]
  };
  const r = await fetch("https://kapi.kakao.com/v2/api/talk/memo/default/send", {
    method:"POST", headers:{ "Authorization":"Bearer "+token, "Content-Type":"application/x-www-form-urlencoded" },
    body: "template_object=" + encodeURIComponent(JSON.stringify(tpl))
  });
  return r.ok;
}
// 프로젝트 중대결정: 카톡으로 승인/보류 버튼 전송 (승인 시 즉시 진행)
async function kakaoProjectApproval(project){
  let token; try { token = await getKakaoToken(); } catch(e){ return false; }
  const base = publicBase();
  const code = randCode();
  project.approveCode = code; saveDB();
  const text = "🙋 프로젝트 확인 요청\n["+String(project.title||"").slice(0,40)+"]\n"+String(project.holdReason||"").slice(0,300)+"\n\n승인하면 이 방향으로 계속 진행해요.";
  const tpl = {
    object_type:"text", text, link:{ web_url:"", mobile_web_url:"" },
    buttons:[
      { title:"✅ 승인·계속 진행", link:{ web_url: base+"/api/projects/approve?id="+project.id+"&code="+code, mobile_web_url: base+"/api/projects/approve?id="+project.id+"&code="+code } },
      { title:"⏸ 보류(앱에서 지시)", link:{ web_url: base+"/api/projects/hold?id="+project.id+"&code="+code, mobile_web_url: base+"/api/projects/hold?id="+project.id+"&code="+code } }
    ]
  };
  const r = await fetch("https://kapi.kakao.com/v2/api/talk/memo/default/send", {
    method:"POST", headers:{ "Authorization":"Bearer "+token, "Content-Type":"application/x-www-form-urlencoded" },
    body: "template_object=" + encodeURIComponent(JSON.stringify(tpl))
  });
  return r.ok;
}
async function doActualPublish(c, platforms){
  const results = [];
  for (const p of (platforms||[])) {
    const fn = publishers[p];
    let r; try { r = fn ? await fn(c) : { ok:false, note:"어댑터 없음" }; } catch(e){ logError("publish:"+p, e); r = { ok:false, note:String(e.message||e) }; }
    results.push({ platform:p, ...r });
    // 실패 시 재시도 큐에 등록 (어댑터 없음/미디어 누락 등 비일시적 오류는 제외)
    if (!r.ok && !/어댑터 없음|필요|미설정/.test(r.note||"")) {
      DB.retryQueue = DB.retryQueue || [];
      DB.retryQueue.push({ id:Date.now()+Math.random(), content:c, platform:p, tries:0, nextAt:Date.now()+60000 });
    }
  }
  const okCount = results.filter(r=>r.ok).length;
  // 발행 성공 결과의 URL/ID를 저장 → 나중에 분석부(04)가 성과 추적
  const published = results.filter(r=>r.ok && (r.url||r.id)).map(r=>({
    platform: r.platform, url: r.url||"", id: r.id||"",
    title: (c.title||c.caption||c.description||"").slice(0,80), at: Date.now()
  }));
  DB.jobs.push({ id:Date.now(), type:"publish", platforms, ok:okCount>0, count:okCount, published, at:Date.now() }); saveDB();
  return results;
}
async function auditContent(c){
  const text = (c.description || c.caption || "").slice(0, 2000);
  if (!text) return { blocked:false, reason:"내용 없음" };
  const sys = "너는 08 감사·법무 에이전트다. 아래 콘텐츠가 저작권 침해·허위/과장 광고(광고법)·플랫폼 정책 위반·명예훼손·개인정보 노출 소지가 있는지 점검하라. 첫 줄에 정확히 PASS 또는 BLOCK 한 단어만, 둘째 줄에 한 줄 이유. 명백하고 심각한 위반만 BLOCK 한다.";
  let out; try { out = await anthropic(sys, text, 200); } catch(e){ return { blocked:false, reason:"감사 생략(오류)" }; }
  return { blocked: /^\s*BLOCK/i.test(out), reason: out };
}
function todayStr(){ return new Date().toISOString().slice(0,10); }
function todaysPublishCount(){
  const t = todayStr();
  return (DB.jobs||[]).filter(j=>j.type==="publish" && j.ok && new Date(j.at).toISOString().slice(0,10)===t)
    .reduce((a,j)=>a+(j.count||1), 0);
}
async function publish(content, platforms){
  // content가 문자열이면 description으로 감싼다
  const c = (typeof content === "string") ? { description: content, caption: content } : (content || {});
  const safety = (DB.state && DB.state.safety) || {};
  // 1) 일 발행 한도 게이트
  if (safety.dailyLimit > 0 && todaysPublishCount() >= safety.dailyLimit) {
    kakaoNotify("⛔ 발행 한도 도달("+safety.dailyLimit+"/일) — 자동 정지").catch(()=>{});
    return [{ platform:"(전체)", ok:false, note:"일 발행 한도 초과 — 자동 정지" }];
  }
  // 2) 발행 전 08 감사·법무 점검
  if (safety.audit) {
    const a = await auditContent(c);
    if (a.blocked) {
      kakaoNotify("⛔ 발행 차단(08 감사) — "+a.reason.slice(0,120)).catch(()=>{});
      DB.jobs.push({ id:Date.now(), type:"publish", platforms, ok:false, blocked:true, at:Date.now() }); saveDB();
      return [{ platform:"(08 감사)", ok:false, note:"감사 차단 — "+a.reason.slice(0,150) }];
    }
  }
  // 3) 사람 승인 대기 (카톡 [승인]/[거절])
  if (safety.requireApproval) {
    const id = Date.now(), code = randCode();
    DB.approvals.push({ id, code, content:c, platforms, status:"pending", at:id }); saveDB();
    const sent = await kakaoApprovalRequest(c, platforms, id, code);
    return [{ platform:"(승인 대기)", ok:false, pending:true, note: sent ? "카카오톡으로 승인 요청을 보냈습니다. 승인 시 발행됩니다." : ("승인 대기 — 앱의 ‘실행 결과’ 탭에서 승인하세요 (코드 "+code+")") }];
  }
  // 4) 발행 실행
  return await doActualPublish(c, platforms);
}

// ===== 카카오 "나에게 보내기" (상행 알림) =====
async function kakaoNotify(text, url){
  sendPush(String(text)).catch(()=>{});   // 웹푸시는 카톡 토큰과 무관하게 발송
  let token; try { token = await getKakaoToken(); } catch(e){ return { ok:false, note:String(e.message||e) }; }
  const u = String(url||"");
  const tpl = { object_type:"text", text:String(text).slice(0,1000), link: u ? { web_url:u, mobile_web_url:u } : { web_url:"", mobile_web_url:"" } };
  const r = await fetch("https://kapi.kakao.com/v2/api/talk/memo/default/send", {
    method:"POST",
    headers:{ "Authorization":"Bearer "+token, "Content-Type":"application/x-www-form-urlencoded" },
    body: "template_object=" + encodeURIComponent(JSON.stringify(tpl))
  });
  return { ok:r.ok, note: r.ok ? "카톡 알림 전송" : "카톡 전송 실패("+r.status+")" };
}

// ===== 팀장 일일 브리핑 — 오세라가 오늘 각 부서·팀이 한 일을 정리 =====
// ===== 팀장 브리핑 — 아침(오늘 할 일)·저녁(오늘 한 일)·주간(한 주 종합) =====
function deptActivityRows(sinceUtc){
  return Object.keys(AGENTS).filter(d=>d!=="ops").map(d=>{
    const items = (DB.deptMemory[d]||[]).filter(x=>(x.at||0) >= sinceUtc);
    const lv = deptLevel(d);
    const list = items.slice(-5).map(x=>"   · "+(x.instruction||"")+" → "+String(x.note||"").slice(0,90)).join("\n");
    return AGENTS[d].no+" "+AGENTS[d].kr+"("+MEMBERS[d]+") — Lv"+lv+" · "+items.length+"건"+(list?"\n"+list:"  (기록 없음)");
  }).join("\n\n");
}
async function runBriefing(kind, engine, force){
  kind = (kind==="morning"||kind==="weekly") ? kind : "evening";
  const dayKey = kstDay();
  const a = AGENTS["ops"];
  let sys = "너는 SNS 자동화 회사의 팀장 '"+a.no+" "+a.kr+"'다. 역할: "+a.role+ADDRESS+STYLE + clientBlock();
  let ctx="", title="", periodKey="";

  if (kind === "morning") {
    periodKey = "m:"+dayKey;
    if (!force && (DB.briefDone&&DB.briefDone.morning)===dayKey) return { ok:true, skipped:true };
    const lastEve = (DB.briefings||[]).filter(b=>b.kind==="evening").slice(-1)[0];
    const dirLines = Object.keys(AGENTS).filter(d=>d!=="ops").map(d=>{ const dir=(DB.state&&DB.state.deptDirective&&DB.state.deptDirective[d])||""; return dir?("   · "+AGENTS[d].kr+": "+dir):null; }).filter(Boolean).join("\n");
    sys += " 지금은 하루를 시작하며 클라이언트님께 '오늘 할 일'을 제안하는 아침 브리핑 시간이다. 어제 마무리와 현재 방향을 보고 오늘 팀이 집중할 일을 제안하라. 형식(이 형식만):\n오늘의 포커스: (오늘 가장 중요한 방향 1문장)\n부서별 할 일: (부서마다 한 줄씩 '부서명 — 오늘 할 구체 작업')\n우선순위: (가장 먼저 처리할 1~2개)\n한마디: (팀에게 건네는 짧은 응원)\n군더더기·서론 없이, 든든하고 따뜻하게, 한국어로만.";
    ctx = "[어제 마무리 브리핑]\n"+(lastEve?lastEve.text:"(없음)")+"\n\n[현재 부서 자율지시]\n"+(dirLines||"(지정 없음)")+"\n\n[부서 레벨·최근 활동]\n"+deptActivityRows(Date.now()-3*86400000);
    title = "☀️ 오세라 아침 브리핑 (오늘 할 일)";
  } else if (kind === "weekly") {
    const weekUtc = Date.now() - 7*86400000;
    periodKey = "w:"+dayKey;
    if (!force && (DB.briefDone&&DB.briefDone.weekly)===weekKey()) return { ok:true, skipped:true };
    const meets = (DB.meetings||[]).filter(m=>(m.at||0) >= weekUtc);
    const meetLine = meets.length ? meets.slice(-10).map(m=>"   · ["+(m.topic||"")+"] "+String(m.summary||"진행").slice(0,70)).join("\n") : "   (이번 주 회의 없음)";
    sys += " 지금은 한 주를 마무리하는 주간 종합 브리핑 시간이다. 지난 7일간 각 부서의 성장·성과와 회의를 종합해 팀장으로서 정리하라. 형식(이 형식만):\n이번 주 한 줄: (한 주 전체 요약 1문장)\n부서별 성과: (부서마다 한 줄씩 '부서명 — 한 주 성과·성장')\n팀 성장 진단: (가장 성장한 부서·정체된 부서·전체 방향 2~3문장)\n다음 주 포커스: (다음 주 먼저 할 일 2~3개)\n든든하고 따뜻하게, 한국어로만.";
    ctx = "[지난 7일 부서 활동]\n"+deptActivityRows(weekUtc)+"\n\n[이번 주 회의]\n"+meetLine;
    title = "📅 오세라 주간 종합 브리핑";
  } else {
    periodKey = "e:"+dayKey;
    if (!force && (DB.briefDone&&DB.briefDone.evening)===dayKey) return { ok:true, skipped:true };
    const kstMidnightUtc = Date.parse(dayKey+"T00:00:00Z") - 9*3600000;
    const meets = (DB.meetings||[]).filter(m=>(m.at||0) >= kstMidnightUtc);
    const meetLine = meets.length ? meets.map(m=>"   · ["+(m.topic||"")+"] "+String(m.summary||"진행").slice(0,80)).join("\n") : "   (오늘 회의 없음)";
    sys += " 지금은 하루를 마무리하며 클라이언트님께 오늘의 팀 브리핑을 드리는 시간이다. 오늘 각 부서의 활동과 회의를 보고 팀장으로서 따뜻하고 명료하게 브리핑하라. 형식(이 형식만):\n오늘의 한 줄: (팀 전체 하루 요약 1문장)\n부서별: (활동이 있던 부서만 한 줄씩 '부서명 — 무엇을 했고 어떤 성과/배움')\n팀 하이라이트: (가장 잘된 점 1~2개)\n내일 제안: (내일 먼저 하면 좋을 일 1~2개)\n군더더기·서론 없이, 든든하고 따뜻하게, 한국어로만.";
    ctx = "[오늘 부서 활동]\n"+deptActivityRows(kstMidnightUtc)+"\n\n[오늘 회의]\n"+meetLine;
    title = "🌙 오세라 저녁 브리핑 (오늘 한 일)";
  }

  const out = await genText(sys, ctx, 1300, engine||"gemini"); // 자동 = 무료(Gemini)
  const brief = { date:dayKey, kind, text:out, at:Date.now() };
  DB.briefings = DB.briefings||[]; DB.briefings.push(brief);
  if (DB.briefings.length > 40) DB.briefings = DB.briefings.slice(-40);
  DB.briefDone = DB.briefDone || {};
  if (kind==="morning") DB.briefDone.morning = dayKey;
  else if (kind==="weekly") DB.briefDone.weekly = weekKey();
  else { DB.briefDone.evening = dayKey; DB.lastBriefDay = dayKey; }
  saveDB();
  kakaoNotify(title+"\n\n"+String(out).slice(0,900)).catch(()=>{});
  return { ok:true, briefing:brief };
}
function weekKey(){ const n=kstNow(); const onejan=new Date(Date.UTC(n.getUTCFullYear(),0,1)); const wk=Math.ceil((((n-onejan)/86400000)+onejan.getUTCDay()+1)/7); return n.getUTCFullYear()+"-W"+wk; }
async function runDailyBriefing(engine, force){ return runBriefing("evening", engine, force); } // 하위호환
app.post("/api/ops/briefing", async (req,res)=>{
  try{ const b=req.body||{}; const r=await runBriefing(b.kind||"evening", b.engine, b.force!==false ? true : false); res.json(r); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.get("/api/briefings", (req,res)=> res.json(DB.briefings||[]));

// ===== 통합 지시 처리 (서버 오케스트레이션) =====
// 협의 모드 1단계: 각 부서가 자기 관점의 의견·아이디어만 짧게 제시 (실행 X)

// ===== 회의 시스템 =====
function kstNow(){ return new Date(Date.now() + 9*3600000); }
function kstHHMM(){ return kstNow().toISOString().slice(11,16); }
function kstDay(){ return kstNow().toISOString().slice(0,10); }
function kstDow(){ return kstNow().getUTCDay(); } // KST 보정된 시각의 요일(0=일~6=토)
let runningMeetings = 0;


// 최근 회의 만족도 피드백 — 다음 회의·지시 수행에 반영할 학습 신호
function meetingFeedbackInsights(){
  const rated = (DB.meetings||[]).filter(m=>m.rating).slice(-6);
  if (!rated.length) return "";
  const lines = rated.map(m=>"· "+(m.topic||"")+" (만족도 ★"+m.rating+"/5)"+(m.feedback?" — 피드백: "+m.feedback:"")).join("\n");
  const low = rated.filter(m=>m.rating<=2).length;
  return "\n\n[지난 회의 만족도 피드백 — 클라이언트님의 평가다. 높게 평가된 점은 유지하고, 낮은 평가·피드백은 이번에 반드시 개선해 반영하라"+(low?" (특히 별 2개 이하 회의의 문제를 되풀이하지 마라)":"")+"]\n"+lines;
}
// 회의 1회 진행: 셸 생성 → 발언을 실시간으로 기록 → 의장 결론 → 참여 부서 학습
function createMeeting(opts){
  opts = opts||{};
  let depts = (opts.depts||[]).filter(d=>AGENTS[d]);
  // 상시 참여 에이전트(팀장 등) 항상 합류
  const always = (opts.alwaysJoin||[]).filter(d=>AGENTS[d]);
  always.forEach(d=>{ if(!depts.includes(d)) depts.push(d); });
  // 중복 제거
  depts = depts.filter((d,i)=>depts.indexOf(d)===i);
  if (depts.length < 2) throw new Error("회의에는 최소 2개 부서가 필요합니다");
  if (runningMeetings >= 5) throw new Error("동시에 진행 가능한 회의는 최대 5개입니다");
  const rounds = Math.max(1, Math.min(3, +opts.rounds || 2));
  const mode = opts.mode || "discuss";
  // 의장(진행): 지정 > 상시참여 팀장(ops) > 상시참여 첫번째 > strategy > 첫 부서
  const chair = (opts.chair && AGENTS[opts.chair] && depts.includes(opts.chair)) ? opts.chair
    : (always.includes("ops") ? "ops" : (always[0] || (depts.includes("strategy") ? "strategy" : depts[0])));
  let agenda = Array.isArray(opts.agenda) ? opts.agenda.filter(x=>String(x||"").trim()).map(x=>String(x).trim()) : [];
  const topic = String(opts.topic||agenda[0]||"회의").trim();
  if (!agenda.length) agenda = [topic];
  if (agenda.length > 4) agenda = agenda.slice(0,4);
  const meeting = { id:Date.now()+Math.floor(Math.random()*1000), type:"meeting", status:"running", phase:"준비", topic, room:opts.room||"", depts, rounds, mode, chair, agenda, agendaSummaries:[], transcript:[], summary:"", clientNote:opts.clientNote||"", prevSummary:opts.prevSummary||"", state:{ topic, market_insight:"", draft_content:"", critique_feedback:"", revision_count:0, final_output:"" }, at:Date.now(), source:opts.source||"app", engine:(((opts.engine || (DB.state&&DB.state.meetingEngine))==="gemini") ? "gemini" : "claude") };
  DB.meetings.push(meeting);
  if (DB.meetings.length > 60) DB.meetings = DB.meetings.slice(-60);
  saveDB();
  return meeting;
}

async function processMeeting(meeting){
  const MODE = {
    discuss:{ label:"협의", g:"서로의 의견을 검토·보완해 합의로 수렴하라." },
    brainstorm:{ label:"브레인스토밍", g:"비판은 미루고 최대한 많고 과감한 아이디어를 발산하라. 남의 아이디어에 \'덧붙이기(yes-and)\'로 확장하라." },
    decision:{ label:"의사결정", g:"선택지를 명확히 비교하고 근거를 들어 하나의 결론으로 빠르게 수렴하라. 모호한 말 금지." },
    redteam:{ label:"레드팀", g:"제시된 안의 허점·리스크·실패 시나리오를 적극적으로 들춰 비판하라. 그다음 그 약점을 메울 보완책을 제시하라." },
    retro:{ label:"회고", g:"지난 작업/결과를 돌아보며 잘된 점·문제점·다음에 바꿀 개선 액션을 솔직하게 짚어라." }
  };
  const mg = (MODE[meeting.mode]||MODE.discuss);
  const depts = meeting.depts, rounds = meeting.rounds, chair = meeting.chair, agenda = meeting.agenda, topic = meeting.topic;
  const transcript = meeting.transcript;
  const eng = (meeting.engine==="gemini") ? "gemini" : "claude"; // 회의 AI 엔진
  runningMeetings++;
  try {
    const spk = (dp)=> AGENTS[dp] ? (AGENTS[dp].kr+"("+MEMBERS[dp]+")") : "클라이언트(운영자)";
    const tline = (ai)=> transcript.filter(t=>t.agenda===ai).slice(-14).map(t => spk(t.dept)+": "+t.text).join("\n");
    const _cn = (meeting.clientNote && String(meeting.clientNote).trim()) ? String(meeting.clientNote).trim() : "";
    const steer = _cn ? ("\n\n[운영자(클라이언트)가 이 회의에서 직접 발언했다 — 최우선으로 존중하고, 네 발언에서 이 내용을 직접 가리키며 반영·응답하라]\n\""+_cn+"\"") : "";
    const prevctx = (meeting.prevSummary && String(meeting.prevSummary).trim()) ? ("\n\n[이전 회의 결론 — 처음부터 다시 하지 말고 여기서 이어 발전시켜라]\n"+String(meeting.prevSummary).trim()) : "";

    if (_cn){ transcript.push({ dept:"client", member:"클라이언트", round:0, agenda:0, text:_cn }); saveDB(); }
    // 회의 전 트렌드 조사: 팀장(오세라)이 주제의 최신 트렌드를 실시간 웹검색해 회의에 주입(무료 한도 내, 실패 시 생략)
    let trendCtx = "";
    try{
      const st = DB.state||{};
      if (st.nightSearchOn !== false && searchAllowedNow()){
        meeting.phase = "트렌드 조사"; saveDB();
        const sp = "너는 팀장 '"+AGENTS.ops.kr+"("+MEMBERS["ops"]+")'다. 곧 열릴 회의 주제 '"+topic+"'에 대해 전 세계 공개 웹에서 지금 유효한 최신 트렌드·키워드·사례를 조사하라. 딥웹·비공개 제외. 한국 SNS 마케팅에 바로 쓸 수 있게 핵심만 200자 내외로 정리(트렌드 키워드·타겟 반응·참고 앵글).";
        const r = await geminiSearch(sp, 1200);
        if (r && r.text){
          trendCtx = "\n\n[회의 전 팀장이 조사한 최신 트렌드 — 발언에 적극 반영하라]\n"+String(r.text).slice(0,600);
          meeting.trendResearch = { text:String(r.text).slice(0,600), sources:(r.sources||[]).slice(0,5), at:Date.now() };
          transcript.push({ dept:"ops", member:MEMBERS["ops"]||"", round:0, agenda:0, text:"[회의 전 트렌드 조사] "+String(r.text).slice(0,400) });
          saveDB();
        }
      }
    }catch(e){ logError("meeting-trend", e); /* 검색 실패해도 회의는 진행 */ }
    meeting.phase = "분석·토론"; saveDB();
    for (let ai=0; ai<agenda.length; ai++){
      const item = agenda[ai];
      for (let r=1; r<=rounds; r++){
        for (const d of depts){
          const a = AGENTS[d];
          const kbm = knowledgeText(d);
          const mem = (DB.deptMemory[d]||[]).slice(-4).map(x=>"· "+x.note).join("\n");
          let sys = "너는 SNS 자동화 회사 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role+ADDRESS+STYLE
            + " 너의 담당 매니저는 '"+(MEMBERS[d]||"")+"'이며 성격은 ["+(PERSONA[d]||"")+"] 이 성격·말투를 자연스럽게 반영하라."
            + " 지금은 클라이언트님이 소집한 ["+mg.label+"] 회의 중이다. "+mg.g+(d===chair?" 너는 이 회의의 의장이다. 흐름을 이끌되 결론은 마지막에 종합한다.":"")
            + " 회의 주제에 대해 네 전문 포지션에서 발언하라."
            + (agenda.length>1 ? " 현재 안건: \""+item+"\". 이 안건에 집중하라." : "")
            + (r===1 ? " 1라운드: 핵심 의견·제안·우려를 3~5문장으로, 네 성격·말버릇을 살려 제시하라."
                     : " "+r+"라운드: 앞 발언자를 이름으로 직접 호명해 반응하라(예: '서연 님 말처럼…', '민서 님 분석에 덧붙이면…'). 동의·반박·농담을 섞어 티키타카하듯 주고받되, 합의로 수렴할 안을 3~5문장으로 제시하라. 이미 나온 말 반복 금지.")
            + " 다른 부서 매니저는 서로 이름(서연·하늘·유진·민서·소희·채원·세라·다은·아라·지우)으로 편하게 부른다. 회의지만 동료끼리 대화하듯 생기있게.";
          if (kbm) sys += "\n\n[네 부서가 축적한 전문성(지식 베이스) — 이 회의에서 적극 활용하라]\n"+kbm;
          if (mem) sys += "\n\n[네 최근 기록]\n"+mem;
          const log = tline(ai);
          if (log) sys += "\n\n[이 안건의 지금까지 발언]\n"+log;
          sys += steer + prevctx + trendCtx + meetingFeedbackInsights() + profileContext() + clientBlock();
          let text;
          try { text = await genText(sys, "회의 안건: "+item, 700, eng); }
          catch(e1){
            try { await new Promise(r=>setTimeout(r,1500)); text = await genText(sys, "회의 안건: "+item, 700, eng); } // 1회 재시도
            catch(e2){ text = "(이 발언은 일시 오류로 건너뜀)"; logError("meeting-stmt", e2); }
          }
          transcript.push({ dept:d, member:MEMBERS[d]||"", round:r, agenda:ai, text });
          saveDB(); // 발언이 생길 때마다 저장 → 앱이 실시간으로 읽어감
        }
      }
      // 안건별 결론
      const ca = AGENTS[chair];
      const csys = "너는 SNS 자동화 회사 '"+ca.no+" "+ca.kr+"' 부서 AI('"+(MEMBERS[chair]||"")+"')로서 이 ["+mg.label+"] 회의의 의장이다."+ADDRESS
        + " 아래 발언 전체를 종합해 다음 형식으로만 출력하라:\n결론: (합의된 최종 방향 2~3문장)\n결정사항: (번호 목록 2~4개)\n액션: (부서별 한 줄씩 '부서명 — 할 일')"
        + steer + meetingFeedbackInsights() + profileContext();
      let asum;
      try { asum = await genText(csys, "회의 안건: "+item+"\n\n[발언 전체]\n"+transcript.filter(t=>t.agenda===ai).map(t=>"["+t.round+"R] "+spk(t.dept)+": "+t.text).join("\n"), 850, eng); }
      catch(e){ asum = "결론: (요약 생성 일시 오류 — 발언 기록을 참고하세요)"; logError("meeting-sum", e); }
      meeting.agendaSummaries.push({ title:item, summary:asum });
      saveDB();
    }

    // 전체 결론
    if (meeting.agendaSummaries.length === 1) meeting.summary = meeting.agendaSummaries[0].summary;
    else {
      const ca = AGENTS[chair];
      const osys = "너는 이 ["+mg.label+"] 회의의 의장('"+(MEMBERS[chair]||"")+"')이다."+ADDRESS+" 여러 안건의 결론을 한 문단으로 종합하라. 형식:\n종합 결론: (2~3문장)\n핵심 결정: (번호 목록)"+profileContext();
      try { meeting.summary = await genText(osys, "[안건별 결론]\n"+meeting.agendaSummaries.map((x,k)=>(k+1)+". "+x.title+"\n"+x.summary).join("\n\n"), 700, eng); }
      catch(e){ meeting.summary = meeting.agendaSummaries.map(x=>x.title+": "+x.summary).join("\n\n"); logError("meeting-osum", e); }
    }

    // 구조화 State 자산화: 회의 결과를 topic→분석→결론 묶음으로 저장 (검수 이력 포함)
    meeting.state.final_output = meeting.summary;
    meeting.state.market_insight = (meeting.agendaSummaries[0] && meeting.agendaSummaries[0].summary) || "";
    meeting.state.draft_content = meeting.summary;
    // 회의 결론에 검수 게이트 적용(과대광고·톤 점검, 통과할 때까지 최대 3회 다듬기)
    try{
      let text = meeting.summary, revs=[];
      for(let i=0;i<3;i++){
        const rv = await reviewContent(text, topic, eng);
        revs.push({ round:i+1, verdict:rv.pass?"PASS":"FAIL", feedback:rv.feedback });
        meeting.state.revision_count = i;
        if(rv.pass) break;
        meeting.state.critique_feedback = rv.feedback;
        const rsys = "너는 이 회의의 의장('"+(MEMBERS[chair]||"")+"')이다."+ADDRESS+" 아래 회의 결론을 검수부 피드백을 반영해 더 안전하고 완성도 높게 다시 정리하라. 같은 형식(종합 결론/핵심 결정) 유지."+profileContext();
        try{ text = await genText(rsys, "[검수 피드백]\n"+rv.feedback+"\n\n[현재 결론]\n"+text, 800, eng); }catch(e){ break; }
      }
      meeting.summary = text; meeting.state.final_output = text; meeting.reviews = revs;
    }catch(e){ logError("meeting-review", e); }
    meeting.phase = "완료";

    // 학습: 참여 부서 메모리 + 경험치
    for (const d of depts){
      if (!DB.deptMemory[d]) DB.deptMemory[d] = [];
      DB.deptMemory[d].push({ at:Date.now(), instruction:"["+mg.label+" 회의] "+topic, note:"회의 결론: "+(meeting.summary.length>180?meeting.summary.slice(0,180)+"…":meeting.summary) });
      if (DB.deptMemory[d].length > 40) DB.deptMemory[d] = DB.deptMemory[d].slice(-40);
      DB.exp = DB.exp || {}; DB.exp[d] = (DB.exp[d]||0) + 1;
      if (DB.exp[d] % 3 === 0) { try{ await distillKnowledge(d); }catch(e){} } // 회의도 전문성 압축
    }
    meeting.status = "done";
    saveDB();
    kakaoNotify("📋 회의 완료: "+topic+" ("+mg.label+" · 참여: "+depts.map(d=>AGENTS[d].kr).join(", ")+")").catch(()=>{});
    sendPush("📋 회의 완료: "+topic).catch(()=>{});
    return meeting;
  } catch(e){
    meeting.status = "error"; meeting.error = String(e.message||e).slice(0,200);
    saveDB(); logError("meeting", e);
    throw e;
  } finally { runningMeetings--; }
}

async function runMeeting(opts){
  const meeting = createMeeting(opts);
  await processMeeting(meeting);
  return meeting;
}


async function opinion(dept, instruction, teamLog){
  const a = AGENTS[dept];
  const mem = (DB.deptMemory[dept]||[]).slice(-4).map(x=>"· "+x.note).join("\n");
  let sys = "너는 SNS 자동화 회사 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role
    + ADDRESS + " 아래 지시에 대해 지금은 '실행'하지 말고, 네 부서 포지션에서의 핵심 의견·아이디어·기여 포인트·우려를 2~3줄로만 간결히 제시하라.";
  if (MEMBERS[dept] || PERSONA[dept]) sys += " 너의 담당 매니저는 '"+(MEMBERS[dept]||"")+"'이며 성격은 ["+(PERSONA[dept]||"")+"] 이 성격·말투를 자연스럽게 반영하라.";
  if (mem) sys += "\n\n[네 경험·학습]\n"+mem;
  if (teamLog) sys += "\n\n[최근 팀 전체 대화(운영자·타 부서 포함, 이미 공유된 맥락)]\n"+teamLog;
  sys += profileContext();
  return await anthropic(sys, "지시: "+instruction, 700);
}
// 협의 모드 2단계: 총괄이 의견을 종합해 최종 실행 분담을 결정
async function deliberatePlan(instruction, opinions){
  const list = opinions.map(o=>AGENTS[o.dept].no+" "+AGENTS[o.dept].kr+"("+MEMBERS[o.dept]+"): "+o.text).join("\n");
  const sys = ADDRESS + " 너는 SNS 자동화 회사의 총괄 진행자다. 각 부서가 제출한 의견을 놓고, 부서들이 서로의 안을 검토하고 더 나은 아이디어가 있으면 제안하며 함께 토론하는 짧은 라운드테이블을 진행한 뒤, 총괄로서 최종 결정을 내린다.\n"
    + "다음 형식으로만 출력하라:\n토론: (부서 간 핵심 논의와, 채택되거나 기각된 더 나은 아이디어를 3~5줄)\n결정: (최종 방향과 부서별 분담을 1~2문장)\nEXEC: id,id (최종 실행 부서를 협업 순서대로, 영문키)\n"
    + "부서 영문키: " + Object.keys(AGENTS).join(", ") + reactionInsights() + profileContext();
  const out = await anthropic(sys, "지시: "+instruction+"\n\n[제출된 부서 의견]\n"+list, 1300);
  let execIds = [];
  const m = out.match(/EXEC:\s*([a-zA-Z,\s]+)/);
  if (m) execIds = m[1].split(",").map(s=>s.trim().toLowerCase()).filter(id=>AGENTS[id]);
  let discussion = "", note = "";
  const dm = out.match(/토론\s*[:：]\s*([\s\S]*?)(?:결정\s*[:：]|EXEC:|$)/);
  if (dm) discussion = dm[1].trim();
  const nm = out.match(/결정\s*[:：]\s*([\s\S]*?)(?:EXEC:|$)/);
  if (nm) note = nm[1].trim();
  if (!note) note = out.replace(/EXEC:.*$/is, "").replace(/^[\s\S]*?결정\s*[:：]/, "").trim() || out.replace(/EXEC:.*$/is,"").trim();
  if (!execIds.length) execIds = opinions.map(o=>o.dept);
  const tasks = parseTasks(out); return { discussion, note, execIds, tasks };
}
// (커뮤니티·CS 부서 폐지) 과거 반응 인사이트가 남아있으면 참고, 없으면 미사용
function reactionInsights(){
  const mem = (DB.deptMemory && DB.deptMemory.engagement) || [];
  if (!mem.length) return "";
  const recent = mem.slice(-5).map(x=>"· "+x.note).join("\n");
  return "\n\n[과거 수집된 시청자 반응·트렌드 인사이트 — 방향성 판단에 참고]\n" + recent;
}
// 운영 프로필(채널 설정) — 부서가 매번 되묻지 않고 전제로 삼을 공통 맥락
function profileContext(){
  const p = (DB.state && DB.state.profile) || null;
  const rows=[];
  if(p){
    if(p.brand) rows.push("브랜드/채널: "+p.brand);
  if(p.topic) rows.push("주제·분야: "+p.topic);
  if(p.audience) rows.push("타깃 시청자: "+p.audience);
  if(p.character) rows.push("대표 캐릭터/화자: "+p.character);
  if(p.tone) rows.push("톤&무드: "+p.tone);
  if(p.platforms) rows.push("주요 플랫폼: "+p.platforms);
    if(p.avoid) rows.push("피해야 할 것: "+p.avoid);
    if(p.extra) rows.push("기타 지침: "+p.extra);
  }
  const profileTxt = rows.length ? ("\n\n[운영 프로필 — 모든 콘텐츠·제안의 기본 전제. 이 정보로 충분하니 사용자에게 되묻지 말고 바로 진행하라]\n" + rows.map(r=>"· "+r).join("\n")) : "";
  return profileTxt + brandBlock() + lessonsBlock();
}
// 🧠 실수 규칙(반복 금지) 장부 — 과거 실패·AI 오류·거짓말 수법을 규칙으로 누적해 모든 생성에 주입, 재발 방지
// v223: '금지 목록'은 쌓일수록 모델의 발상을 가둔다(제약 → 맥락 전환).
// 규칙을 "~하지 마라"가 아니라 "이렇게 하라"는 지향문(guide)으로 저장·주입하고,
// 오래되고 안 쓰이는 규칙은 만료시켜 프롬프트가 무한히 부풀지 않게 한다.
const LESSON_MAX_INJECT = 10;                    // 프롬프트에 넣는 최대 개수
const LESSON_TTL_MS = 180*24*3600*1000;          // 180일 지난 규칙은 주입 제외(고정 규칙 제외)
function lessonText(x){ return String((x&&(x.guide||x.text))||"").trim(); }
// ===== 🎨 v224: 브랜드 디자인 시스템 (design.md 역할) =====
// AI는 매번 기억이 없어서 화면마다 색·여백·버튼이 제각각이 된다.
// 고정 도면(색·폰트·간격·금지목록)을 모든 HTML 생성에 주입해 "한 브랜드"로 보이게 한다.
function designSystemBlock(){
  const ds = (DB.state && DB.state.designSystem) || null;
  const txt = ds && ds.on !== false ? String(ds.text||"").trim() : "";
  if(!txt) return "";
  return "\n\n[민앤팜 브랜드 디자인 시스템 — 이 도면을 반드시 지켜라]\n"
    + txt.slice(0,1800)
    + "\n(색·폰트·간격·모서리는 위 값을 '그대로' 쓴다. 새로 지어내지 마라. "
    + "미학 스타일·레이아웃 구성·시그니처 요소만 이번 주제에 맞게 새로 정하라. "
    + "위 '쓰지 않을 것' 항목은 어떤 경우에도 쓰지 마라.)";
}
// v226: 생성된 HTML이 '열어보면 백지'인지 서버에서 먼저 검사한다.
// 잘림(</html> 없음), 본문 없음, JS 의존 리빌 → 사용자가 빈 화면을 보기 전에 잡는다.
function checkHtmlOutput(html){
  const h = String(html||"");
  const problems = [];
  if(h.length < 300) problems.push("HTML이 너무 짧음("+h.length+"자)");
  if(!/<\/html\s*>/i.test(h)) problems.push("문서가 잘림(</html> 없음)");
  const bm = h.match(/<body[^>]*>([\s\S]*)<\/body>/i);
  const body = bm ? bm[1] : "";
  const textLen = body.replace(/<script[\s\S]*?<\/script>/gi,"").replace(/<style[\s\S]*?<\/style>/gi,"").replace(/<[^>]+>/g," ").replace(/\s+/g," ").trim().length;
  if(!bm) problems.push("<body>가 없음");
  else if(textLen < 80) problems.push("본문 글자가 거의 없음("+textLen+"자)");
  return { ok: problems.length===0, problems, len:h.length, textLen };
}
// JS 없이는 안 보이는 리빌 패턴을 CSS로 무력화 (안전망)
function hardenHtml(html){
  let h = String(html||"");
  if(!/<\/body\s*>/i.test(h)) return h;
  const guard = '<style id="__visguard">'
    + '/* 안전망: JS가 실패해도 콘텐츠가 보이도록 */'
    + '[class*="reveal"],[class*="fade"],[class*="animate"],[data-aos],[data-reveal]{opacity:1!important;visibility:visible!important;transform:none!important}'
    + '</style>';
  return h.replace(/<\/body\s*>/i, guard+"</body>");
}
function hasDesignSystem(){
  const ds = (DB.state && DB.state.designSystem) || null;
  return !!(ds && ds.on !== false && String(ds.text||"").trim());
}
function lessonsBlock(){
  const now = Date.now();
  const arr = (DB.lessons||[]).filter(x=>x && (x.guide||x.text))
    .filter(x=> x.pinned || !x.at || (now - x.at) < LESSON_TTL_MS);
  if(!arr.length) return "";
  // 고정(pinned) 우선 → 최신 순
  const sorted = arr.slice().sort((a,b)=> (b.pinned?1:0)-(a.pinned?1:0) || (b.at||0)-(a.at||0));
  const rows = sorted.slice(0, LESSON_MAX_INJECT).map(x=>"· "+lessonText(x)).join("\n");
  return "\n\n[우리 팀의 품질 원칙 — 과거 경험에서 뽑은 것. 이 방향으로 만들어라(금지 목록이 아니라 지향점이다)]\n"+rows;
}
// 금지문("~하지 마라")을 같은 뜻의 지향문("~하라")으로 바꾼다. 실패하면 원문 유지.
async function toGuideline(text){
  const t = String(text||"").trim();
  if(!t) return "";
  try{
    const sys = "너는 프롬프트 엔지니어다. 아래는 AI가 저지른 실수에서 뽑은 '금지 규칙'이다. "
      + "이걸 같은 의미의 '지향문'으로 한 줄로 바꿔라. 금지형(~하지 마라, ~금지)이 아니라 "
      + "'무엇을 어떻게 하라'는 긍정 지시로. 금지 대상 단어를 그대로 반복하지 말고, 올바른 행동을 구체적으로 적어라. "
      + "예) '가짜 데이터로 테스트 통과라고 보고하지 마라' → '테스트 결과는 실제 실행 로그를 근거로만 보고하고, 실행하지 못했으면 못했다고 밝혀라'. "
      + "따옴표·머리말·설명 없이 한국어 한 줄만 출력.";
    const out = await anthropic(sys, t, 220);
    const g = String(out||"").trim().replace(/^[\"'“]|[\"'”]$/g,"").split("\n")[0].trim();
    return (g && g.length>5) ? g.slice(0,300) : t;
  }catch(e){ return t; }
}
// 아주 비슷한 규칙이면 새로 쌓지 말고 교체(병합) — 중복 누적 방지
function _lsnNorm(x){ return String(x||"").replace(/[\s.,!?·\-—"'"'()[\]]/g,"").toLowerCase(); }
function _lsnSim(a,b){
  const A=_lsnNorm(a), B=_lsnNorm(b);
  if(!A||!B) return 0;
  const gram = (s)=>{ const g=new Set(); for(let i=0;i<s.length-1;i++) g.add(s.slice(i,i+2)); return g; };
  const ga=gram(A), gb=gram(B);
  if(!ga.size||!gb.size) return 0;
  let inter=0; ga.forEach(v=>{ if(gb.has(v)) inter++; });
  return inter / (ga.size + gb.size - inter);
}
function findSimilarLesson(text){
  const arr = DB.lessons||[];
  for(const x of arr){ if(_lsnSim(text, x.text||"")>=0.55 || _lsnSim(text, x.guide||"")>=0.55) return x; }
  return null;
}
function parseTasks(out){
  var tasks={}; var mt = (out||"").match(/TASKS:\s*([^\n]+)/i);
  if(mt){ mt[1].split("|").forEach(function(p){ var i=p.indexOf("="); if(i>0){ var id=p.slice(0,i).trim().toLowerCase(); var t=p.slice(i+1).trim(); if(AGENTS[id]&&t) tasks[id]=t.slice(0,24); } }); }
  return tasks;
}
async function leadPlan(instruction, teamLog){

  const list = Object.keys(AGENTS).map(id => id+" = "+AGENTS[id].no+" "+AGENTS[id].kr+" ("+MEMBERS[id]+")").join("\n");
  const sys = ADDRESS + " 너는 SNS 자동화 회사의 총괄 대리인이다. 클라이언트님의 지시를 받아 실행을 설계한다.\n"
    + "1) 작업을 처음부터 끝까지(필요 시 조사→기획→제작→발행→분석 등) 생각해, 기여할 수 있는 부서를 충분히 폭넓게 참여시켜라. 한두 부서에만 몰지 말고, 각 부서가 어떤 아이디어·역할을 맡을지 한 줄씩 분담하라.\n"
    + "2) 단, 정말 관련 없는 부서는 빼라(억지로 다 넣지 말 것).\n"
    + "출력: 먼저 2~4문장으로 기획(접근 방향 + 부서별 역할/요청 아이디어)을 쓰고, 그 다음 줄에 'EXEC: id,id,id' 형식으로 협업 실행 순서대로 부서 영문키를 적고, 마지막 줄에 각 부서가 무엇을 만드는지 'TASKS: id=할일요약 | id=할일요약' 형식으로 적어라(할일요약은 한국어 12자 내외, 예: strategy=채널 콘셉트 기획).\n[부서 목록]\n" + list + reactionInsights() + profileContext()
    + (teamLog ? "\n\n[최근 팀 전체 대화 — 운영자·각 부서가 이 공용 콘솔에서 나눈 내용. 연속된 대화이니 흐름을 이어서 설계하라]\n"+teamLog : "");
  const out = await anthropic(sys, "지시: "+instruction, 1100);
  let execIds = [];
  const m = out.match(/EXEC:\s*([a-zA-Z0-9_,\s]+)/);
  if (m) execIds = m[1].split(",").map(s=>s.trim().toLowerCase()).filter(id=>AGENTS[id]);
  const tasks = parseTasks(out);
  const note = out.replace(/EXEC:.*$/is, "").replace(/TASKS:.*$/is, "").trim();
  if (!execIds.length) execIds = await route(instruction);
  return { note, execIds, tasks };
}
async function handleInstruction(instruction, source, images, history, shell){
  try{ recordClient(instruction); }catch(e){}
  let depts = directDept(instruction);
  const direct = depts.length > 0;
  const deliberate = !!(DB.state && DB.state.deliberate);
  const job = shell || { id:Date.now(), type:"instruct", instruction, source:source||"api", at:Date.now() };
  // 진행 상황 기록(앱이 폴링해 '현재 작업 내용'으로 표시)
  function kr(d){ return AGENTS[d] ? AGENTS[d].kr : d; }
  function setProg(list, idx){
    var done = (list||[]).slice(0, idx).map(kr);
    var curId = (list && list[idx]!==undefined) ? list[idx] : null;
    var cur = curId ? kr(curId) : null;
    var task = (job.tasks && curId && job.tasks[curId]) ? job.tasks[curId] : "";
    var t = "";
    if(done.length) t += "완료: "+done.join(" · ")+"  ";
    if(cur) t += "→ "+cur+(task?(" — "+task):"")+" 작성 중 ("+(idx+1)+"/"+list.length+")";
    job.progress = t || "진행 중…";
    job.progAt = Date.now();
    if(shell){ shell.progAt = Date.now(); try{ saveDB(); }catch(e){} }
  }
  job.progress = "총괄이 지시를 분석하고 부서에 분담하는 중…";
  if(shell){ try{ saveDB(); }catch(e){} }

  // 최근 팀 전체 대화 맥락(부서 공유): 앱이 보낸 화면 대화 우선, 없으면 백엔드 작업기록에서 복원
  let teamLog = "";
  if (history && String(history).trim()){
    teamLog = String(history).trim();
  } else {
    teamLog = (DB.jobs||[]).slice(-4).map(function(j){
      var head = "운영자: " + (j.instruction||"");
      var reps = (j.results||[]).map(function(r){ return (AGENTS[r.dept]?AGENTS[r.dept].no+" "+AGENTS[r.dept].kr:r.dept)+": "+String(r.text||"").slice(0,220); }).join("\n");
      return head + (reps?"\n"+reps:"");
    }).join("\n\n");
  }
  if (teamLog.length > 3000) teamLog = "…" + teamLog.slice(-3000);

  // 누적 협업 맥락: 모든 앞 부서 결과를 요약해 다음 부서에 전달 (몇 부서만 일하지 않도록)
  function buildCtx(base, results){
    let c = base ? base+"\n" : "";
    c += results.map(r=>"["+AGENTS[r.dept].no+" "+AGENTS[r.dept].kr+" "+MEMBERS[r.dept]+"]\n"+(r.text.length>600?r.text.slice(0,600)+"…":r.text)).join("\n\n");
    return c;
  }

  if (direct) {
    // 부서·이름을 콕 집으면 협의 없이 그 담당이 바로 처리
    const results = []; job.depts = depts;
    for (let i=0;i<depts.length;i++){ const d=depts[i]; setProg(depts, i); job.results=results;
      const t = await work(d, instruction, buildCtx("", results), images, teamLog); results.push({ dept:d, text:t }); if(shell){try{saveDB();}catch(e){}} }
    job.direct = true; job.depts = depts; job.results = results;
  } else if (deliberate) {
    // 협의 모드: 1) 부서 의견 수렴 → 2) 총괄 재분담 → 3) 협업 실행
    job.progress = "부서들이 의견을 내는 중…"; if(shell){try{saveDB();}catch(e){}}
    const candidates = await route(instruction);
    const opinions = [];
    for (let i=0;i<candidates.length;i++){ const d=candidates[i]; job.progress="의견 수렴: "+kr(d)+" ("+(i+1)+"/"+candidates.length+")"; if(shell){try{saveDB();}catch(e){}} try{ opinions.push({ dept:d, text: await opinion(d, instruction, teamLog) }); }catch(e){} }
    job.progress = "총괄이 의견을 종합해 분담하는 중…"; if(shell){try{saveDB();}catch(e){}}
    const plan = await deliberatePlan(instruction, opinions);
    job.tasks = plan.tasks || {};
    const base = (plan.note?"[총괄 최종 결정] "+plan.note+"\n":"") + (plan.discussion?"[부서 토론 요지] "+plan.discussion+"\n":"") + "[각 부서가 제출한 아이디어]\n" + opinions.map(o=>AGENTS[o.dept].no+" "+AGENTS[o.dept].kr+": "+o.text).join("\n");
    const results = []; job.depts = plan.execIds;
    for (let i=0;i<plan.execIds.length;i++){ const d=plan.execIds[i]; setProg(plan.execIds, i); job.results=results;
      const t = await work(d, instruction, buildCtx(base, results), images, teamLog); results.push({ dept:d, text:t }); if(shell){try{saveDB();}catch(e){}} }
    job.deliberate = true; job.candidates = candidates; job.opinions = opinions;
    job.discussion = plan.discussion; job.plan = plan.note; job.depts = plan.execIds; job.results = results;
  } else {
    // 일반 지시: 총괄이 받아서 기획(아이디어 분담) → 부서들이 협업 수행
    const plan = await leadPlan(instruction, teamLog);
    job.tasks = plan.tasks || {};
    const base = plan.note ? "[총괄 기획·분담] "+plan.note : "";
    const results = []; job.depts = plan.execIds;
    for (let i=0;i<plan.execIds.length;i++){ const d=plan.execIds[i]; setProg(plan.execIds, i); job.results=results;
      const t = await work(d, instruction, buildCtx(base, results), images, teamLog); results.push({ dept:d, text:t }); if(shell){try{saveDB();}catch(e){}} }
    job.plan = plan.note; job.depts = plan.execIds; job.results = results;
  }

  if (!shell) DB.jobs.push(job);
  job.status = "done"; job.progress = "";
  if ((job.depts||[]).length > 1 || job.deliberate)
    DB.meetings.push({ id:job.id, at:job.at, instruction, depts:job.depts, opinions:job.opinions, discussion:job.discussion, plan:job.plan, exchanges:job.results });
  saveDB();
  return job;
}

// ========================= 엔드포인트 =========================
app.get("/", (req,res)=> res.send("SNS 에이전트 확장 백엔드 작동 중"));
let KEEPALIVE_ON=false; // 자기-핑 활성 여부(서버 자동 깨우기)
// 외부 핑(Keep-alive): 서버를 깨우고, 근무 중이고 자율수행이 밀렸으면 그 자리에서 한 사이클 실행 + 상태 보고
let _lastPingAt = 0, _lastPingSavedAt = 0;
app.get("/api/ping", (req,res)=>{
  _lastPingAt = Date.now();
  // 핑 시각을 가끔만 저장(매 핑마다 저장하면 낭비) — 재시작 후에도 cron 작동 확인 가능
  if (Date.now() - _lastPingSavedAt > 10*60000){ _lastPingSavedAt = Date.now(); DB.lastPingAt = _lastPingAt; try{ saveDB(); }catch(e){} }
  const col = (DB.state && DB.state.collect) || {};
  const on = col.everyMin > 0;
  const working = (function(){ try{ return isWorking(); }catch(e){ return true; } })();
  const sinceMin = DB.lastCollectAt ? Math.round((Date.now()-DB.lastCollectAt)/60000) : null;
  const due = on && working && (Date.now() - (DB.lastCollectAt||0) >= col.everyMin*60000);
  if (due) { runAutoCycle().catch(e=>logError("ping-autocycle", e)); } // 핑이 트리거가 되어 평소 수행이 이어짐
  checkStaleDepts().catch(e=>logError("ping-stale", e)); // 서버가 깰 때마다 24h+ 정체 부서를 팀장이 점검·재지시
  res.json({ ok:true, awake:true, keepAlive:KEEPALIVE_ON, working, autonomy:{ on, everyMin:col.everyMin||0, lastRunMinAgo:sinceMin, ranNow:!!due }, gemini:geminiRateInfo(), ts:Date.now() });
});
// 진단: 브라우저로 열어 cron·키·학습 상태를 한눈에 확인 (값은 노출하지 않고 설정 여부만)
app.get("/api/diag", (req,res)=>{
  const has = (k)=> !!(process.env[k] && String(process.env[k]).trim());
  const pingAt = _lastPingAt || DB.lastPingAt || 0;
  const pingAgoMin = pingAt ? Math.round((Date.now()-pingAt)/60000) : null;
  const jobs = (DB.pageJobs||[]);
  const knows = Object.keys(DB.deptKnowledge||{});
  res.json({
    ok:true,
    시각: new Date().toLocaleString("ko-KR",{timeZone:"Asia/Seoul"}),
    "1_서버깨우기(cron)": {
      마지막_핑: pingAt ? new Date(pingAt).toLocaleString("ko-KR",{timeZone:"Asia/Seoul"}) : "없음",
      몇분전: pingAgoMin,
      판정: (pingAgoMin!=null && pingAgoMin<=15) ? "✅ cron 정상 작동 중" : (pingAt ? "⚠️ 최근 핑 없음 — cron 확인 필요" : "❌ 핑 기록 없음 — cron 미설정")
    },
    "2_API키_설정여부": {
      ANTHROPIC_API_KEY: has("ANTHROPIC_API_KEY") ? "✅ 설정됨" : "❌ 없음",
      GEMINI_API_KEY: has("GEMINI_API_KEY") ? "✅ 설정됨" : "❌ 없음",
      OPENAI_API_KEY: has("OPENAI_API_KEY") ? "✅ 설정됨(이미지 생성 가능)" : "❌ 없음(🎨 이미지 생성 불가)",
      openai_소문자_잘못된키: has("openai_api_key") ? "⚠️ 소문자 키가 남아있음 — 지워도 됩니다" : "없음(정상)",
      KAKAO: (has("KAKAO_ACCESS_TOKEN")||has("KAKAO_REFRESH_TOKEN")) ? "✅ 설정됨(카톡 알림 가능)" : "❌ 없음(카톡 알림 불가)",
      SUPABASE: (has("SUPABASE_URL")&&has("SUPABASE_KEY")) ? "✅ 설정됨(데이터 보존)" : "❌ 없음(데이터 유실 위험)",
      APP_PASSWORD: has("APP_PASSWORD") ? "✅ 설정됨(백엔드 보호)" : "⚠️ 없음 — 누구나 백엔드 접근 가능"
    },
    "3_데이터_안전": {
      부팅시_상태: (typeof _bootRestoreNote!=="undefined" && _bootRestoreNote) ? _bootRestoreNote : "?",
      Supabase_접속: (typeof _bootRestoreOk!=="undefined") ? (_bootRestoreOk ? "✅ 접속 성공(저장 가능)" : "❌ 접속 실패(저장 보류 — 데이터 보호 중)") : "?",
      실제_저장상태: (function(){
        if(!useSupabase) return "파일 모드(Supabase 미설정)";
        if(_lastSaveErr){
          const rls = /42501|row-level security/i.test(_lastSaveErr);
          return "❌ 저장 실패("+_saveFailStreak+"회): "+_lastSaveErr + (rls ? "  → 해결: Render의 SUPABASE_KEY를 Supabase service_role 키로 교체하세요" : "");
        }
        return _lastSaveOkAt ? ("✅ 저장 성공 — 마지막 "+new Date(_lastSaveOkAt).toLocaleString("ko-KR",{timeZone:"Asia/Seoul"}))
                             : "⏳ 아직 저장 시도 없음(작업하면 저장됨)";
      })(),
      현재_내용유무: (function(){ try{ return dbHasContent(DB) ? "✅ 내용 있음" : "비어있음(정상 — 작업하면 쌓임)"; }catch(e){ return "?"; } })()
    },
    "4_자동학습": {
      마지막_벤치마크_학습일: DB.lastBenchmarkDay || "아직 없음",
      학습된_부서수: knows.length,
      학습된_부서: knows,
      팀장_총괄기준: (function(){ const o=(DB.deptKnowledge||{}).ops; return o&&o.text ? ("✅ 보유 ("+(o.benchmark||"")+")") : "아직 없음 — 부서가 학습하면 자동 흡수됩니다"; })(),
      다음_학습_차례: (function(){ try{ const o=Object.keys(DEPT_BENCHMARK); return o[(DB.benchmarkTurn||0)%o.length]; }catch(e){ return "?"; } })(),
      실행조건: "서버가 깨어있는 아무 시간에 하루 1회(cron 필수)",
      유튜브학습_검사: "/api/diag/youtube 를 열면 실제 작동 여부를 검사합니다"
    },
    "5_상품페이지_작업": {
      전체: jobs.length,
      진행중: jobs.filter(j=>j.status==="running"||j.status==="queued").length,
      완료: jobs.filter(j=>j.status==="done").length,
      실패: jobs.filter(j=>j.status==="error").length,
      최근: jobs.slice(-3).map(j=>({ 상품:j.product, 상태:j.status, 오류:j.error||"" }))
    },
    "6_부서경험치": DB.exp || {},
    "7_팀장_플랫폼변경_승인대기": (function(){
      const p=(DB.patchProposals||[]).filter(x=>x.status==="pending");
      return p.length ? p.map(x=>({ 제목:x.title, 이유:x.reason })) : "없음";
    })()
  });
});
// Gemini가 실제로 되는지 라이브 검사 (기본 호출 / 웹검색 grounding) — 실패하면 원문 오류를 그대로 보여준다
app.get("/api/diag/gemini", async (req,res)=>{
  const out = { ok:true, 단계:{} };
  try{
    out.단계["0_키"] = geminiKey() ? "✅ 있음" : "❌ 없음";
    out.쿨다운 = (Date.now() < geminiCooldownUntil)
      ? ("⏳ 한도 초과로 대기 중 — "+Math.ceil((geminiCooldownUntil-Date.now())/1000)+"초 남음")
      : "없음";
    out.오늘_웹검색_횟수 = (DB.geminiSearchDaily && DB.geminiSearchDaily.n) || 0;
    out.요금_모드 = paidModeOn() ? "유료(승인 필요)" : "무료";
    out.웹검색_한도 = (function(){ const c=searchDailyCap(); return c<=0 ? "무제한(무료)" : (c+"회/일"); })();
    out.웹검색_허용 = (function(){ try{ return searchAllowedNow() ? "✅ 가능" : "❌ 상한 도달"; }catch(e){ return "?"; } })();
    if(!geminiKey()){ out.결론="GEMINI_API_KEY가 없어 학습 불가"; return res.json(out); }
    try{
      const t = await geminiText("한 단어로만 답하라: 사과의 색은?", 20);
      out.단계["1_기본_호출"] = "✅ 성공 — 응답: "+String(t).slice(0,40);
    }catch(e){ out.단계["1_기본_호출"] = "❌ 실패: "+String(e.message||e).slice(0,220); }
    try{
      const s = await geminiSearch("오늘 대한민국 날씨를 한 줄로", 200);
      out.단계["2_웹검색(grounding)"] = "✅ 성공 — "+String((s&&s.text)||"").slice(0,60);
      out.검색_출처수 = ((s&&s.sources)||[]).length;
    }catch(e){ out.단계["2_웹검색(grounding)"] = "❌ 실패: "+String(e.message||e).slice(0,220); }
    const ok1 = String(out.단계["1_기본_호출"]||"").startsWith("✅");
    const ok2 = String(out.단계["2_웹검색(grounding)"]||"").startsWith("✅");
    out.결론 = (ok1&&ok2) ? "✅ Gemini 정상 — 학습 가능"
             : (!ok1 ? "❌ Gemini 기본 호출부터 실패 (키 권한·모델명·한도 확인)"
                     : "⚠️ 기본 호출은 되지만 웹검색(grounding)이 실패 — 벤치마크 학습 불가");
  }catch(e){ out.ok=false; out.error=String(e.message||e).slice(0,220); }
  res.json(out);
});
// 웹페이지를 실제로 가져올 수 있는지 검사 (차단·403 확인)
app.get("/api/diag/fetch", async (req,res)=>{
  const url = String(req.query.url||"");
  if(!/^https?:\/\/.+/.test(url)) return res.json({ ok:false, note:"?url=https://... 형식으로 주소를 주세요" });
  try{
    const t0 = Date.now();
    const html = await fetchPageText(url);
    res.json({ ok:true, url, 소요초: Math.round((Date.now()-t0)/1000),
      본문길이: html.length,
      판정: html.length>200 ? "✅ 수집 성공 — 학습에 쓸 수 있어요" : "⚠️ 본문이 너무 짧음(차단 페이지·JS 렌더링 사이트일 수 있음)",
      미리보기: html.replace(/<[^>]+>/g," ").replace(/\s+/g," ").slice(0,200) });
  }catch(e){ res.json({ ok:false, url, 오류:String(e.message||e).slice(0,220),
      힌트:"403/봇 차단이면 그 사이트는 학습에 못 씁니다. 다른 페이지를 쓰세요." }); }
});
// 유튜브 학습이 실제로 되는지 라이브 검사 (검색 → 영상 분석 순서로 확인)
app.get("/api/diag/youtube", async (req,res)=>{
  const out = { ok:true, 단계:{} };
  try{
    const key = process.env.YT_API_KEY || process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY || "";
    out.단계["0_키"] = key ? "✅ 있음" : "❌ 없음";
    if(!key){ out.결론="키가 없어 유튜브 학습 불가"; return res.json(out); }
    const vids = await ytSearchTop(String(req.query.q||"고흥 여행"), 2);
    out.단계["1_유튜브검색"] = vids.length ? ("✅ 성공 — "+vids.length+"개 찾음") : "❌ 실패(YouTube Data API 미활성화) — 검색 없이도 URL 직접 분석은 가능";
    out.찾은영상 = vids.map(v=>({ 제목:v.title, url:v.url }));
    // 테스트 편의: ?url=https://youtu.be/ID  또는  ?v=ID  둘 다 허용
    const vId = String(req.query.v||"").trim();
    const target = vids.length ? vids[0].url : (vId ? ("https://youtu.be/"+vId) : String(req.query.url||""));
    if(!target){ out.결론 = "검색 실패 + 분석할 URL 없음 → /api/diag/youtube?v=영상ID 로 직접 테스트하세요"; return res.json(out); }
    try{
      const a = await analyzeYouTube(target, "이 영상을 한 문장으로 요약하라.", 200);
      out.단계["2_영상분석(화면+음성)"] = "✅ 성공";
      out.분석샘플 = String(a.text||"").slice(0,200);
      out.결론 = vids.length ? "✅ 유튜브 자동 학습 완전 작동" : "⚠️ 검색은 안 되지만 URL 직접 분석은 작동";
    }catch(e){
      out.단계["2_영상분석(화면+음성)"] = "❌ 실패: "+String(e.message||e).slice(0,140);
      out.결론 = "영상 분석 불가 — Gemini 키 권한/모델 확인 필요";
    }
  }catch(e){ out.ok=false; out.error=String(e.message||e).slice(0,200); }
  res.json(out);
});
// 수동: 팀장이 지금 정체 부서를 점검해 재지시 (오피스 버튼)
app.post("/api/ops/check-stale", async (req,res)=>{
  try{
    const before = (DB.leadDirectives||[]).length;
    await checkStaleDepts(10, true);
    const added = (DB.leadDirectives||[]).slice(before);
    res.json({ ok:true, newDirectives: added, total:(DB.leadDirectives||[]).length });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// ===== 로그인 · 권한 =====
import crypto from "crypto";
const APP_PASSWORD = process.env.APP_PASSWORD || "";   // 미설정 시 인증 없이 개방
function makeToken(){ return crypto.createHash("sha256").update("tok:"+APP_PASSWORD).digest("hex").slice(0,32); }
function requireAuth(req,res,next){
  if (!APP_PASSWORD) return next();                    // 비밀번호 미설정 = 개방
  const t = req.headers["x-auth-token"] || req.query.token || "";
  if (t === makeToken()) return next();
  return res.status(401).json({ error:"인증 필요" });
}
app.post("/api/login", (req,res)=>{
  if (!APP_PASSWORD) return res.json({ token:"", open:true });
  if ((req.body && req.body.password) === APP_PASSWORD) return res.json({ token: makeToken() });
  res.status(401).json({ error:"비밀번호가 올바르지 않습니다" });
});

// ===== 웹푸시 (VAPID) =====
// 환경변수: VAPID_PUBLIC, VAPID_PRIVATE, VAPID_SUBJECT(mailto:you@x.com)
let webpush = null;
try {
  if (process.env.VAPID_PUBLIC && process.env.VAPID_PRIVATE) {
    webpush = (await import("web-push")).default;
    webpush.setVapidDetails(process.env.VAPID_SUBJECT || "mailto:admin@example.com", process.env.VAPID_PUBLIC, process.env.VAPID_PRIVATE);
  }
} catch(e){ console.error("web-push 미설치", e.message); }
async function sendPush(text){
  if (!webpush) return;
  const subs = (DB.state && DB.state.pushSubs) || [];
  for (const s of subs) {
    try { await webpush.sendNotification(s, JSON.stringify({ title:"SNS 에이전트", body:text })); }
    catch(e){ /* 만료 구독은 무시 */ }
  }
}
app.get("/api/push/key", (req,res)=> res.json({ key: process.env.VAPID_PUBLIC || "" }));
app.post("/api/push/subscribe", (req,res)=>{
  DB.state = DB.state || {};
  DB.state.pushSubs = DB.state.pushSubs || [];
  const sub = req.body;
  if (sub && sub.endpoint && !DB.state.pushSubs.find(s=>s.endpoint===sub.endpoint)) { DB.state.pushSubs.push(sub); saveDB(); }
  res.json({ ok:true });
});

// ===== 콘텐츠 캘린더 (예약 + 발행 이력 날짜별) =====
app.get("/api/calendar", (req,res)=>{
  const days = {};
  (DB.scheduled||[]).forEach(t=>{ const d=new Date(t.runAt).toISOString().slice(0,10); (days[d]=days[d]||{scheduled:[],published:[]}).scheduled.push({ id:t.id, instruction:t.instruction, done:!!t.done, runAt:t.runAt }); });
  (DB.jobs||[]).filter(j=>j.type==="publish").forEach(j=>{ const d=new Date(j.at).toISOString().slice(0,10); (days[d]=days[d]||{scheduled:[],published:[]}).published.push({ platforms:j.platforms, ok:j.ok, count:j.count, at:j.at }); });
  res.json(days);
});


// (앱 호환) 단순 AI 호출: { system, user } → { text }
app.post("/api/claude", async (req,res)=>{
  try { const { system, user } = req.body||{}; if(!user) return res.status(400).json({error:"user 필요"});
    res.json({ text: await anthropic(system, user) });
  } catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// 통합 지시: { instruction, source } → { job }
app.post("/api/instruct", (req,res)=>{
  try { const { instruction, source, images, history } = req.body||{}; if(!instruction) return res.status(400).json({error:"instruction 필요"});
    // 작업 셸을 먼저 저장하고 즉시 응답 → 앱을 닫아도 서버가 백그라운드에서 계속 수행
    const shell = { id:Date.now()+Math.floor(Math.random()*1000), type:"instruct", instruction, source:source||"app", at:Date.now(), status:"running" };
    DB.jobs.push(shell); saveDB();
    handleInstruction(instruction, source, images, history, shell)
      .then((ret)=>{
        // 어떤 종료 경로로 끝나든 결과를 셸에 반영하고 반드시 done 처리 (중간 return로 status 누락 방지)
        if (ret && typeof ret==="object" && ret!==shell){
          ["results","depts","plan","discussion","opinions","reply","deliberate","candidates","note"].forEach(function(k){ if(ret[k]!==undefined && shell[k]===undefined) shell[k]=ret[k]; });
        }
        if (shell.status==="running") shell.status="done";
        saveDB();
      })
      .catch(e=>{ shell.status="error"; shell.error=String(e.message||e).slice(0,200); saveDB(); logError("instruct", e); });
    res.json({ ok:true, id:shell.id, status:"running" });
  } catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 회의 미결 항목 추천안 — '미결/확인 필요' 항목마다 채택 가능한 구체안 제시
app.post("/api/meeting/suggest-unresolved", async (req,res)=>{
  try{
    const body=req.body||{};
    const mt=(DB.meetings||[]).find(x=>x.id===+body.id);
    const topic=(mt&&mt.topic)||body.topic||"";
    const summary=(mt&&mt.summary)||body.summary||"";
    if(!summary) return res.json({ ok:true, text:"" });
    const sys="너는 이 회사의 전략·운영 총괄이다."+ADDRESS
      +" 아래 회의 결론에는 '미결' 또는 '클라이언트 확인 필요'로 남은 항목이 있다. 각 미결/확인 항목마다 클라이언트가 바로 채택할 수 있는 '구체적 추천안'을 한 줄씩 제시하라."
      +" 형식: 줄마다 '· 항목: 추천안 (근거 한마디)'. 결정 가능한 값(숫자·시점·방식)으로, 군더더기 없이. 한국어로만."+profileContext();
    const out=await anthropic(sys, "회의 주제: "+topic+"\n\n[회의 결론]\n"+summary, 1300);
    res.json({ ok:true, text: out });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 회의 내용 → 콘텐츠 직접 생성(라우터 거치지 않고 제작부가 완성형 콘텐츠 반환 → 콘텐츠 탭에 바로 표시)
// ===== 검수 게이트: 감사(09)가 콘텐츠를 PASS/FAIL 판정 → FAIL이면 제작부가 재작성(최대 3회) =====
async function reviewContent(text, topic, engine){
  const rev="advisory"; // 09 자문·서기(감사·법무·리스크 성향)가 검수
  const a=AGENTS[rev]||AGENTS["ops"];
  let sys="너는 SNS 자동화 회사의 '"+a.no+" "+a.kr+"' 부서 AI로, 콘텐츠 품질·규정 검수를 맡는다."+ADDRESS
    +" 아래 SNS 콘텐츠를 냉정하게 검수하라. 기준: ①과대·허위광고 요소 없음(의학적 효능 단정, '최고/유일' 등 객관적 근거 없는 표현) ②브랜드 톤앤매너 적합 ③플랫폼 정책 위반 없음 ④가독성·후킹·해시태그 적절 ⑤한국어 표현이 자연스럽고 어색한 번역투·비문·오탈자 없음 ⑥요청된 형식(제목·소제목·해시태그·메타설명 등)을 지킴 ⑦근거 없는 수치·통계·상(賞)·인증·후기 등 '지어낸 사실(환각)'이 없음 — 확인 불가한 구체 주장은 빼거나 근거를 요구. "
    +"반드시 아래 형식 한 줄만 출력(설명 금지):\nVERDICT: PASS 또는 FAIL | (FAIL이면 무엇을 어떻게 고칠지 구체적 지시 한두 문장)";
  const out=await genText(sys, "주제: "+(topic||"")+"\n\n[검수 대상 콘텐츠]\n"+String(text).slice(0,1800), 500, engine);
  const m=String(out).match(/VERDICT:\s*(PASS|FAIL)\s*\|?\s*(.*)/i);
  if(!m) return { pass:true, feedback:"" }; // 파싱 실패 시 통과(막지 않음)
  return { pass:/PASS/i.test(m[1]), feedback:String(m[2]||"").trim() };
}
// 🔥 v224: 바이럴 점수 게이트 — "규정 위반 없음"만 보지 말고 "이게 실제로 먹힐까"도 채점한다.
// 발행 전에 점수를 매겨 낮으면 1회 개선. 점수는 결과에 함께 남겨 성과 학습에 쓴다.
function viralGateOn(){ return !!(DB.state && DB.state.viralGate); }
function viralMin(){ const v=+((DB.state&&DB.state.viralMin)||70); return (v>=50&&v<=95)?v:70; }
async function scoreViral(text, topic, kindLabel){
  const bench = knowledgeText("strategy");   // 벤치마크로 축적한 후킹·트렌드 지식
  const sys = "너는 SNS·콘텐츠 성과 분석가다. 아래 콘텐츠가 '실제로 반응이 올지'를 냉정하게 채점하라. "
    + "채점 축(각 20점): ①첫 3초 후킹(스크롤을 멈추게 하는가) ②구체성(숫자·장면·경험 — 뻔한 일반론이면 감점) "
    + "③저장·공유 유발(남에게 보낼 이유가 있는가) ④차별성(누구나 쓸 수 있는 문장이면 감점) ⑤행동 유도(다음 행동이 명확한가). "
    + "후한 점수 금지 — 평범하면 50점대다. "
    + (bench?("\n\n[우리가 벤치마크로 배운 것]\n"+bench.slice(0,900)):"")
    + "\n반드시 JSON만 출력(설명·마크다운 금지): "
    + '{"score":0~100,"why":"점수 이유 한 줄","fix":"점수를 올릴 가장 중요한 수정 지시 한 줄"}';
  const raw = await anthropic(sys, "종류: "+(kindLabel||"콘텐츠")+"\n주제: "+(topic||"")+"\n\n[콘텐츠]\n"+String(text).slice(0,2200), 400);
  const j = safeJson(raw, null);
  if(!j || typeof j.score === "undefined") return null;
  const sc = Math.max(0, Math.min(100, parseInt(j.score,10)||0));
  return { score: sc, why: String(j.why||"").slice(0,140), fix: String(j.fix||"").slice(0,200) };
}
// 제작부가 콘텐츠를 만들고, 검수 통과할 때까지 자동 재작성(최대 3회). 검수 이력 반환.
// 제작부 내부 3역 파이프라인: 작가(대본·카피) → PD(기획·구성·타깃) → 연출(영상 연출·컷·비주얼)
// 각 역할은 이름·성격을 가진 크루가 담당하고, 협업으로 품질을 높인다. 영상성은 3역, 글은 작가→PD.
// 제작부 0단계: 자료수집(리서치). 실검색(Gemini 구글 그라운딩) 우선 → 한도/실패 시 지식기반 폴백.
async function gatherResearch(baseSys, userMsg, topic){
  const subject = String(topic||userMsg||"").slice(0,220);
  const q = "고흥 특산물 SNS 콘텐츠 제작을 위한 자료조사. 주제: "+subject
    + "\n\n다음을 사실 위주로 간결히 정리(한국어, 짧은 불릿):"
    + "\n1) 콘텐츠에 근거로 쓸 핵심 사실·수치·제철/시기·지역 특징"
    + "\n2) 요즘 이 주제로 반응 좋은 콘텐츠 앵글·후킹 3가지"
    + "\n3) 우리(민앤팜·고흥)만의 차별화 포인트"
    + "\n4) 피해야 할 과장·표현(광고법 리스크)"
    + "\n출처가 있으면 끝에 표기.";
  // 실검색 가능하면 우선 사용
  if (searchAllowedNow() && Date.now() >= geminiCooldownUntil){
    try{
      const res = await geminiSearch(q, 1200);
      let brief = String((res && res.text) || "").trim();
      if (res && res.sources && res.sources.length){
        brief += "\n\n[참고 출처]\n" + res.sources.map(s=>"- "+(s.title||s.uri)).join("\n");
      }
      if (brief) return { brief, searched:true };
    }catch(e){ /* 폴백으로 진행 */ }
  }
  // 폴백: 지식 기반 리서치 브리프
  try{
    const sys = baseSys+"\n\n[지금 너의 역할: 🔎 제작부 리서처]\n주제에 대한 '콘텐츠 근거 브리프'를 사실 위주로 간결히 작성한다(한국어, 짧은 불릿). 위 1~4 항목을 채운다.";
    const brief = await genText(sys, q, 900, "gemini");
    if (brief && brief.trim()) return { brief: brief.trim(), searched:false };
  }catch(e){}
  return { brief:"", searched:false };
}

// 작가(정유진) 심화 학습·연구: 블로그 글쓰기 + 영상 대본 전문성을 매일 누적. crewKnowledge.writer 저장.
async function studyWriterCraft(){
  DB.crewKnowledge = DB.crewKnowledge || {};
  const W = CREW.writer;
  const prev = DB.crewKnowledge.writer || {};
  const prior = prev.text || "";
  const rounds = prev.rounds || 0;
  // 3회 학습마다 1회는 실검색으로 최신 블로그 트렌드 보강(검색 여유 있을 때만)
  const searchDue = (rounds % 3 === 0) && searchAllowedNow() && Date.now() >= geminiCooldownUntil;
  let fresh = "";
  if (searchDue){
    try{
      const s = await geminiSearch("2025~2026 블로그 글쓰기·티스토리/네이버 블로그 상위노출(SEO) 최신 요령, 클릭되는 제목 공식, 잘 읽히는 글 구조. 핵심만 짧은 불릿으로.", 900);
      fresh = String((s && s.text) || "").trim();
    }catch(e){}
  }
  const sys = "너는 민앤팜(고흥 특산물)의 제작부 작가 '"+W.name+"'다. 지금은 스스로 실력을 키우는 '심화 학습·연구' 시간이다. "
    + "특히 블로그 글쓰기(티스토리·네이버 블로그)를 깊게 파고, 영상 대본 실력도 함께 다진다. "
    + "블로그: 검색 상위노출(SEO)·키워드 배치·클릭되는 제목 공식, 첫 문단 후킹, 소제목 구조와 가독성(스캔), 경험·전문성·신뢰(E-E-A-T), 사진·정보 배치, 자연스러운 CTA. "
    + "대본: 첫 3초 훅, 기승전결, 감정선. "
    + "아래 형식으로 한국어 16줄 이내 '실전에 바로 쓰는 노하우'만 압축(설명·서론 금지):\n"
    + "블로그 상위노출 공식: (제목·키워드·구조 재사용 템플릿 3~5개)\n"
    + "블로그 몰입 구조: (도입 훅·가독성·신뢰 요소 3~5개)\n"
    + "대본 후킹·전개: (바로 쓰는 훅·구성 3~4개)\n"
    + "피해야 할 것: (약한 글·저품질·과장 1~3개)\n"
    + "이번에 새로 깨달은 점: (기존 대비 나아진 1줄)";
  const ctx = "[기존 나의 노하우]\n"+(prior||"(아직 없음 — 처음부터 정리)")+(fresh?("\n\n[방금 조사한 최신 트렌드]\n"+fresh):"");
  try{
    const out = await genText(sys, ctx, 1200, "gemini");
    if (out && out.trim()){
      DB.crewKnowledge.writer = { text: out.trim(), at: Date.now(), rounds: rounds+1, searched: !!searchDue };
      DB.crewExp = DB.crewExp || {}; DB.crewExp.writer = (DB.crewExp.writer||0)+1;
      DB.exp = DB.exp || {}; DB.exp.creation = (DB.exp.creation||0)+1;
      saveDB();
    }
  }catch(e){ logError("study-writer", e); }
}

// ===== 블로그 글 아카이브 + SEO 내부링크 =====
// 발행/게시된 블로그 글의 제목·URL을 모아, 새 글 쓸 때 관련 과거 글로 내부링크를 걸게 한다(상위노출·체류시간 강화).
function archiveBlogPost(item){
  DB.blogArchive = DB.blogArchive || [];
  const url = String(item.url||"").trim();
  const title = String(item.title||"").trim();
  if (!title) return null;
  // 같은 URL 또는 같은 제목이면 갱신
  const dup = DB.blogArchive.find(x=> (url && x.url===url) || (x.title===title));
  const rec = { title, url, topic:String(item.topic||"").slice(0,120),
    tags: Array.isArray(item.tags)?item.tags.slice(0,8):[], at: Date.now() };
  if (dup) { Object.assign(dup, rec); } else { DB.blogArchive.push(rec); }
  if (DB.blogArchive.length > 300) DB.blogArchive = DB.blogArchive.slice(-300);
  saveDB();
  return rec;
}
function _kwset(s){ return String(s||"").toLowerCase().replace(/[^\w가-힣\s]/g," ").split(/\s+/).filter(w=>w.length>=2); }
function relatedBlogPosts(topic, extra, limit){
  const arr = DB.blogArchive || [];
  if (!arr.length) return [];
  const qk = _kwset(topic+" "+(extra||""));
  if (!qk.length) return [];
  return arr.map(p=>{
    const pk = _kwset(p.title+" "+p.topic+" "+(p.tags||[]).join(" "));
    let score = 0; qk.forEach(w=>{ if(pk.indexOf(w)>=0) score++; });
    return { p, score };
  }).filter(x=>x.score>0).sort((a,b)=>b.score-a.score).slice(0, limit||3).map(x=>x.p);
}
function internalLinkBlock(topic, extra){
  const rel = relatedBlogPosts(topic, extra, 3);
  if (!rel.length) return "";
  const lines = rel.map(p=> "- "+p.title + (p.url? (" ("+p.url+")") : "")).join("\n");
  return "\n\n[내부링크용 과거 글 — 관련 있으면 본문에 자연스럽게 1~2개 링크로 언급(억지 삽입 금지, 맥락 맞을 때만)]\n"+lines
    + "\n작성 규칙: 관련 글을 언급할 땐 '자세한 내용은 「글 제목」 글을 참고하세요' 형태로 자연스럽게. URL이 있으면 마크다운 [제목](URL)로, 없으면 제목만 큰따옴표로 표기.";
}
async function productionPipeline(baseSys, userMsg, topic, engine){
  const _all = String(topic)+" "+String(userMsg)+" "+String(baseSys);
  const isVideo = /(영상|쇼츠|shorts|유튜브|youtube|릴스|reels|틱톡|영화|컷|장면|스토리보드|콘티|나레이션)/i.test(_all);
  const isBlog = !isVideo && /(블로그|blog|티스토리|tistory|네이버\s*블로그|포스팅|포스트|아티클|칼럼|글쓰기|본문|상세페이지)/i.test(_all);
  const W=CREW.writer, P=CREW.pd, D=CREW.director;
  DB.crewExp=DB.crewExp||{}; DB.exp=DB.exp||{};
  // 크루 경험치 + 제작부(creation) 경험치 성장
  const bump=(c)=>{ var k=(c===W?"writer":c===P?"pd":"director"); DB.crewExp[k]=(DB.crewExp[k]||0)+1; DB.exp.creation=(DB.exp.creation||0)+1; };
  // 0) 자료수집(리서치) — 실제 근거·트렌드·차별점 확보. PD(제작부 팀장)의 성과로 반영.
  let research={ brief:"", searched:false };
  try{ research = await gatherResearch(baseSys, userMsg, topic); }catch(e){}
  const briefBlock = research.brief
    ? ("\n\n[제작부 자료조사 브리프"+(research.searched?" · 실검색":"")+"]\n"+research.brief+"\n(위 근거를 반드시 활용하되, 사실만 사용하고 과장·허위표현은 금지)")
    : "";
  if (research.brief){ DB.crewExp.pd=(DB.crewExp.pd||0)+1; DB.exp.creation=(DB.exp.creation||0)+1; }
  // 1) 작가(정유진) — 무엇을 말할지 (브리프 근거로 대본/카피/블로그 글). 축적된 노하우 주입.
  const writerKb = (DB.crewKnowledge && DB.crewKnowledge.writer && DB.crewKnowledge.writer.text)
    ? ("\n\n[작가 "+W.name+"이(가) 축적한 노하우 — 반드시 활용]\n"+DB.crewKnowledge.writer.text) : "";
  const blogCraft = isBlog
    ? ("\n\n[블로그 글쓰기 모드] 이건 블로그 글이다. 다음을 지켜 완성 글을 써라: "
      + "① 검색 유도형 제목(H1, 핵심 키워드 포함) "
      + "② 첫 문단 3줄 안에 후킹(궁금증·공감·이득) "
      + "③ ##/### 소제목으로 스캔 가능한 구조 "
      + "④ 핵심 키워드를 본문에 자연스럽게 분산(억지 반복 금지) "
      + "⑤ 경험·구체 사례·수치로 신뢰 확보(E-E-A-T) "
      + "⑥ 사진 자리는 [사진: 무엇을 찍을지] 로 표시 "
      + "⑦ 마무리에 자연스러운 CTA(구매·문의·구독) "
      + "⑧ 글 끝에 '메타설명(1~2문장)'과 '추천 태그 5개'. "
      + "티스토리·네이버 블로그 상위노출을 노린 완성 본문을 한국어로.")
    : "\n작가로서 핵심 메시지·후킹·기승전결과 대본/카피 초안을 쓴다. 시청자가 끝까지 보게 만드는 스토리·문장에 집중하고, '무엇을 말할지'에 집중한 완성 초안을 한국어로.";
  const linkBlock = isBlog ? internalLinkBlock(topic, userMsg) : "";
  const writerSys=baseSys+briefBlock+writerKb+linkBlock+"\n\n[지금 너의 역할: ✍️ 작가 "+W.name+"]\n성격: "+W.persona+"\n자료조사 브리프의 사실·수치·차별점을 근거로 녹인다."+blogCraft;
  let draft=await genText(writerSys, userMsg, isBlog?2400:1700, engine); bump(W);
  // 2) PD(이서연) — 왜/누구에게: 기획·구성·타깃 최적화 총괄
  const pdBlog = isBlog ? " 이건 블로그 글이므로 제목 클릭률·소제목 구조·키워드 배치·가독성·메타설명·태그가 상위노출에 맞게 최적화됐는지 점검·보완하라." : " 타깃·플랫폼(길이·포맷·썸네일/제목 후킹)에 맞게 구성을 재배치하라.";
  const pdSys=baseSys+briefBlock+"\n\n[지금 너의 역할: 🎬 PD "+P.name+" (제작부 팀장)]\n성격: "+P.persona+"\n작가 "+W.name+"의 초안을 PD로서 강화하라. 브리프의 근거·차별점이 콘텐츠에 확실히 드러나게 하고,"+pdBlog+" 도입 훅·전개·마무리 CTA를 명확히, 군더더기를 덜어 밀도를 높여 완성본만 한국어로.";
  draft=await genText(pdSys, "[작가 "+W.name+" 초안]\n"+draft+"\n\n(PD 관점으로 강화한 완성본만 출력)", isBlog?2400:1800, engine); bump(P);
  // 3) 연출(임채원) — 어떻게 보여줄지 (영상성만)
  if(isVideo){
    const dirSys=baseSys+briefBlock+"\n\n[지금 너의 역할: 🎥 연출 "+D.name+"]\n성격: "+D.persona+"\n기획을 영상으로 구현하게 컷 단위로 확정하라. 각 컷마다 '장면/카메라(앵글·무빙)/자막/나레이션/AI 프롬프트(영어)'를 표 또는 컷 목록으로 정리. 화면 구도·리듬·전환이 드러나게. 스토리보드로 바로 쓸 완성본을 한국어로(AI 프롬프트는 영어).";
    draft=await genText(dirSys, "[PD "+P.name+" 기획본]\n"+draft+"\n\n(연출가가 컷 단위 스토리보드로 확정한 완성본만 출력)", 2000, engine); bump(D);
  }
  saveDB();
  const credit = "\n\n───\n🎬 제작: "+(research.brief?("리서치"+(research.searched?"(실검색)":"")+" · "):"")+"작가 "+W.name+" · PD "+P.name+(isVideo?(" · 연출 "+D.name):"");
  return draft + credit;
}
async function createReviewedContent(baseSys, userMsg, topic, engine){
  const freeGen = (engine==="free" || engine==="nvidia" || engine==="gemini");  // 무료/비-Claude 모델로 생성했는지
  let content=await productionPipeline(baseSys, userMsg, topic, engine);
  // 무료 모델 생성물은 Claude가 '표현·형식'만 다듬어 보완(사실·구조·핵심 메시지는 그대로) — 무거운 초안은 무료, 마무리는 Claude
  if(freeGen){
    try{
      const polishSys = "너는 한국어 카피 에디터다. 아래 초안의 사실·구조·핵심 메시지·정보는 그대로 두고 ①어색한 한국어 표현·번역투·비문을 자연스럽게 다듬고 ②요청된 형식(제목/소제목/불릿/해시태그/메타설명 등)을 정확히 맞추고 ③오탈자를 고쳐라. 내용을 새로 지어내거나 과장·허위 표현을 추가하지 말 것. 다듬은 완성본만 한국어로 출력.";
      const polished = await anthropic(polishSys, "[초안]\n"+content+"\n\n(표현·형식만 다듬은 완성본만 출력)", 2200);
      if(polished && polished.trim()) content = polished.trim();
    }catch(e){ /* 다듬기 실패 시 원문 유지 */ }
  }
  const history=[];
  let passed=false, feedback="";
  // 검수 게이트는 정확성이 중요하므로 항상 Claude로 판정. 무료 생성이면 재작성도 Claude로(표현·형식 교정 품질 확보).
  const reviewEngine = "claude";
  const reviseEngine = freeGen ? "claude" : engine;
  for(let i=0;i<3;i++){
    const r=await reviewContent(content, topic, reviewEngine);
    history.push({ round:i+1, verdict:r.pass?"PASS":"FAIL", feedback:r.feedback });
    if(r.pass){ passed=true; break; }
    feedback=r.feedback;
    // 검수 피드백을 반영해 재작성
    const reviseSys=baseSys+"\n\n[검수부 피드백 — 반드시 반영해 고쳐서 다시 작성]\n"+feedback;
    content=await genText(reviseSys, userMsg+"\n\n(위 피드백을 반영해 개선한 완성본만 출력)", 1600, reviseEngine);
  }
  // 🔥 바이럴 점수 게이트: 규정은 통과했지만 '먹히지 않는' 평범한 결과물을 한 번 더 끌어올린다.
  let viral = null;
  if(viralGateOn()){
    try{
      viral = await scoreViral(content, topic, "");
      if(viral && viral.score < viralMin() && viral.fix){
        const boostSys = baseSys
          + "\n\n[성과 분석 — 이대로면 반응이 안 온다. 아래를 반영해 '먹히게' 다시 써라]\n"
          + "현재 점수: "+viral.score+"점 (기준 "+viralMin()+"점)\n이유: "+viral.why+"\n가장 중요한 수정: "+viral.fix;
        const boosted = await genText(boostSys, userMsg+"\n\n(위 성과 분석을 반영해 더 강하게 고친 완성본만 출력)", 1800, reviseEngine);
        if(boosted && boosted.trim().length>20){
          content = boosted.trim();
          try{ const re = await scoreViral(content, topic, ""); if(re) viral = Object.assign(re, { boosted:true, before:viral.score }); }catch(_){ viral.boosted=true; }
        }
      }
    }catch(e){ /* 채점 실패는 발행을 막지 않는다 */ }
  }
  return { content, passed, reviews:history, viral };
}

// ===== 채널별 카피 병렬(순차) 생성: 인스타/블로그·스마트스토어/카카오톡 각각 최적화 =====
const CHANNEL_SPEC = {
  instagram: { name:"인스타그램", guide:"피드 캡션: 첫 줄 후킹(스크롤 멈추게), 이모지 적절, 2~4문단, 해시태그 8~15개(대·중·소 믹스). 릴스면 짧은 훅+자막용 문장.", limit:"2200자 이내" },
  blog: { name:"블로그·스마트스토어", guide:"정보성·신뢰형 톤. 제목(H1)+소제목 2~3개+본문. 상세페이지면 헤드카피+핵심 셀링포인트 3개+구매 유도. 검색 키워드 자연스럽게 포함.", limit:"1500~2500자" },
  kakao: { name:"카카오톡 채널", guide:"알림톡/친구톡 톤. 짧고 명확, 첫 문장에 핵심 혜택, CTA 버튼 문구 제안. 과한 이모지 지양.", limit:"400자 이내" }
};
// ===== 비주얼 디렉터: 완성된 카피 → 이미지 생성 프롬프트 카드(유료 API 미사용, 프롬프트만) =====
app.post("/api/content/visual", async (req,res)=>{
  try{
    const body=req.body||{};
    const copy=String(body.content||body.source||"").slice(0,1600);
    if(!copy) return res.status(400).json({ error:"카피 내용이 필요해요" });
    const d="creation", a=AGENTS[d];
    const eng=pickEngine(body.engine);
    let sys="너는 SNS 자동화 회사의 비주얼 디렉터다. 아래 완성된 카피의 분위기·핵심 메시지에 딱 맞는 'SNS용 이미지 프롬프트'와 '짧은 영상(Veo/릴스) 프롬프트'를 둘 다 만들어라."+ADDRESS
      +" 실제 이미지·영상은 클라이언트가 Gemini(나노바나나·Veo)·생성 도구에 붙여 만들 것이다. 바로 붙여넣어 쓸 수 있게 구체적으로. "
      +"아래 형식으로만 출력(한국어 설명 + 영문 프롬프트 병기):\n"
      +"【이미지】\n"
      +"컨셉: (한 줄)\n"
      +"한글 프롬프트: (피사체·구도·조명·색감·분위기·스타일 구체적으로)\n"
      +"ENGLISH PROMPT: (same, in English)\n"
      +"네거티브: (피해야 할 요소)\n"
      +"추천 비율: (1:1 / 4:5 / 9:16 중 택1)\n"
      +"【영상(Veo/릴스, 8~15초)】\n"
      +"컨셉: (한 줄)\n"
      +"구간 구성: (3~4컷, 각 컷의 화면·움직임·자막 한 줄씩)\n"
      +"ENGLISH VIDEO PROMPT: (하나의 영상 생성 프롬프트로, 카메라 무빙·분위기·조명 포함)\n"
      +"BGM·톤: (분위기 한 줄)\n"
      +"추천 비율: (9:16 세로 권장)"+profileContext();
    const out=await genText(sys, "[완성 카피]\n"+copy, 1300, eng);
    if(!DB.deptMemory[d]) DB.deptMemory[d]=[];
    DB.deptMemory[d].push({ at:Date.now(), instruction:"[비주얼 디렉터]", note:String(out).slice(0,300) });
    DB.exp=DB.exp||{}; DB.exp[d]=(DB.exp[d]||0)+1;
    saveDB();
    res.json({ ok:true, visualPrompt: out });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

app.post("/api/content/multichannel", async (req,res)=>{
  try{
    const body=req.body||{};
    const chans = Array.isArray(body.channels)&&body.channels.length ? body.channels.filter(c=>CHANNEL_SPEC[c]) : ["instagram","blog","kakao"];
    const d="creation", a=AGENTS[d];
    const kb=knowledgeText(d);
    const rel=await relevantSmart(d, String(body.source||"")+" "+(body.topic||""), 2);
    const eng=pickEngine(body.engine);
    const results=[];
    // 순차 생성(무료 Gemini 429 방지). 채널마다 최적화 + 검수 게이트.
    for(const c of chans){
      const spec=CHANNEL_SPEC[c];
      let sys="너는 SNS 자동화 회사의 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role+ADDRESS+STYLE+personaLine(d);
      if(kb) sys+="\n\n[축적 전문성]\n"+kb;
      if(rel) sys+="\n\n[비슷한 과거 작업 — 톤·형식 재활용]\n"+rel;
      sys+=" 아래 회의 내용을 바탕으로 '"+spec.name+"' 채널에 최적화된 완성형 콘텐츠를 만들어라. 채널 규칙: "+spec.guide+" 분량: "+spec.limit+". 되묻지 말고 완성본만, 한국어로."+profileContext();
      const userMsg="주제: "+(body.topic||"")+" / 출처: "+(body.label||"")+"\n\n[회의 내용]\n"+String(body.source||"").slice(0,1600);
      let out;
      try{
        const gated=await createReviewedContent(sys, userMsg, body.topic||"", eng);
        out=gated.content;
      }catch(e){ out="(생성 실패: "+String(e.message||e).slice(0,80)+")"; }
      results.push({ channel:c, name:spec.name, content:out });
      await new Promise(r=>setTimeout(r, 600)); // 순차 간격(429 방지)
    }
    // 학습 + 승인 대기(요청 시)
    if(!DB.deptMemory[d]) DB.deptMemory[d]=[];
    DB.deptMemory[d].push({ at:Date.now(), instruction:"[채널별 콘텐츠] "+(body.label||""), note:results.map(r=>r.name).join("/")+" "+chans.length+"개 채널 생성" });
    DB.exp=DB.exp||{}; DB.exp[d]=(DB.exp[d]||0)+1;
    if(DB.exp[d]%3===0){ try{ await distillKnowledge(d); }catch(e){} }
    saveDB();
    let approvals=[];
    if(body.needApproval){
      results.forEach(r=>{ const it=queueContentApproval(d, body.topic||"", (body.label||"")+" · "+r.name, r.content, []); approvals.push(it.id); });
    }
    res.json({ ok:true, results, approvalIds:approvals });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

app.post("/api/content/create", async (req,res)=>{
  try{
    const body=req.body||{};
    const d="creation"; const a=AGENTS[d];
    let sys="너는 SNS 자동화 회사의 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role+ADDRESS+STYLE+personaLine(d);
    const kb=knowledgeText(d); if(kb) sys+="\n\n[이 부서가 축적한 전문성(지식 베이스)]\n"+kb;
    const rel=await relevantSmart(d, String(body.source||"")+" "+(body.topic||""), 3);
    if(rel) sys+="\n\n[비슷한 과거 콘텐츠 작업 — 톤·형식 재활용]\n"+rel;
    sys+=" 아래 회의 내용을 바탕으로, 바로 게시 가능한 완성형 SNS 콘텐츠를 직접 만들어라. 되묻지 말고 합리적으로 가정해 완성하라. 플랫폼에 맞는 게시물 카피(또는 영상 스크립트·구성안)와 해시태그까지 포함. 질문·선택 요청 없이 결과물만, 한국어로."+profileContext();
    const userMsg="회의 주제: "+(body.topic||"")+" / 출처: "+(body.label||"")+"\n\n[회의 내용]\n"+String(body.source||"").slice(0,1800);
    const eng=pickEngine(body.engine);
    // 검수 게이트 통과할 때까지 자동 다듬기(감사부 PASS/FAIL, 최대 3회)
    const gated = (body.review!==false) ? await createReviewedContent(sys, userMsg, body.topic||"", eng)
                                        : { content: await genText(sys, userMsg, 1500, eng), passed:true, reviews:[] };
    const out=gated.content;
    if(!DB.deptMemory[d]) DB.deptMemory[d]=[];
    DB.deptMemory[d].push({ at:Date.now(), instruction:"[회의→콘텐츠] "+(body.label||""), note:String(out).slice(0,500) });
    DB.exp=DB.exp||{}; DB.exp[d]=(DB.exp[d]||0)+1;
    if(DB.exp[d]%3===0){ try{ await distillKnowledge(d); }catch(e){} }
    // 검수를 수행한 부서도 경험치(자기 성장)
    DB.exp.advisory=(DB.exp.advisory||0)+1;
    saveDB();
    // Human-in-the-Loop: 요청 시 승인 대기로 올림(승인/반려/직접수정)
    let approval=null;
    if(body.needApproval){ approval=queueContentApproval(d, body.topic||"", body.label||"", out, gated.reviews); }
    res.json({ ok:true, content: out, dept:d, passed:gated.passed, reviews:gated.reviews, approvalId:(approval?approval.id:null) });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 동영상 기획 — 제작부(Claude)가 기획안 → Gemini가 Veo 3.1 프롬프트로 변환(협력). 클라이언트가 Gemini 앱(Ultra)에 붙여 샘플 생성.
// ===== 웹툰 제작 =====
// 캐릭터 일관성: 한 번 등록한 캐릭터 시트를 모든 컷 프롬프트에 자동 삽입 → 매 컷 같은 얼굴 유지.
function buildCharacterBlock(names){
  const chars = DB.characters || [];
  if (!chars.length) return "";
  let list = chars;
  if (Array.isArray(names) && names.length){
    const set = names.map(n=>String(n).trim());
    list = chars.filter(c=> set.some(n=> c.name && c.name.indexOf(n)>=0));
    if (!list.length) list = chars; // 이름 매칭 실패 시 전체 제공
  }
  const rows = list.slice(0,6).map(c=>{
    const en = c.promptEn ? (" / EN: "+c.promptEn) : "";
    return "· "+c.name+": "+(c.look||"")+(c.outfit?(" · 의상: "+c.outfit):"")+(c.tone?(" · 분위기: "+c.tone):"")+en;
  }).join("\n");
  return "\n\n[캐릭터 시트 — 모든 컷에서 아래 외모·의상을 동일하게 유지(일관성 필수). 작화 프롬프트에 반드시 이 설정을 녹여라]\n"+rows;
}
// 웹툰 콘티 생성: 형식(4컷 단편 / 세로 스크롤 / 장편) 분기, 컷별 구도·대사·작화 프롬프트
async function webtoonPipeline(body){
  const topic = String(body.topic||"").slice(0,200);
  const source = String(body.source||"").slice(0,1500);
  // format: 4cut | scroll | long
  let format = body.format;
  if (format!=="4cut" && format!=="long") format = "scroll";
  let cuts;
  if (format==="4cut") cuts = 4;
  else if (format==="long") cuts = Math.max(16, Math.min(40, +body.cuts || 24));
  else cuts = Math.max(6, Math.min(15, +body.cuts || 8));
  const engine = pickEngine(body.engine);
  const d="creation"; const a=AGENTS[d];
  const W=CREW.writer, P=CREW.pd, D=CREW.director;
  DB.crewExp=DB.crewExp||{}; DB.exp=DB.exp||{};
  const bump=(k)=>{ DB.crewExp[k]=(DB.crewExp[k]||0)+1; DB.exp.creation=(DB.exp.creation||0)+1; };
  const charBlock = buildCharacterBlock(body.characters);
  const kb = knowledgeText(d); const kbBlock = kb ? ("\n\n[제작부 축적 전문성]\n"+kb) : "";
  const baseSys = "너는 민앤팜(고흥 특산물·지역 홍보)의 제작부 웹툰 팀이다."+ADDRESS+STYLE+kbBlock+charBlock+profileContext();

  // 1) PD 이서연 — 에피소드 구성·후킹
  const fmtDesc = format==="4cut"
    ? "형식: 4컷 단편(기-승-전-결). 마지막 컷에 펀치라인/반전/CTA. 홍보·정보 전달에 최적."
    : format==="long"
    ? "형식: 장편 웹툰("+cuts+"컷). 기승전결이 뚜렷한 하나의 완결 에피소드. 도입(인물·상황 소개)→전개(사건·갈등)→절정→마무리 구조로, 중간중간 몰입 훅을 넣어라."
    : "형식: 세로 스크롤 웹툰("+cuts+"컷 내외). 위→아래로 자연스럽게 읽히는 흐름, 마지막에 다음 화 궁금증 또는 마무리 임팩트.";
  const pdSys = baseSys+"\n\n[역할: 🎬 PD "+P.name+"]\n"+P.persona+"\n"+fmtDesc+" 아래 주제로 웹툰 한 편의 '구성안'을 짜라. 로그라인 1줄 / 등장인물(있으면 캐릭터 시트 활용) / "+(format==="long"?"막(도입·전개·절정·마무리)별 흐름과 ":"")+"전체 흐름을 "+cuts+"컷 기준으로 컷별 한 줄 요약. 한국어로.";
  const plan = await genText(pdSys, "주제: "+topic+"\n참고: "+source, format==="long"?1800:1200, engine); bump("pd");

  // 2) 작가 정유진 — 컷별 대사·나레이션
  const wSys = baseSys+"\n\n[역할: ✍️ 작가 "+W.name+"]\n"+W.persona+" 아래 PD 구성안을 바탕으로, 각 컷의 '대사'와 '나레이션'을 써라. 대사는 캐릭터 성격이 드러나게 짧고 생생하게. 컷마다 [컷N] 대사/나레이션 형식으로. 한국어로.";
  const script = await genText(wSys, "[PD 구성안]\n"+plan, format==="long"?2200:1400, engine); bump("writer");

  // 3) 연출 임채원 — 콘티(칸) + 작화 프롬프트
  const cutFormat = "각 컷마다 반드시 아래 형식으로 출력(캐릭터 시트가 있으면 외모·의상을 프롬프트에 그대로 반영해 일관성 유지):\n"
    + "[컷 N]\n"
    + "장면: (무슨 상황)\n"
    + "구도: (클로즈업/롱샷/부감/정면 등)\n"
    + "인물·표정·동작: (누가 어떤 표정·자세)\n"
    + "대사/나레이션: (말풍선 내용, 위치 힌트)\n"
    + "🎨 작화 프롬프트(한글): (피사체·배경·구도·조명·색감·웹툰체 스타일 구체적으로)\n"
    + "🎨 ART PROMPT(EN): (same in English, include 'webtoon style, vertical panel, consistent character')\n"
    + "─────\n"
    + "AI 이미지 생성기(나노바나나/Gemini)에 바로 붙여넣어 그릴 수 있게 구체적으로. 한국어 설명 + 영문 프롬프트 병기.";
  const dirBase = baseSys+"\n\n[역할: 🎥 연출 "+D.name+"]\n"+D.persona;
  let storyboard;
  if (format==="long" && cuts>10){
    // 장편: 컷을 여러 배치로 나눠 순차 생성(품질 유지). 배치당 최대 8컷.
    const batch = 8; const parts=[];
    for (let start=1; start<=cuts; start+=batch){
      const end = Math.min(cuts, start+batch-1);
      const dirSys = dirBase+" 아래 구성안·대사를 '세로 스크롤 웹툰 콘티'로 확정하라. 지금은 전체 "+cuts+"컷 중 [컷 "+start+"]부터 [컷 "+end+"]까지"+(parts.length?" 이어서":"")+" 그 구간만 출력하라. "+cutFormat;
      const priorNote = parts.length ? ("\n\n[이미 그린 앞부분 — 흐름·캐릭터 이어가기]\n"+parts.join("\n").slice(-1200)) : "";
      const seg = await genText(dirSys, "[PD 구성안]\n"+plan+"\n\n[작가 대사]\n"+script+priorNote, 1900, engine);
      parts.push(String(seg).trim());
    }
    storyboard = parts.join("\n\n");
    bump("director");
  } else {
    const dirSys = dirBase+" 아래 구성안·대사를 '세로 스크롤 웹툰 콘티'로 확정하라. "+(format==="4cut"?"정확히 4컷.":cuts+"컷 내외.")+" "+cutFormat;
    storyboard = await genText(dirSys, "[PD 구성안]\n"+plan+"\n\n[작가 대사]\n"+script, format==="4cut"?1800:2800, engine); bump("director");
  }

  // 학습·경험치
  DB.deptMemory=DB.deptMemory||{}; DB.deptMemory[d]=DB.deptMemory[d]||[];
  DB.deptMemory[d].push({ at:Date.now(), instruction:"[웹툰 "+(format==="4cut"?"4컷":format==="long"?"장편":"연재")+"]", note:(topic+" — "+String(plan).slice(0,200)) });
  DB.exp[d]=(DB.exp[d]||0)+1; if(DB.exp[d]%3===0){ try{ await distillKnowledge(d); }catch(e){} }
  saveDB();
  const fmtKr = format==="4cut"?"4컷 단편":format==="long"?(cuts+"컷 장편"):(cuts+"컷 연재");
  const credit = "\n\n───\n🎨 웹툰 제작: PD "+P.name+" · 작가 "+W.name+" · 연출 "+D.name+" · "+fmtKr;
  return { ok:true, format, cuts, plan, script, storyboard: storyboard+credit,
    by:{ pd:P.name, writer:W.name, director:D.name } };
}
app.post("/api/webtoon/create", async (req,res)=>{
  try { res.json(await webtoonPipeline(req.body||{})); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// ===== HTML 상품페이지 생성 (커머스·그로스 부서의 학습된 품질 공식을 실제 페이지로) =====
// ===== 디자인 목표 기반 루프: 객관적 린터(코드 검증) + 검수관(Claude 판단) =====
// 자체 검증 수단 강제: AI 주관 대신 코드로 확인 가능한 것부터 잡는다(분포 수렴·팔레트 미사용·불완전 문서).
function lintDesignHtml(html, direction){
  const h=String(html||""); const low=h.toLowerCase(); const issues=[];
  if(!/<!doctype html/i.test(h) || !/<\/html>/i.test(h)) issues.push("완전한 단일 HTML 문서가 아님(<!DOCTYPE>~</html> 확인)");
  if(direction && Array.isArray(direction.palette) && direction.palette.length){
    const used = direction.palette.filter(p=>p&&p.hex && low.indexOf(String(p.hex).toLowerCase())>=0).length;
    if(used===0) issues.push("확정한 팔레트 색상(hex)이 실제 CSS에 하나도 안 쓰임 — 디렉션 색을 실제로 적용할 것");
  }
  const usesInter = /font-family\s*:[^;{}]*inter/i.test(h);
  const purpleGrad = /linear-gradient[^;{}]*(#?(6d28d9|7c3aed|8b5cf6|818cf8|6366f1|4f46e5|4338ca)|indigo|rebeccapurple|purple)/i.test(h);
  if(usesInter && purpleGrad) issues.push("AI 클리셰 의심: Inter 폰트 + 보라·인디고 그라디언트 동시 사용(분포 수렴)");
  return { issues };
}
async function reviewDesignHtml(html, direction){
  const lint = lintDesignHtml(html, direction);
  const dir = direction ? ("미학:"+(direction.aesthetic||"")+" / 레퍼런스:"+(direction.reference||"")+" / 팔레트:"+((direction.palette||[]).map(p=>p&&p.hex).filter(Boolean).join(","))+" / 타이포:"+((direction.typography&&direction.typography.heading)||"")+" / 레이아웃:"+(direction.layout||"")+" / 시그니처:"+(direction.signature||"")) : "(디렉션 없음)";
  const sys = "너는 웹디자인 검수관(Evaluator)이다. 아래 HTML이 목표 기준을 충족하는지 냉정히 평가하라. 기준: ①비주얼 디렉션(미학·레퍼런스·팔레트 hex 실제 사용·레이아웃·시그니처)을 실제로 구현했는가 ②'AI가 만든 흔한 웹' 클리셰(보라→인디고 그라디언트, Inter류 기본 폰트, '가운데 정렬 큰 제목+아이콘 카드 3개')를 피했는가 ③완전한 단일 HTML(문서 완결·모바일 반응형)인가 ④과장·허위표현이 없는가. "
    + (lint.issues.length ? ("자동 점검에서 발견된 문제(반드시 FAIL 처리하고 반영): "+lint.issues.join(" / ")+". ") : "")
    + "반드시 한 줄만 출력(설명 금지):\nVERDICT: PASS 또는 FAIL | (FAIL이면 무엇을 어떻게 고칠지 구체적으로 한두 문장)";
  let pass=true, feedback="";
  try{
    const out = await anthropic(sys, "[비주얼 디렉션]\n"+dir+"\n\n[검수 대상 HTML(일부)]\n"+String(html).slice(0,12000), 500); // 판단은 큰 모델(Claude)
    const m = String(out).match(/VERDICT:\s*(PASS|FAIL)\s*\|?\s*(.*)/i);
    if(m){ pass=/PASS/i.test(m[1]); feedback=String(m[2]||"").trim(); }
  }catch(e){ /* 검수 실패 시 통과로 두어 막지 않음(단, 린트 문제는 아래서 강제) */ }
  if(lint.issues.length){ pass=false; feedback=(feedback?feedback+" ":"")+"[자동점검] "+lint.issues.join(" / "); }
  return { pass, feedback, lint:lint.issues };
}
async function productPagePipeline(body, setStep){
  const _step = (typeof setStep==="function") ? setStep : function(){};
  let j_visualDirection = null;
  // v222: kind="home"이면 브랜드 홈페이지, dept로 담당 부서 지정 가능(홈페이지=웹 디자이너 강민서)
  const kind = (String(body.kind||"product")==="home") ? "home" : "product";
  const isHome = (kind==="home");
  const KIND_NM = isHome ? "홈페이지" : "상품페이지";
  _step((isHome?"사이트 정보":"상품 정보")+"·품질공식 준비 중");
  const d=(body.dept && AGENTS[body.dept]) ? String(body.dept) : (isHome ? "analytics" : "monetization");
  const a=AGENTS[d];
  const product = String(body.product||"").slice(0,120);
  const info = String(body.info||"").slice(0,2000);
  const photos = Array.isArray(body.photos) ? body.photos.slice(0,10) : []; // [{url, desc}] 실제 사진(있으면)
  const price = String(body.price||"").slice(0,60);
  const brand = String(body.brand||"민앤팜").slice(0,40);
  if(!product) throw new Error(isHome?"홈페이지 주제(브랜드·사이트명)가 필요합니다":"상품명이 필요합니다");
  // 학습된 품질 공식(벤치마크 학습으로 축적된 것)을 그대로 주입
  const formula = knowledgeText(d);
  const formulaBlock = formula ? ("\n\n[이 부서가 축적한 "+KIND_NM+" 품질 공식 — 반드시 이 기준을 적용해 만들어라]\n"+formula) : "";
  // 실제 사진 자리 안내
  let photoBlock = "";
  if (photos.length){
    photoBlock = "\n\n[제공된 실제 사진 — 아래 URL을 <img src>로 그대로 사용하라]\n"
      + photos.map((p,i)=>"사진"+(i+1)+": "+(p.url||"")+(p.desc?(" ("+p.desc+")"):"")).join("\n");
  }
  photoBlock += "\n\n[사진이 없는 자리는 <div class=\"img-ph\">[AI이미지: 구체적 묘사]</div> 형태의 플레이스홀더로 남겨, 나중에 이미지를 넣을 수 있게 하라.]";
  const baseHtml = String(body.baseHtml||"");
  const feedback = String(body.feedback||"").slice(0,1500);
  const isRevise = !!(baseHtml && feedback);
  let sys, user;
  if (isRevise){
    sys = "너는 민앤팜(고흥 특산물)의 '"+a.no+" "+a.kr+"' 부서 수석 웹디자이너다. "+ADDRESS
      + " 아래 '기존 "+KIND_NM+" HTML'을 클라이언트 요청대로 수정·보완해, 수정된 '완전한 단일 HTML 문서'를 다시 출력하라. "
      + "반드시 지킬 것: (1) <!DOCTYPE html>부터 </html>까지 완전한 문서, CSS는 <style>에 인라인. "
      + "(2) 요청한 부분만 정확히 반영하고, 나머지 디자인·구조·톤은 그대로 유지하라(불필요하게 갈아엎지 말 것). "
      + "(3) 기존의 고급스러운 여백·타이포그래피·반응형을 유지하라. (4) 과장광고 금지. "
      + "설명·머리말 없이 HTML 코드만 출력하라(```html 같은 마크다운 표시도 금지)."
      + formulaBlock + designSystemBlock() + profileContext();
    user = "[기존 "+KIND_NM+" HTML]\n"+baseHtml+"\n\n[클라이언트 수정·추가 요청]\n"+feedback;
  } else {
    // 🎨 '비주얼 먼저' 원칙: HTML을 짜기 전에 색 팔레트·타이포·핵심 이미지 컨셉·무드를 먼저 확정하고, 그에 정확히 맞춰 페이지를 설계하게 한다.
    _step("비주얼 디렉션 잡는 중 (색·이미지·무드)");
    let visualBlock = "";
    try{
      const vSys = "너는 민앤팜(전남 고흥 특산물)의 아트디렉터다."+brandBlock()
        + " "+KIND_NM+"를 코딩하기 전에 '비주얼을 먼저' 확정한다(이 단계가 디자인 품질을 좌우한다). "
        + "AI가 방향 없이 만들면 가장 흔한 평균값('분포 수렴')으로 도망쳐 싸구려 티가 난다. '깔끔하게·모던하게' 같은 무의미한 말 대신 방향을 언어로 못 박아라: "
        + "①이 상품에 맞는 구체적 미학 스타일 하나(예: 에디토리얼/럭셔리/미니멀 아티즈널/레트로/브루탈리즘 등). "
        + "②이 상품 결에 맞는 실제 브랜드 레퍼런스 느낌(프리미엄 식품이면 오설록·마켓컬리·에르메스 푸드 같은, AI가 잘 아는 독창적 브랜드 — 아무 브랜드나 말고 결이 맞는 것). "
        + "③색·폰트·레이아웃·시그니처를 명확히. "
        + "반드시 JSON만(설명 금지): {\"aesthetic\":\"구체적 미학 스타일(한 단어~한 줄)\",\"reference\":\"레퍼런스 브랜드/느낌 + 왜 이 결이 맞는지 짧게\",\"mood\":\"전체 무드 한 줄\",\"palette\":[{\"name\":\"\",\"hex\":\"#______\",\"use\":\"용도\"}],\"typography\":{\"heading\":\"제목 폰트 느낌(구체적으로, 흔한 Inter류 금지)\",\"body\":\"본문 느낌\"},\"layout\":\"화면 배치를 한두 줄로 — 흔한 '가운데 정렬 히어로+아이콘 카드 3개'는 피하고 이 상품만의 구조로\",\"signature\":\"이 페이지만의 강렬한 한 방 요소 하나\",\"imagePlan\":[{\"slot\":\"히어로/상세1 등\",\"desc\":\"구도·색·분위기까지 구체적 이미지 묘사\"}]}. palette 4~6개, imagePlan 3~5개.";
      const vRaw = await anthropic(vSys + designSystemBlock()
        + (hasDesignSystem()? " ★브랜드 디자인 시스템이 있으므로 palette·typography는 위 도면의 색·폰트를 '그대로' JSON에 옮겨 담아라(새 색 창작 금지). aesthetic·layout·signature만 이번 주제에 맞게 새로 정하라." : ""),
        "브랜드: "+brand+"\n상품: "+product+(price?("\n가격: "+price):"")+"\n정보: "+info.slice(0,900)+(formula?("\n\n[품질 공식 참고]\n"+formula.slice(0,600)):""), 900);
      const vj = safeJson(vRaw, null);
      if(vj){
        const pal = (vj.palette||[]).map(p=>"· "+(p.name||"")+" "+(p.hex||"")+(p.use?(" ("+p.use+")"):"")).join("\n");
        const imgs = (vj.imagePlan||[]).map((p,i)=>"· ["+(p.slot||("이미지"+(i+1)))+"] "+(p.desc||"")).join("\n");
        visualBlock = "\n\n[먼저 확정한 비주얼 디렉션 — 이 미학·레퍼런스·색·타이포·레이아웃·시그니처에 '정확히' 맞춰 페이지를 설계하라. 색상은 아래 hex를 실제 CSS에 사용하고, 이미지 플레이스홀더에는 아래 '핵심 이미지' 묘사를 그대로 넣어라]\n"
          + (vj.aesthetic?("미학 스타일: "+vj.aesthetic+"\n"):"")
          + (vj.reference?("레퍼런스: "+vj.reference+"\n"):"")
          + "무드: "+(vj.mood||"")+"\n"
          + (pal?("팔레트:\n"+pal+"\n"):"")
          + (vj.typography?("타이포: 제목 "+(vj.typography.heading||"")+" / 본문 "+(vj.typography.body||"")+"\n"):"")
          + (vj.layout?("레이아웃: "+vj.layout+"\n"):"")
          + (vj.signature?("시그니처(한 방 요소): "+vj.signature+"\n"):"")
          + (imgs?("핵심 이미지:\n"+imgs):"");
        j_visualDirection = vj;
      }
    }catch(e){ /* 비주얼 디렉션 실패 시 기존 방식으로 진행 */ }
    sys = "너는 민앤팜(고흥 특산물)의 '"+a.no+" "+a.kr+"' 부서 수석 웹디자이너다. "+ADDRESS
      + (isHome
          ? " 아래 브랜드로, 위 비주얼 디렉션의 미학 스타일·레퍼런스를 살린 전문가 수준의 '실제로 바로 열리는 완성형 HTML 브랜드 홈페이지(공식 홈)' 하나를 만들어라. "
          : " 아래 상품으로, 위 비주얼 디렉션의 미학 스타일·레퍼런스를 살린 전문가 수준의 '실제로 바로 열리는 완성형 HTML 상품페이지' 하나를 만들어라. ")
      + "반드시 지킬 것: (1) 완전한 단일 HTML 문서(<!DOCTYPE html>부터 </html>까지, CSS는 <style>에 인라인). " + "★필수: 콘텐츠는 자바스크립트 없이도 즉시 보여야 한다. opacity:0 / visibility:hidden 상태로 시작해서 JS(IntersectionObserver 등)로 나타나게 하는 스크롤 리빌은 금지 — JS가 한 줄이라도 실패하면 페이지 전체가 백지가 된다. 애니메이션은 CSS @keyframes로만, 끝난 상태가 아니라 처음부터 보이는 상태에서 시작하라(animation-fill-mode 의존 금지). localStorage·sessionStorage·쿠키는 절대 쓰지 마라(샌드박스에서 예외를 던져 스크립트가 죽는다). "
      + (isHome
          ? "(2) 홈페이지 구성: 상단 헤더+내비게이션(홈·브랜드·상품·후기·문의, 앵커 링크로 동작), 히어로(브랜드 슬로건+대표 비주얼), 브랜드 스토리(우리는 누구인가·왜 이 땅에서), 대표 상품 카드 3~6개(이름·한 줄 설명·가격 자리), 산지/농장 소개, 고객 후기, 문의·주문 안내(연락처·오시는 길)+푸터. 단일 페이지 스크롤형으로. "
          : "(2) 히어로 섹션(풀블리드 대형 비주얼 + 브랜드 스토리 한 문장), 상품 소개, 스토리텔링, 상세 특징, 구매 유도(CTA) 섹션 포함. ")
      + "(3) 고급스러운 여백·타이포그래피·색감, 스크롤 시 자연스러운 리듬. 모바일 반응형. "
      + "(4) 과장광고·허위표현 금지, 실제 판매에 쓸 수 있는 신뢰감 있는 카피. "
      + "(5) 절대 'AI가 만든 흔한 웹' 티를 내지 마라 — 보라→인디고 그라디언트, Inter류 기본 폰트, '가운데 정렬 큰 제목 + 아이콘 박힌 카드 3개' 같은 평균적 클리셰(분포 수렴)를 피하고, 위 비주얼 디렉션의 미학·레퍼런스·색(hex)·폰트·레이아웃을 실제 화면에 그대로 구현하라. 특히 '시그니처(한 방 요소)'는 눈에 띄게 살려 이 페이지만의 인상을 만들어라. "
      + "설명·머리말 없이 HTML 코드만 출력하라(```html 같은 마크다운 표시도 금지)."
      + formulaBlock + photoBlock + visualBlock + designSystemBlock() + profileContext();
    user = "브랜드: "+brand+"\n"+(isHome?"홈페이지 주제":"상품")+": "+product+(price?("\n가격: "+price):"")+"\n"+(isHome?"참고 정보":"상품 정보")+":\n"+info;
  }
  _step((body.baseHtml?("수정 사항 반영해 "+KIND_NM+" 재작성 중"):("AI가 "+KIND_NM+" 작성 중"))+" (1~3분)");
  let html = await anthropic(sys, user, 8000); // 페이지는 길어야 하므로 큰 토큰
  _step("마무리·정리 중");
  // 혹시 코드펜스가 붙어 나오면 제거
  html = String(html).replace(/^```html\s*/i,"").replace(/^```\s*/,"").replace(/```\s*$/,"").trim();
  // 🔁 목표 기반 루프: 검수관(Claude)이 비주얼 디렉션·기준 충족까지 최대 N회 수정. 무한 토큰 방지 위해 상한.
  let designReviews = [];
  const maxLoops = Math.max(0, Math.min(3, (DB.state && DB.state.designReviewLoops!=null) ? (+DB.state.designReviewLoops||0) : 1));
  if(!isRevise && maxLoops>0){
    for(let i=0;i<maxLoops;i++){
      _step("검수관이 디자인 점검 중 ("+(i+1)+"/"+maxLoops+")");
      const rv = await reviewDesignHtml(html, j_visualDirection);
      designReviews.push({ round:i+1, pass:rv.pass, feedback:rv.feedback });
      if(rv.pass) break;
      _step("검수 피드백 반영해 디자인 수정 중 ("+(i+1)+"/"+maxLoops+")");
      const fixSys = sys + "\n\n[검수관 피드백 — 아래를 반드시 반영해 고쳐서 '완전한 단일 HTML'을 다시 출력하라. 잘된 부분은 유지]\n"+rv.feedback;
      try{
        let fixed = await anthropic(fixSys, "[현재 HTML]\n"+html.slice(0,14000)+"\n\n(피드백 반영한 완성 HTML만 출력)", 8000);
        fixed = String(fixed).replace(/^```html\s*/i,"").replace(/^```\s*/,"").replace(/```\s*$/,"").trim();
        if(fixed && /<\/html>/i.test(fixed)) html = fixed;
      }catch(e){ break; }
    }
  }
  // 학습·경험치
  DB.deptMemory=DB.deptMemory||{}; DB.deptMemory[d]=DB.deptMemory[d]||[];
  DB.deptMemory[d].push({ at:Date.now(), instruction:(isRevise?("["+KIND_NM+" 수정]"):("["+KIND_NM+" 제작]")), note:product+" — "+(isRevise?feedback.slice(0,60):(price||"")) });
  if(DB.deptMemory[d].length>40) DB.deptMemory[d]=DB.deptMemory[d].slice(-40);
  DB.exp=DB.exp||{}; DB.exp[d]=(DB.exp[d]||0)+1; if(DB.exp[d]%3===0){ try{ await distillKnowledge(d); }catch(e){} }
  if (formula) logKnowledgeApplied(d, (isRevise?(KIND_NM+" 수정"):(KIND_NM+" 제작")), product);
  saveDB();
  const _chk = checkHtmlOutput(html);
  if(!_chk.ok){
    _step("⚠️ 결과물 이상 감지 — 다시 만드는 중 ("+_chk.problems.join(", ")+")");
    try{
      const retry = await anthropic(sys + "\n\n★이전 시도가 실패했다: "+_chk.problems.join(", ")+". 반드시 <!DOCTYPE html>로 시작해 </html>로 끝나는 완전한 문서를, 본문 텍스트를 충분히 담아 출력하라.", user, 8000);
      const h2 = String(retry).replace(/^```html\s*/i,"").replace(/^```\s*/,"").replace(/```\s*$/,"").trim();
      if(checkHtmlOutput(h2).ok) html = h2;
    }catch(_){}
  }
  html = hardenHtml(html);
  return { ok:true, html, product, kind, dept:d, appliedFormula: !!formula, visualDirection:j_visualDirection, reviews:designReviews, check:checkHtmlOutput(html), by:a.kr };
}
// v227: 보고서 검수관 — 팔 수 있는 보고서인지 '근거'로 심사한다.
// 디자인 검수(색·레이아웃)와 완전히 다른 축이다. 지어낸 숫자 하나면 보고서 가치가 0이 된다.
async function reviewReportHtml(html, topic, research){
  const h = String(html||"");
  // 자동 점검(결정적) — AI 판단 이전에 기계적으로 잡을 수 있는 것
  const issues = [];
  const bodyText = h.replace(/<script[\s\S]*?<\/script>/gi,"").replace(/<style[\s\S]*?<\/style>/gi,"").replace(/<[^>]+>/g," ");
  if(!/<table/i.test(h)) issues.push("키워드 지도가 실제 <table>이 아님");
  if(!/@media\s+print/i.test(h)) issues.push("@media print 없음(인쇄·PDF 불가)");
  const secs = ["요약","키워드","경쟁","기회","30일","근거"];
  const missing = secs.filter(k=> bodyText.indexOf(k)<0);
  if(missing.length) issues.push("빠진 섹션: "+missing.join(", "));
  if(bodyText.replace(/\s+/g," ").trim().length < 600) issues.push("내용이 너무 얇음");

  const sys = "너는 민앤팜의 '글3 이야기팀·블로그 작가' 정유진이자, 보고서 감사관이다. "
    + "이 리서치 보고서는 쇼핑몰 사장님에게 '팔 물건'이다. 냉정하게 심사하라.\n"
    + "★심사 기준 (하나라도 걸리면 FAIL):\n"
    + "① 조작 여부 — [조사자료]에 없는 검색량·순위·점유율·퍼센트 숫자를 지어냈는가? (가장 중요. 하나라도 있으면 무조건 FAIL)\n"
    + "② 일반론 — '고객 중심으로 소통하세요', '꾸준히 올리세요' 같은 누구나 할 말이 섞였는가?\n"
    + "③ 빈틈(기회) — ④번 섹션이 '남들이 안 하는 구체적인 것'인가, 아니면 뻔한 말인가?\n"
    + "④ 실행 가능성 — 30일 플랜이 '무엇을·어디에·몇 개'까지 적혔는가?\n"
    + "⑤ 정직성 — 모르는 건 '자료상 확인 불가'라고 적었는가, 아니면 아는 척했는가?\n"
    + (issues.length ? ("자동 점검에서 발견된 문제(반드시 FAIL 처리하고 반영): "+issues.join(" / ")+"\n") : "")
    + "반드시 한 줄만 출력(설명 금지):\nVERDICT: PASS 또는 FAIL | (FAIL이면 무엇을 어떻게 고칠지 구체적으로)";
  let pass=true, feedback="";
  try{
    const out = await anthropic(sys,
      "주제: "+(topic||"")
      + "\n\n[조사자료 — 이 안에 있는 사실만 근거로 인정한다]\n"+String(research||"(없음)").slice(0,6000)
      + "\n\n[심사 대상 보고서 HTML]\n"+h.slice(0,12000), 600);
    const m = String(out).match(/VERDICT:\s*(PASS|FAIL)\s*\|?\s*(.*)/i);
    if(m){ pass=/PASS/i.test(m[1]); feedback=String(m[2]||"").trim(); }
  }catch(e){ /* 검수 실패가 발행을 막지는 않는다 */ }
  if(issues.length){ pass=false; feedback=(feedback?feedback+" ":"")+"[자동점검] "+issues.join(" / "); }
  return { pass, feedback, lint:issues };
}
// ===== 📊 v225: 리서치 보고서 (경쟁사·키워드·트렌드 → 사장님용 1장) =====
// 팔 수 있는 산출물. 근거 없는 일반론이 나오면 가치가 0이므로 반드시 '실제 검색'을 먼저 돌리고,
// 그 자료에 없는 숫자·순위는 지어내지 못하게 못 박는다.
async function reportPipeline(body, setStep){
  const _step = (typeof setStep==="function") ? setStep : function(){};
  const d = "ops"; const a = AGENTS[d];                    // 총괄 비서 오세라 — 취합·판단이 본업
  const topic = String(body.topic||"").slice(0,120);
  const info  = String(body.info||"").slice(0,1500);
  const memo  = String(body.memo||"").slice(0,800);
  const url   = String(body.url||"").trim();
  if(!topic) throw new Error("분석할 주제(상품·카테고리)가 필요합니다");

  const baseHtml = String(body.baseHtml||"");
  const feedback = String(body.feedback||"").slice(0,1500);
  const isRevise = !!(baseHtml && feedback);

  let research = "", sources = [];
  if(!isRevise){
    // ── 실제 조사 3축 (무료 Gemini 그라운딩 우선 → 실패 시 폴백) ──
    const qs = [
      { k:"키워드",   q: topic+" 검색 키워드 연관검색어 소비자가 실제로 검색하는 말 2026" },
      { k:"경쟁사",   q: topic+" 잘 파는 판매자 상위노출 상세페이지 후기 마케팅 사례" },
      { k:"트렌드",   q: topic+" 시장 트렌드 시즌 수요 소비자 반응 2026" }
    ];
    for(let i=0;i<qs.length;i++){
      _step("리서치 "+(i+1)+"/3 · "+qs[i].k+" 조사 중");
      try{
        const r = await researchWeb(qs[i].q, 1400);
        const t = String(r.text||"").trim();
        if(t) research += "\n\n===== ["+qs[i].k+"] =====\n"+t;
        if(Array.isArray(r.sources)) sources = sources.concat(r.sources);
      }catch(e){ /* 한 축이 실패해도 나머지로 진행 */ }
    }
    if(url){ _step("참고 URL 학습 중"); try{ await learnFromWebUrls(d, [url]); }catch(_){} }
  }

  const kbS = knowledgeText("strategy");   // 벤치마크로 축적한 후킹·마케팅 지식
  const kbC = knowledgeText("scout");      // 트렌드·소재 가공 지식
  const srcList = sources.slice(0,10).map((x,i)=>"["+(i+1)+"] "+(x.title||x.url||"")+" — "+(x.url||"")).join("\n");

  let sys, user;
  if(isRevise){
    sys = "너는 민앤팜의 '"+a.no+" "+a.kr+"' 오세라다."+ADDRESS
      + " 아래 '기존 리서치 보고서 HTML'을 요청대로 수정해, 수정된 '완전한 단일 HTML 문서'를 다시 출력하라. "
      + "요청한 부분만 반영하고 나머지 구조·톤·디자인은 유지하라. 자료에 없는 숫자·순위를 새로 지어내지 마라. "
      + "설명·머리말 없이 HTML만 출력(```html 금지)."
      + designSystemBlock();
    user = "[기존 보고서 HTML]\n"+baseHtml+"\n\n[수정 요청]\n"+feedback;
  } else {
    _step("보고서 작성 중 (1~3분)");
    sys = "너는 민앤팜(전남 고흥 특산물)의 '"+a.no+" "+a.kr+"' 오세라다."+ADDRESS
      + " 쇼핑몰 사장님에게 납품할 '리서치 보고서' 한 장을 만든다. 사장님은 바빠서 3분 안에 읽고 바로 움직여야 한다. "
      + "\n\n★ 절대 규칙: (1) 아래 [조사자료]에 실제로 나온 내용만 근거로 써라. "
      + "자료에 없는 검색량·순위·점유율 숫자를 지어내면 보고서 가치가 0이 된다. "
      + "확실치 않으면 숫자 대신 '자료상 확인 불가 — 직접 확인 필요'라고 정직하게 적어라. "
      + "(2) '고객 중심으로 소통하세요' 같은 일반론 금지 — 이 주제에서만 통하는 구체적인 것만. "
      + "(3) 모든 판단에 근거를 붙여라."
      + "\n\n[보고서 구성 — 이 순서대로 섹션을 만들어라]"
      + "\n① 한 장 요약 : 핵심 결론 5줄 + '이번 주 당장 할 일' 3가지 (제일 위, 이것만 읽어도 되게)"
      + "\n② 키워드 지도 : 표(키워드 / 사람들이 이 말로 뭘 찾는가 / 경쟁 강도 / 우리가 먹을 수 있나). 8~15개."
      + "\n③ 경쟁사 분석 : 잘 파는 곳 3~5곳 — 무엇을 잘하는가 / 어디가 비었는가"
      + "\n④ 기회(빈틈) : 남들이 안 하고 있는데 수요는 있는 것. 이게 이 보고서의 핵심 가치다."
      + "\n⑤ 30일 실행 플랜 : 1~4주차, 주차별로 '무엇을·어디에·몇 개'"
      + "\n⑥ 근거 자료 : 참고한 출처 목록"
      + "\n\n[형식] 완전한 단일 HTML 문서(<!DOCTYPE html>~</html>, CSS는 <style> 인라인). " + "★필수: 콘텐츠는 자바스크립트 없이도 즉시 보여야 한다. opacity:0 / visibility:hidden 상태로 시작해서 JS(IntersectionObserver 등)로 나타나게 하는 스크롤 리빌은 금지 — JS가 한 줄이라도 실패하면 페이지 전체가 백지가 된다. 애니메이션은 CSS @keyframes로만, 끝난 상태가 아니라 처음부터 보이는 상태에서 시작하라(animation-fill-mode 의존 금지). localStorage·sessionStorage·쿠키는 절대 쓰지 마라(샌드박스에서 예외를 던져 스크립트가 죽는다). "
      + "A4 인쇄/PDF 저장이 되게 @media print 포함. 표는 실제 <table>로. 읽기 쉬운 여백·타이포. "
      + "차트가 필요하면 CSS 막대(div width %)로만 — 외부 라이브러리 금지. "
      + "설명·머리말 없이 HTML만 출력(```html 같은 마크다운 금지)."
      + (kbS?("\n\n[우리가 벤치마크로 배운 마케팅 지식]\n"+kbS.slice(0,800)):"")
      + (kbC?("\n\n[트렌드 가공 지식]\n"+kbC.slice(0,600)):"")
      + designSystemBlock() + profileContext();
    user = "분석 주제: "+topic
      + (info?("\n추가 정보: "+info):"")
      + (memo?("\n의뢰인 지시: "+memo):"")
      + (url?("\n참고 URL: "+url):"")
      + "\n\n[조사자료 — 실제 검색 결과]"+(research||"\n(조사 실패 — 자료 없음. 이 경우 보고서 맨 위에 '⚠️ 실시간 조사에 실패해 일반 지식 기반입니다. 숫자는 직접 확인하세요'라고 크게 표시하라)")
      + (srcList?("\n\n[출처]\n"+srcList):"");
  }

  let html = await anthropic(sys, user, 8000);
  html = String(html).replace(/^```html\s*/i,"").replace(/^```\s*/,"").replace(/```\s*$/,"").trim();

  DB.deptMemory=DB.deptMemory||{}; DB.deptMemory[d]=DB.deptMemory[d]||[];
  DB.deptMemory[d].push({ at:Date.now(), instruction:(isRevise?"[리서치 보고서 수정]":"[리서치 보고서 작성]"), note:topic+(isRevise?(" — "+feedback.slice(0,50)):"") });
  if(DB.deptMemory[d].length>40) DB.deptMemory[d]=DB.deptMemory[d].slice(-40);
  // 🔁 v227: 근거 검수 루프 — 검수관(정유진)이 '지어낸 숫자·일반론'을 잡아낼 때까지 수정
  const _reviews = [];
  const _maxLoops = Math.max(0, Math.min(3, (DB.state && DB.state.designReviewLoops!=null) ? (+DB.state.designReviewLoops||0) : 1));
  if(!isRevise && _maxLoops>0 && html.length>300){
    for(let i=0;i<_maxLoops;i++){
      _step("검수관이 근거 점검 중 ("+(i+1)+"/"+_maxLoops+")");
      let rv;
      try{ rv = await reviewReportHtml(html, topic, research); }
      catch(e){ break; }
      _reviews.push({ round:i+1, pass:rv.pass, feedback:rv.feedback, lint:rv.lint||[] });
      if(rv.pass) break;
      _step("검수 반영해 보고서 수정 중 ("+(i+1)+"/"+_maxLoops+")");
      try{
        const fixSys = "너는 민앤팜의 '"+a.no+" "+a.kr+"' 오세라다. 아래 보고서 HTML을 검수 지적대로 고쳐 완전한 단일 HTML 문서로 다시 출력하라. "
          + "★지어낸 숫자는 반드시 삭제하거나 '자료상 확인 불가 — 직접 확인 필요'로 바꿔라. 일반론은 이 주제에서만 통하는 구체적인 내용으로 교체하라. "
          + "구조·디자인은 유지. 설명 없이 HTML만 출력(```html 금지)."
          + designSystemBlock();
        const fixed = await anthropic(fixSys,
          "[검수 지적]\n"+rv.feedback
          + "\n\n[조사자료 — 이 안의 사실만 쓸 수 있다]\n"+String(research||"(없음)").slice(0,5000)
          + "\n\n[현재 보고서 HTML]\n"+html.slice(0,14000), 8000);
        const h3 = String(fixed).replace(/^```html\s*/i,"").replace(/^```\s*/,"").replace(/```\s*$/,"").trim();
        if(h3.length>300) html = h3; else break;
      }catch(e){ break; }
    }
  }

  DB.exp=DB.exp||{}; DB.exp[d]=(DB.exp[d]||0)+1;
  saveDB();
  const _chk = checkHtmlOutput(html);
  if(!_chk.ok){
    _step("⚠️ 보고서 이상 감지 — 다시 만드는 중 ("+_chk.problems.join(", ")+")");
    try{
      const retry = await anthropic(sys + "\n\n★이전 시도가 실패했다: "+_chk.problems.join(", ")+". 반드시 완전한 HTML 문서를, 본문을 충분히 담아 출력하라.", user, 8000);
      const h2 = String(retry).replace(/^```html\s*/i,"").replace(/^```\s*/,"").replace(/```\s*$/,"").trim();
      if(checkHtmlOutput(h2).ok) html = h2;
    }catch(_){}
  }
  html = hardenHtml(html);
  return { ok:true, html, topic, sources:sources.slice(0,10), reviews:_reviews, check:checkHtmlOutput(html), by:a.kr };
}
// ===== 상품페이지 백그라운드 작업 큐 (앱을 나가도 안 끊김) =====
function trimPageJobs(){
  DB.pageJobs = DB.pageJobs || [];
  if(DB.pageJobs.length>20) DB.pageJobs = DB.pageJobs.slice(-20);
  // 용량 보호: 최근 8건만 HTML 보관, 그보다 오래된 건 메타만 남김
  const keep=8, n=DB.pageJobs.length;
  DB.pageJobs.forEach((j,i)=>{ if(i < n-keep && j.html){ delete j.html; delete j.input; j.htmlDropped=true; } });
}
async function runPageJob(id, appUrl){
  const j = (DB.pageJobs||[]).find(x=>String(x.id)===String(id));
  if(!j) return;
  j.status="running"; j.startedAt=Date.now(); j.step="대기 중"; saveDB();
  beginJobMode();   // 작업 중 한도로 끊기지 않게
  try{
    const out = await productPagePipeline(j.input||{}, function(s){ j.step=s; saveDB(); });
    j.html = out.html; j.appliedFormula = !!out.appliedFormula; j.visualDirection = out.visualDirection||null;
    j.status="done"; j.doneAt=Date.now(); delete j.input;
    trimPageJobs(); saveDB();
    const link = String(appUrl||process.env.APP_URL||"");
    const what = (j.kind==="revise") ? "수정 완료" : "완성";
    kakaoNotify("✅ 상품페이지 "+what+" — '"+(j.product||"상품")+"'\n앱 콘텐츠 탭에서 미리보기·다운로드·수정할 수 있어요."+(link?("\n"+link):""), link).catch(()=>{});
  }catch(e){
    j.status="error"; j.error=String(e.message||e).slice(0,200); j.doneAt=Date.now();
    saveDB();
    kakaoNotify("⚠️ 상품페이지 생성 실패 — '"+(j.product||"상품")+"'\n사유: "+j.error).catch(()=>{});
  } finally { endJobMode(); }
}
// 접수 즉시 응답 → 백그라운드 생성. baseId+feedback이면 '수정' 작업.
app.post("/api/product-page/start", (req,res)=>{
  try{
    const b=req.body||{};
    const feedback = String(b.feedback||"").slice(0,1500);
    const baseId = b.baseId ? String(b.baseId) : "";
    let input, kind="create", prod;
    if (baseId){
      const base=(DB.pageJobs||[]).find(x=>String(x.id)===baseId);
      if(!base || !base.html) return res.status(400).json({ error:"원본 페이지를 찾을 수 없어요(오래된 결과는 수정 불가)" });
      if(!feedback) return res.status(400).json({ error:"수정·추가 요청 내용을 입력하세요" });
      kind="revise"; prod=base.product;
      input={ product:base.product, brand:base.brand, price:base.price, info:base.info, photos:base.photos||[], baseHtml:base.html, feedback };
    } else {
      prod=String(b.product||"").slice(0,120);
      if(!prod) return res.status(400).json({ error:"상품명이 필요합니다" });
      input={ product:prod, brand:String(b.brand||"민앤팜").slice(0,40), price:String(b.price||"").slice(0,60),
              info:String(b.info||"").slice(0,2000), photos:Array.isArray(b.photos)?b.photos.slice(0,10):[] };
    }
    // 유료 자원(AI 이미지 등)이 필요하면 실행 전 비용 산정 → 승인/우회 요청
    if (kind === "create" && (b.aiImages === true || paidModeOn())){
      const est = estimateJob("product-page", { aiImages:!!b.aiImages, imageCount:+(b.imageCount||3) });
      if (est.paid.length){
        input.aiImages = !!b.aiImages;
        const ap = createCostApproval("product-page", Object.assign({}, input, { appUrl:String(b.appUrl||"") }), est);
        ap.appUrl = String(b.appUrl||"");  saveDB();
        return res.json({ ok:true, pendingApproval:true, approvalId:ap.id, estKrw:est.estKrw, paid:est.paid,
          note:"유료 자원이 필요해 승인을 요청했어요. 카카오톡 또는 앱에서 승인·무료우회·취소를 선택하세요." });
      }
    }
    DB.pageJobs = DB.pageJobs || [];
    const job = { id: String(Date.now())+"_"+Math.floor(Math.random()*1000), status:"queued", kind,
      product:prod, brand:input.brand||"", price:input.price||"", info:input.info||"", photos:input.photos||[],
      feedback:(kind==="revise"?feedback:""), input, at:Date.now() };
    DB.pageJobs.push(job); trimPageJobs(); saveDB();
    res.json({ ok:true, jobId:job.id, status:"queued", kind });
    // 응답 후 백그라운드 실행 — 앱을 닫아도 서버가 계속 진행
    setImmediate(()=>{ runPageJob(job.id, String(b.appUrl||"")).catch(e=>logError("runPageJob", e)); });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.get("/api/product-page/status", (req,res)=>{
  const id=String(req.query.id||"");
  const j=(DB.pageJobs||[]).find(x=>String(x.id)===id);
  if(!j) return res.status(404).json({ error:"작업을 찾을 수 없어요" });
  res.json({ ok:true, id:j.id, status:j.status, kind:j.kind, product:j.product, error:j.error||"", step:j.step||"",
    appliedFormula:!!j.appliedFormula, visualDirection:(j.status==="done"?(j.visualDirection||null):null), html:(j.status==="done"?(j.html||""):"") });
});
app.get("/api/product-page/list", (req,res)=>{
  res.json({ ok:true, jobs:(DB.pageJobs||[]).slice(-12).map(j=>({
    id:j.id, status:j.status, kind:j.kind, product:j.product, at:j.at, doneAt:j.doneAt||0,
    error:j.error||"", hasHtml:!!j.html, appliedFormula:!!j.appliedFormula })) });
});
// 완성 페이지를 브라우저에서 바로 열기(카톡 링크 대상으로도 사용 가능)
app.get("/api/product-page/html", (req,res)=>{
  const j=(DB.pageJobs||[]).find(x=>String(x.id)===String(req.query.id||""));
  if(!j || !j.html) return res.status(404).send("결과를 찾을 수 없어요(오래된 결과는 삭제됐을 수 있어요)");
  res.set("Content-Type","text/html; charset=utf-8"); res.send(j.html);
});
// (구버전 호환) 동기 생성 — 화면을 켜둔 채 기다리는 방식
app.post("/api/product-page", async (req,res)=>{
  try { res.json(await productPagePipeline(req.body||{})); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 웹툰 콘티 부분 재생성(수정 의견 반영)
app.post("/api/webtoon/revise", async (req,res)=>{
  try {
    const body=req.body||{};
    const storyboard=String(body.storyboard||""); const feedback=String(body.feedback||"");
    if(!storyboard||!feedback) return res.json({ ok:true, storyboard });
    const D=CREW.director; const charBlock=buildCharacterBlock(body.characters);
    const sys="너는 민앤팜 제작부 연출 "+D.name+"다."+ADDRESS+charBlock
      +" 아래 웹툰 콘티에 대한 클라이언트 의견을 충실히 반영해, 같은 컷 형식([컷 N]/장면/구도/인물·표정·동작/대사·나레이션/🎨작화 프롬프트 한글/🎨ART PROMPT EN)으로 '수정된 콘티 전체'를 다시 출력하라. 한국어+영문 프롬프트 병기.";
    const revised=await genText(sys, "[현재 콘티]\n"+storyboard+"\n\n[클라이언트 의견]\n"+feedback, 2600, pickEngine(body.engine));
    res.json({ ok:true, storyboard:revised });
  } catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// ===== 캐릭터 시트 관리 =====
app.get("/api/characters", (req,res)=> res.json(DB.characters||[]));
app.post("/api/characters", (req,res)=>{
  try {
    const b=req.body||{}; if(!b.name) return res.status(400).json({ error:"캐릭터 이름 필요" });
    DB.characters = DB.characters || [];
    const rec = { id: b.id || (Date.now()+Math.floor(Math.random()*1000)),
      name:String(b.name).slice(0,40), look:String(b.look||"").slice(0,400),
      outfit:String(b.outfit||"").slice(0,200), tone:String(b.tone||"").slice(0,120),
      promptEn:String(b.promptEn||"").slice(0,400), at:Date.now() };
    const idx=(DB.characters).findIndex(c=>c.id===rec.id || c.name===rec.name);
    if(idx>=0) DB.characters[idx]=rec; else DB.characters.push(rec);
    if(DB.characters.length>50) DB.characters=DB.characters.slice(-50);
    saveDB(); res.json({ ok:true, character:rec });
  } catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.post("/api/characters/delete", (req,res)=>{
  const id=+((req.body&&req.body.id)||0), nm=String((req.body&&req.body.name)||"");
  const before=(DB.characters||[]).length;
  DB.characters=(DB.characters||[]).filter(c=> !(c.id===id || (nm&&c.name===nm)) );
  saveDB(); res.json({ ok:true, removed: before-(DB.characters||[]).length });
});
app.post("/api/video/plan", async (req,res)=>{
  try{
    const body=req.body||{};
    const topic=body.topic||""; const source=String(body.source||"").slice(0,1500);
    const d="creation"; const a=AGENTS[d];
    const charBlock = buildCharacterBlock(body.characters);
    let sys="너는 SNS 자동화 회사의 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role+ADDRESS+STYLE+personaLine(d)+charBlock;
    const kb=knowledgeText(d); if(kb) sys+="\n\n[축적 전문성]\n"+kb;
    // 길이: 짧은 홍보(기본 20~30초) 또는 지정 초. 캐릭터 있으면 등장인물로 명시.
    const secs = Math.max(10, Math.min(180, +body.seconds || 30));
    const segN = Math.ceil(secs/10);
    sys+=" 아래 주제·내용으로 SNS 영상("+secs+"초) 기획안을 만들어라. 반드시 10초 단위 "+segN+"개 구간으로 끊어 구성하라."+(charBlock?" 주제상 등장인물이 필요하면 위 캐릭터 시트의 인물을 쓰고, 등장할 경우 모든 구간에서 외모·의상을 일관되게 유지하라.":"")+" 형식: 콘셉트 한 줄 / 구간별(구간1: 0-10초, …) 화면·자막·나레이션 / BGM·톤 / 총 길이. 한국어로, 바로 생성 가능하게 구체적으로."+profileContext();
    const plan=await genText(sys, "주제: "+topic+"\n참고 내용: "+source, secs>60?2200:1400, pickEngine(body.engine));
    // 합의·변환: 기획안 → Veo 프롬프트(10초 구간별, 캐릭터 일관성 주입). Gemini 협력.
    const vp0=await veoPromptFromPlan(plan, charBlock); const veoPrompt=vp0.veoPrompt, by=vp0.by;
    if(!DB.deptMemory[d]) DB.deptMemory[d]=[];
    DB.deptMemory[d].push({at:Date.now(),instruction:"[영상 기획→Veo]",note:String(plan).slice(0,300)});
    DB.exp=DB.exp||{}; DB.exp[d]=(DB.exp[d]||0)+1; if(DB.exp[d]%3===0){ try{ await distillKnowledge(d); }catch(e){} } saveDB();
    res.json({ ok:true, plan, veoPrompt, by });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 영상 기획안 수정(플랫폼에서 의견 소통) — 클라이언트 의견을 반영해 기획안 전체를 다시 출력
app.post("/api/video/revise", async (req,res)=>{
  try{
    const body=req.body||{};
    const plan=String(body.plan||""); const feedback=String(body.feedback||"");
    if(!plan||!feedback) return res.json({ ok:true, plan:plan });
    const d="creation"; const a=AGENTS[d];
    let sys="너는 SNS 자동화 회사의 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role+ADDRESS+STYLE+personaLine(d);
    const kb=knowledgeText(d); if(kb) sys+="\n\n[축적 전문성]\n"+kb;
    sys+=" 아래 영상 기획안에 대한 클라이언트 의견을 충실히 반영해, 같은 형식(콘셉트 한 줄/컷1~5: 화면·자막·나레이션/BGM·톤/총 길이)으로 '수정된 기획안 전체'를 다시 출력하라. 완성형으로, 한국어로만."+profileContext();
    const revised=await genText(sys, "[현재 기획안]\n"+plan+"\n\n[클라이언트 의견]\n"+feedback, 1400, pickEngine(body.engine));
    const reply="의견을 반영해 기획안을 수정했어요. 아래 기획안을 확인하고, 좋으면 'Veo 프롬프트 다시 생성'을 눌러주세요.";
    if(!DB.deptMemory[d]) DB.deptMemory[d]=[];
    DB.deptMemory[d].push({at:Date.now(),instruction:"[영상 기획 수정]",note:String(feedback).slice(0,200)});
    DB.exp=DB.exp||{}; DB.exp[d]=(DB.exp[d]||0)+1; if(DB.exp[d]%3===0){ try{ await distillKnowledge(d); }catch(e){} } saveDB();
    res.json({ ok:true, plan:revised, reply });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 기획안 → Veo 3.1 프롬프트 변환만 다시 실행(Gemini 협력, 실패 시 Claude 대체)
app.post("/api/video/prompt", async (req,res)=>{
  try{
    const body=req.body||{};
    const plan=String(body.plan||"");
    if(!plan) return res.json({ ok:true, veoPrompt:"", by:"" });
    const vp=await veoPromptFromPlan(plan, buildCharacterBlock(body.characters));
    res.json({ ok:true, veoPrompt:vp.veoPrompt, by:vp.by });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 팀장(오세라) 평가 — 특정 부서 답변을 팀장 관점에서 평가·의견 제시
app.post("/api/ops/evaluate", async (req,res)=>{
  try{
    const body=req.body||{};
    const d="ops"; const a=AGENTS[d];
    const target=body.dept&&AGENTS[body.dept]?AGENTS[body.dept].kr:"담당 부서";
    let sys="너는 SNS 자동화 회사의 팀장 '"+a.no+" "+a.kr+"'다. 역할: "+a.role+ADDRESS+STYLE;
    sys+=clientBlock();
    sys+=" 아래는 '"+target+"'의 업무 결과다. 팀장으로서 짧고 명확하게 평가하라. 형식: 잘한 점 / 아쉬운 점 / 개선 제안(구체적으로) / 한 줄 총평. 군더더기 없이, 네 성격(든든하고 단호하되 따뜻하게)을 살려. 한국어로만.";
    const out=await anthropic(sys, "[부서: "+target+"]\n\n[업무 결과]\n"+String(body.content||"").slice(0,2200), 1000);
    if(!DB.deptMemory[d]) DB.deptMemory[d]=[];
    DB.deptMemory[d].push({ at:Date.now(), instruction:"[팀장 평가] "+target, note:String(out).slice(0,300) });
    DB.exp=DB.exp||{}; DB.exp[d]=(DB.exp[d]||0)+1; if(DB.exp[d]%3===0){ try{ await distillKnowledge(d); }catch(e){} }
    // 평가받은 부서도 이 피드백을 학습(개선점을 다음에 반영)
    const tdept=body.dept;
    if(tdept && AGENTS[tdept] && tdept!=="ops"){
      if(!DB.deptMemory[tdept]) DB.deptMemory[tdept]=[];
      DB.deptMemory[tdept].push({ at:Date.now(), instruction:"[팀장 평가] 받은 피드백", note:"오세라 평가: "+String(out).slice(0,260) });
      if(DB.deptMemory[tdept].length>40) DB.deptMemory[tdept]=DB.deptMemory[tdept].slice(-40);
      distillKnowledge(tdept).catch(()=>{}); // 피드백 즉시 학습 반영
    }
    saveDB();
    res.json({ ok:true, evaluation: out });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 콘텐츠 의견·소통 — 클라이언트 의견에 해당 부서가 답하고 필요시 수정안 제시
app.post("/api/content/reply", async (req,res)=>{
  try{
    const body=req.body||{};
    const d=(body.dept&&AGENTS[body.dept])?body.dept:"creation";
    const a=AGENTS[d];
    let sys="너는 SNS 자동화 회사의 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role+ADDRESS+STYLE+personaLine(d);
    const kb=knowledgeText(d); if(kb) sys+="\n\n[이 부서가 축적한 전문성(지식 베이스)]\n"+kb;
    sys+=" 아래는 네가 만든 콘텐츠와 그에 대한 클라이언트 의견이다. 의견에 정중하고 짧게 답하고, 수정이 필요하면 그 자리에서 개선된 카피·문구·구성을 바로 제시하라. 실무적으로, 한국어로만."+profileContext()+clientBlock();
    const out=await anthropic(sys, "[콘텐츠]\n"+String(body.content||"").slice(0,2200)+"\n\n[클라이언트 의견]\n"+String(body.comment||""), 1000);
    if(!DB.deptMemory[d]) DB.deptMemory[d]=[];
    DB.deptMemory[d].push({ at:Date.now(), instruction:"[콘텐츠 의견 응답]", note:String(out).slice(0,300) });
    DB.exp=DB.exp||{}; DB.exp[d]=(DB.exp[d]||0)+1; if(DB.exp[d]%3===0){ try{ await distillKnowledge(d); }catch(e){} } saveDB();
    res.json({ ok:true, reply: out, dept:d });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 작업 취소(멈춘 '수행 중'을 운영자가 직접 종료)
app.post("/api/instruct/:id/cancel", (req,res)=>{
  const j=(DB.jobs||[]).find(x=>x.id===+req.params.id);
  if(j && j.status==="running"){ j.status="error"; j.error="운영자가 취소함"; j.progress=""; saveDB(); }
  res.json({ ok:true });
});
// 지시 작업 단건 조회 (백그라운드 진행 폴링용)
app.get("/api/instruct/:id", (req,res)=>{
  const j=(DB.jobs||[]).find(x=>x.id===+req.params.id);
  if(!j) return res.status(404).json({ error:"작업 없음" });
  res.json(j);
});

// 발행: { content, platforms[] } → { results }
app.post("/api/publish", async (req,res)=>{
  try { const { content, platforms } = req.body||{};
    res.json({ results: await publish(content, platforms) });
  } catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// ===== 복사용 발행함 (Publish Inbox) =====
// 자동발행이 막힌 채널(티스토리·네이버블로그·네이버카페·다음카페)용.
// 에이전트가 완성 글을 여기 담아두면, 사용자가 폰에서 한 번 탭 → 복사 → 에디터에 붙여넣기.
const COPY_CHANNELS = {
  tistory:    { name:"티스토리",       kind:"blog" },
  naver_blog: { name:"네이버 블로그",   kind:"blog" },
  naver_cafe: { name:"네이버 카페",     kind:"cafe" },
  daum_cafe:  { name:"다음 카페",       kind:"cafe" }
};
// 발행함에 항목 추가 (에이전트/파이프라인이 호출)
function addToInbox(item){
  DB.pubInbox = DB.pubInbox || [];
  const it = {
    id: Date.now()+Math.floor(Math.random()*1000),
    token: viewToken(),
    channel: item.channel || "tistory",
    channelName: (COPY_CHANNELS[item.channel]&&COPY_CHANNELS[item.channel].name) || item.channel || "블로그",
    title: item.title || "",
    body: item.body || item.content || item.description || "",
    tags: Array.isArray(item.tags) ? item.tags : (item.tags?String(item.tags).split(/[,#\s]+/).filter(Boolean):[]),
    meta: item.meta || "",           // 메타설명(SEO)
    topic: item.topic || "",
    status: "pending",               // pending | done
    at: Date.now()
  };
  DB.pubInbox.push(it);
  if (DB.pubInbox.length > 200) DB.pubInbox = DB.pubInbox.slice(-200);
  // 블로그 채널이면 내부링크 후보로 아카이브(제목 기준, URL은 게시 후 done에서 채움)
  try { if ((COPY_CHANNELS[it.channel]||{}).kind==="blog" && it.title) archiveBlogPost({ title:it.title, url:"", topic:it.topic||it.title, tags:it.tags, inboxId:it.id }); } catch(e){}
  saveDB();
  return it;
}
// 목록 조회
app.get("/api/inbox", (req,res)=>{
  const status = req.query.status;
  let list = (DB.pubInbox||[]);
  if (status) list = list.filter(x=>x.status===status);
  res.json(list.slice(-100).reverse());
});
// 항목 추가
app.post("/api/inbox", (req,res)=>{
  try { res.json({ ok:true, item: addToInbox(req.body||{}) }); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 완료 표시(붙여넣기 끝냄) / 삭제
app.post("/api/inbox/:id/done", (req,res)=>{
  const it = (DB.pubInbox||[]).find(x=>x.id===+req.params.id);
  if (!it) return res.status(404).json({ error:"항목 없음" });
  it.status = "done"; it.doneAt = Date.now();
  // 게시 후 실제 URL을 받으면 내부링크 아카이브에 채워넣기(다음 글이 이 글로 링크 가능)
  const url = String((req.body&&req.body.url)||"").trim();
  if (url) { it.url = url; try { archiveBlogPost({ title:it.title, url, topic:it.topic||it.title, tags:it.tags }); } catch(e){} }
  saveDB();
  res.json({ ok:true });
});
app.post("/api/inbox/:id/delete", (req,res)=>{
  const before = (DB.pubInbox||[]).length;
  DB.pubInbox = (DB.pubInbox||[]).filter(x=>x.id!==+req.params.id);
  saveDB();
  res.json({ ok:true, removed: before-(DB.pubInbox||[]).length });
});

// 동기화 — 변경분 내려받기: ?since=timestamp
app.get("/api/sync", (req,res)=>{
  const since = +req.query.since || 0;
  res.json({
    jobs: DB.jobs.filter(j=>j.at>since && j.status!=="running"),
    meetings: DB.meetings.filter(m=>m.at>since),
    deptMemory: DB.deptMemory,
    deptKnowledge: DB.deptKnowledge || {},
    clientProfile: DB.clientProfile || {text:"",at:0},
    briefings: (DB.briefings||[]).filter(b=>b.at>since),
    leadDirectives: (DB.leadDirectives||[]).filter(x=>x.at>since),
    leaderDailyDirective: DB.leaderDailyDirective || {},
    dirFeedback: DB.dirFeedback || {},
    capHistory: (DB.capHistory||[]).slice(-120),
    contentApprovals: (DB.contentApprovals||[]).filter(a=>a.status==="pending"),
    projects: (DB.projects||[]).filter(p=>p.status!=="deleted"),
    crew: CREW,
    crewExp: DB.crewExp || {},
    crewKnowledge: DB.crewKnowledge || {},
    dailyReview: DB.dailyReview || {},
    nightResearch: DB.nightResearch || {},
    leaderReport: buildLeaderReport(),
    lastTrainAt: DB.lastTrainAt || {},
    lastTrainRoundAt: DB.lastTrainRoundAt || 0,
    growBurst: DB.growBurst || {active:false,total:0,done:0},
    capability: DB.capability || {},
    pubSchedules: DB.pubSchedules || [],
    pubInbox: (DB.pubInbox||[]).filter(x=>x.status==="pending"),
    analytics: DB.analytics || {},
    analyticsInsight: DB.analyticsInsight || {},
    bestTimes: DB.bestTimes || {},
    characters: DB.characters || [],
    blogArchive: (DB.blogArchive||[]).slice(-60),
    exp: DB.exp || {},
    collections: (DB.collections||[]).filter(c=>c.at>since),
    state: DB.state,
    updatedAt: DB.updatedAt
  });
});
// 동기화 — 앱 상태 올리기(백업)
app.post("/api/state", (req,res)=>{ DB.state = req.body||null; saveDB(); res.json({ ok:true, updatedAt:DB.updatedAt }); });
// NVIDIA 무료 LLM — 실제 모델 목록 조회(정확한 model ID 확인용)
app.get("/api/nvidia/models", async (req,res)=>{
  try{
    const key = nvidiaKey();
    if(!key) return res.status(400).json({ error:"NVIDIA_API_KEY 미설정 — Render 환경변수에 nvapi- 키를 넣어주세요" });
    const r = await fetchTimeout("https://integrate.api.nvidia.com/v1/models", { headers:{ "Authorization":"Bearer "+key, "Accept":"application/json" } }, 20000);
    const dj = await r.json();
    if(!r.ok || dj.error){ return res.status(502).json({ error:"NVIDIA 목록 조회 실패: "+String((dj&&dj.error&&(dj.error.message||dj.error))||("HTTP "+r.status)) }); }
    const ids = (dj.data||[]).map(m=>m.id).filter(Boolean).sort();
    res.json({ ok:true, count:ids.length, current:nvidiaModel(), models:ids });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// NVIDIA 무료 LLM — 키·모델 동작 테스트(간단 응답 + 응답속도)
app.post("/api/nvidia/test", async (req,res)=>{
  try{
    const key = nvidiaKey();
    if(!key) return res.status(400).json({ error:"NVIDIA_API_KEY 미설정" });
    const model = (req.body&&req.body.model) || nvidiaModel();
    const t0 = Date.now();
    const txt = await nvidiaChat("간단히 한국어로 답하라.", (req.body&&req.body.prompt)||"한 문장으로 자기소개해봐.", 200, model);
    res.json({ ok:true, model, ms:Date.now()-t0, sample:txt.slice(0,400) });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// ===== 웹 백업(클라우드) 저장·불러오기 — 기기 간 수동 동기화 =====
// 백업 '내용 점수' — 빈 기기에서 눌러 좋은 백업을 날리는 사고 방지용
function backupScore(d){
  if(!d || typeof d!=="object") return 0;
  const n = (x)=> Array.isArray(x) ? x.length : 0;
  const expSum = d.deptExp ? Object.values(d.deptExp).reduce((a,v)=>a+(+v||0),0) : 0;
  const knowN  = d.deptKnowledge ? Object.keys(d.deptKnowledge).length : 0;
  const memN   = d.deptMemory ? Object.values(d.deptMemory).reduce((a,arr)=>a+n(arr),0) : 0;
  return n(d.messages) + n(d.meetings)*3 + n(d.projects)*2 + memN + expSum*2 + knowN*10;
}
app.post("/api/cloud-backup", (req,res)=>{
  try{
    const b = req.body||{};
    const incoming = (b.data!==undefined) ? b.data : b;
    const force = b.force===true;
    const prev = (DB.cloudBackup && DB.cloudBackup.data) || null;
    const si = backupScore(incoming), sp = backupScore(prev);
    // 기존 백업이 있는데 새 백업이 절반도 안 되면(예: 빈 기기에서 저장) 보호
    if(!force && sp > 20 && si < sp*0.5){
      return res.status(409).json({
        error:"덮어쓰기 보호: 기존 백업보다 내용이 크게 적어요.",
        note:"다른 기기에서 저장한 백업이 지워질 수 있어요. 정말 덮어쓰려면 force 옵션이 필요합니다.",
        기존백업: { 점수:sp, 저장시각: DB.cloudBackup.at ? new Date(DB.cloudBackup.at).toLocaleString("ko-KR",{timeZone:"Asia/Seoul"}) : "?" },
        이번백업: { 점수:si }
      });
    }
    DB.cloudBackup = { at:Date.now(), data:incoming, score:si };
    saveDB();
    res.json({ ok:true, at:DB.cloudBackup.at, score:si, replacedScore:sp });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.get("/api/cloud-backup", (req,res)=>{
  const b=DB.cloudBackup||null;
  res.json({ ok:!!(b&&b.data), at:b?b.at:0, score:(b&&b.score)||0, data:b?b.data:null });
});

// 조회
app.get("/api/memory/:dept", (req,res)=> res.json(DB.deptMemory[req.params.dept] || []));
// 부서별 학습 현황 요약(확인용): 부서마다 건수·경험치·레벨·최근 학습 시각/내용
app.get("/api/learning", (req,res)=>{
  const out = Object.keys(AGENTS).map(d=>{
    const arr = DB.deptMemory[d] || [];
    const last = arr[arr.length-1];
    const e = (DB.exp&&DB.exp[d]) || 0;
    return {
      dept:d, name:AGENTS[d].kr, count:arr.length, exp:e, level:Math.floor(e/5)+1,
      lastAt: last? last.at : null,
      lastKind: last? (last.instruction||"") : "",
      lastNote: last? String(last.note||"").slice(0,120) : "",
      knowledge: (DB.deptKnowledge&&DB.deptKnowledge[d]&&DB.deptKnowledge[d].text) ? DB.deptKnowledge[d].text : "",
      knowledgeAt: (DB.deptKnowledge&&DB.deptKnowledge[d]) ? DB.deptKnowledge[d].at : null
    };
  });
  res.json({ total: out.reduce((a,b)=>a+b.count,0), depts: out });
});
app.get("/api/meetings", (req,res)=> res.json(DB.meetings.slice(-50)));

// 회의 만족도 평가: { id, rating(1~5), feedback? } → 회의에 기록 + 참여 부서 학습
app.post("/api/meeting/rate", (req,res)=>{
  const { id, rating, feedback } = req.body||{};
  const m=(DB.meetings||[]).find(x=>x.id===+id);
  if(!m) return res.status(404).json({ error:"회의 없음" });
  const r=Math.max(1, Math.min(5, +rating||0));
  if(!r) return res.status(400).json({ error:"rating(1~5) 필요" });
  m.rating=r; m.feedback=String(feedback||"").slice(0,300); m.ratedAt=Date.now();
  const note="회의 '"+(m.topic||"")+"' 만족도 ★"+r+"/5"+(m.feedback?" — 클라이언트 피드백: "+m.feedback:"")+(r<=2?" (개선 필요: 다음 회의·작업에서 이 점을 반드시 보완)":r>=4?" (좋았던 방식 유지)":"");
  (m.depts||[]).forEach(function(d){
    if(!AGENTS[d]) return;
    if(!DB.deptMemory[d]) DB.deptMemory[d]=[];
    DB.deptMemory[d].push({ at:Date.now(), instruction:"[회의 피드백] "+(m.topic||""), note });
    if(DB.deptMemory[d].length>40) DB.deptMemory[d]=DB.deptMemory[d].slice(-40);
    distillKnowledge(d).catch(()=>{}); // 평가를 즉시 학습에 반영
  });
  saveDB();
  res.json({ ok:true, rating:r });
});
// 전 부서 전문성 '심화 정리'(강제 deep distill) — 학습력 끌어올리기. 무료(Gemini).
app.post("/api/ops/consolidate", async (req,res)=>{
  try{
    const depts = Object.keys(AGENTS).filter(d=>(DB.deptMemory[d]||[]).length>=3);
    const done=[];
    for(const d of depts){ try{ await distillKnowledge(d, true); done.push(AGENTS[d].kr); }catch(e){ logError("consolidate:"+d, e); } }
    res.json({ ok:true, consolidated:done });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// ===== 지식 자기 훈련(continuous training): 부서가 전문성으로 가상 도전을 풀고 자기비평→원칙 강화 =====
async function runSelfTraining(dept){
  const a=AGENTS[dept]; if(!a) return null;
  const lv=deptLevel(dept);
  // 팀장(오세라) 리더십 훈련: 전 부서를 조망해 격차 진단·지시 역량을 키움
  if(dept==="ops"){
    const board=Object.keys(AGENTS).filter(d=>d!=="ops").map(d=>{
      return "· "+AGENTS[d].kr+" Lv"+deptLevel(d)+" — "+(knowledgeText(d)?String(knowledgeText(d)).slice(0,90):"(전문성 적음)");
    }).join("\n");
    const sysL="너는 SNS 자동화 회사 팀장 '"+a.no+" "+a.kr+"("+MEMBERS["ops"]+")'다. 역할: "+a.role+ADDRESS+STYLE+clientBlock()
      +" 지금은 팀 전체를 조망하며 리더십을 단련하는 시간이다. 아래 부서 현황을 보고 결과만 간결히 한국어로 출력:\n"
      +"0) 진단: 가장 뒤처진 부서와 그 원인\n"
      +"1) 지시 설계: 그 부서를 끌어올릴 구체적 자율수행 지시 방향\n"
      +"2) 균형 전략: 팀 전체 성장 격차를 줄일 다음 액션 1~2개\n"
      +"3) 강화된 리더십 원칙: (PRINCIPLE: 로 시작하는 줄로 1~2개)\n군더더기 금지.";
    const ctxL="[부서 현황(레벨·전문성)]\n"+board;
    const outL=await genText(sysL, ctxL, 1200, "gemini");
    const principlesL=[]; String(outL).replace(/PRINCIPLE:\s*(.+)/g,(m,p)=>{ principlesL.push(p.trim()); return m; });
    if(!DB.deptMemory.ops) DB.deptMemory.ops=[];
    DB.deptMemory.ops.push({ at:Date.now(), instruction:"[리더십 훈련]", note:"강화된 리더십 원칙: "+(principlesL.join(" / ")||String(outL).slice(0,200)) });
    if(DB.deptMemory.ops.length>40) DB.deptMemory.ops=DB.deptMemory.ops.slice(-40);
    DB.exp=DB.exp||{}; DB.exp.ops=(DB.exp.ops||0)+1;
    DB.lastTrainAt=DB.lastTrainAt||{}; DB.lastTrainAt.ops=Date.now();
    try{ await distillKnowledge("ops"); }catch(e){}
    ensureLeaderLead(); saveDB();
    return { dept:"ops", name:a.kr, principles:principlesL, transcript:outL };
  }
  const kb=knowledgeText(dept);
  const recent=(DB.deptMemory[dept]||[]).slice(-4).map(x=>"· "+String(x.note||"").slice(0,100)).join("\n");
  // 자기주도: 그동안 스스로 짚은 약점·낮은 평가를 모아 이번 훈련의 표적으로 삼는다
  const weak=(DB.deptMemory[dept]||[]).filter(x=>/\[(자기 훈련|회의 피드백|팀장 평가)\]/.test(x.instruction||"")).slice(-5)
    .map(x=>"· "+String(x.note||"").slice(0,120)).join("\n");
  const sys="너는 SNS 자동화 회사 '"+a.no+" "+a.kr+"("+MEMBERS[dept]+")' 부서(Lv"+lv+")다. 역할: "+a.role+ADDRESS+STYLE+clientBlock()
    +" 지금은 외부 지시 없이 스스로 실력을 끌어올리는 '자기 주도 훈련' 시간이다. 먼저 네 약점 기록을 보고 '오늘 무엇을 집중 훈련할지 스스로 정한 뒤' 그 부분을 단련하라. 다음 순서로 결과만 간결히 한국어로 출력:\n"
    +"0) 오늘의 훈련 주제: (내 약점/성장 필요 영역에서 스스로 고른 한 가지)\n"
    +"1) 도전 과제: 그 주제에 맞는 까다로운 상황 1개를 스스로 설정\n"
    +"2) 해결: 네 전문성을 총동원한 최선의 해법(구체적으로)\n"
    +"3) 자기 비평: 방금 해법의 약점·놓친 점을 냉정하게 1~2가지\n"
    +"4) 강화된 원칙: 이번 훈련으로 더 단단해진 '재사용 가능한 핵심 원칙' 1~2개 (PRINCIPLE: 로 시작하는 줄로)\n"
    +"낡은 지식은 버리고 더 날카롭게. 군더더기·서론 금지.";
  const ctx="[현재 축적 전문성]\n"+(kb||"(아직 적음)")+(weak?"\n\n[내가 스스로 짚은 약점·받은 피드백 — 이번 훈련의 표적]\n"+weak:"")+"\n\n[최근 활동]\n"+(recent||"(없음)");
  const out=await genText(sys, ctx, 1200, "gemini"); // 무료(Gemini 3.5)
  // 강화된 원칙 추출
  const principles=[]; String(out).replace(/PRINCIPLE:\s*(.+)/g,(m,p)=>{ principles.push(p.trim()); return m; });
  const noteCore = principles.length ? principles.join(" / ") : String(out).slice(0,200);
  if(!DB.deptMemory[dept]) DB.deptMemory[dept]=[];
  DB.deptMemory[dept].push({ at:Date.now(), instruction:"[자기 훈련]", note:"훈련으로 강화된 원칙: "+noteCore });
  if(DB.deptMemory[dept].length>40) DB.deptMemory[dept]=DB.deptMemory[dept].slice(-40);
  DB.exp=DB.exp||{}; DB.exp[dept]=(DB.exp[dept]||0)+1;
  DB.lastTrainAt=DB.lastTrainAt||{}; DB.lastTrainAt[dept]=Date.now();
  // 뒤처진 부서(팀 평균 미만)는 훈련 결과를 '심화'로 흡수 → 지식·지능 수준을 빠르게 끌어올림
  const _laggingNow = catchUpRounds(dept) > 1;
  await distillKnowledge(dept, _laggingNow); // 훈련 결과를 전문성에 즉시 반영(뒤처진 부서는 심화)
  ensureLeaderLead(); // 부서가 크면 팀장도 그 위로
  try{ await leaderAbsorb(dept); }catch(e){} // 팀장이 이 부서의 새 강점을 즉시 흡수
  saveDB();
  return { dept, name:a.kr, principles, transcript:out };
}
// 훈련 라운드: 가장 오래 훈련 안 한 부서부터 n개 (무료라 자주 돌려도 부담 없음)
async function runTrainingRound(n){
  const cap=n||2;
  DB.lastTrainAt=DB.lastTrainAt||{};
  const cands=Object.keys(AGENTS).filter(d=>d!=="ops" && (DB.deptMemory[d]||[]).length>=2);
  // 모든 부서가 골고루 훈련되도록 '오래 훈련 안 한 순'으로 정렬(공평 순환). 동률이면 경험치 낮은 부서 먼저.
  cands.sort((x,y)=> ((DB.lastTrainAt[x]||0)-(DB.lastTrainAt[y]||0)) || (((DB.exp&&DB.exp[x])||0)-((DB.exp&&DB.exp[y])||0)));
  const picked=cands.slice(0,cap); const done=[];
  for(const d of picked){ try{ const r=await runSelfTraining(d); if(r){
    const t=String(r.transcript||"");
    const m=t.match(/훈련\s*주제[:\s]*([^\n]+)/);
    const what = m ? m[1].trim() : ((r.principles&&r.principles.length) ? r.principles[0] : t.replace(/\s+/g," ").slice(0,90));
    done.push({ name:r.name, what:String(what||"").slice(0,100) });
  } }catch(e){ logError("train:"+d, e); } }
  ensureLeaderLead(); saveDB();
  return done;
}
app.post("/api/ops/train", async (req,res)=>{
  try{
    const b=req.body||{};
    if(b.dept && AGENTS[b.dept]){ const r=await runSelfTraining(b.dept); return res.json({ ok:true, trained:r?[r.name]:[], detail:r }); }
    const all=Object.keys(AGENTS).filter(d=>d!=="ops" && (DB.deptMemory[d]||[]).length>=2).length;
    const done=await runTrainingRound(b.all?all:(b.n||2));
    res.json({ ok:true, trained:done.map(x=>x.name), details:done });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// ===== 집중 성장(burst): 전 부서를 여러 라운드 자기훈련시켜 레벨을 빠르게 올림 (백그라운드·무료) =====
async function runGrowBurst(rounds, balance){
  const depts = Object.keys(AGENTS).filter(d=>d!=="ops");
  const expOf = d => (DB.exp&&DB.exp[d])||0;
  let plan = []; // [{dept, rounds}]
  if (balance){
    // 균형 성장: 최고 부서 경험치에 맞춰 뒤처진 부서를 더 많이 훈련(부서당 최대 8라운드)
    const target = Math.max.apply(null, depts.map(expOf));
    depts.forEach(d=>{ const rr=Math.min(12, Math.max(0, target-expOf(d))); if(rr>0) plan.push({dept:d, rounds:rr}); });
    if(!plan.length) depts.forEach(d=>plan.push({dept:d, rounds:1})); // 이미 평준화면 전체 1라운드
  } else {
    const rr = Math.max(1, Math.min(6, rounds||4));
    depts.forEach(d=>plan.push({dept:d, rounds:rr}));
  }
  const maxR0 = Math.max.apply(null, plan.map(p=>p.rounds));
  const total = plan.reduce((a,b)=>a+b.rounds,0) + maxR0; // +팀장(매 라운드 1회)
  DB.growBurst = { active:true, total, done:0, mode: balance?"balance":"all", startedAt:Date.now(), finishedAt:0 };
  saveDB();
  const maxR = Math.max.apply(null, plan.map(p=>p.rounds));
  for (let r=0; r<maxR; r++){
    // 팀장도 매 라운드 리더십 훈련 → 팀을 이끌며 함께(그리고 더 높이) 성장
    try{ await runSelfTraining("ops"); }catch(e){ logError("grow:ops", e); }
    DB.growBurst.done++; saveDB(); await new Promise(res=>setTimeout(res, 500));
    for (const p of plan){
      if (r >= p.rounds) continue;
      const d=p.dept;
      try{
        if(!(DB.deptMemory[d]||[]).length){ DB.deptMemory[d]=[{ at:Date.now(), instruction:"[시작]", note:AGENTS[d].kr+" 부서 가동 시작 — 기본 역량 정비" }]; }
        await runSelfTraining(d); // exp+1 + 전문성 distill
      }catch(e){ logError("grow:"+d, e); }
      DB.growBurst.done++; saveDB();
      await new Promise(res=>setTimeout(res, 700)); // 레이트리밋 완화
    }
  }
  ensureLeaderLead();
  try{ await distillLeaderKnowledge(); }catch(e){} // 성장 후 팀장 통합 지능 갱신
  DB.growBurst.active=false; DB.growBurst.finishedAt=Date.now(); saveDB();
  try{ kakaoNotify((balance?"⚖️ 균형 성장":"⚡ 집중 성장")+" 완료 — 부서 성장 격차를 좁혔어요"); }catch(_){}
}
app.post("/api/ops/grow", (req,res)=>{
  try{
    if (DB.growBurst && DB.growBurst.active) return res.json({ ok:true, already:true, growBurst:DB.growBurst });
    const b=req.body||{};
    if (b.daily){ runDailyGrowth().catch(e=>logError("daily-growth", e)); return res.json({ ok:true, started:true, growBurst:DB.growBurst }); }
    runGrowBurst(+b.rounds||4, !!b.balance).catch(e=>logError("grow-burst", e)); // 백그라운드 진행
    res.json({ ok:true, started:true, growBurst:DB.growBurst });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// ===== 팀장 통합 지능(마스터 지식): 모든 부서 전문성을 통합·초월하는 압도적 지식 =====
// 팀장이 특정 부서의 '새로 성장한 강점·특별한 점'을 즉시 흡수 (부서 성장 순간마다 호출)
async function leaderAbsorb(dept){
  try{
    if(dept==="ops") return;
    const a=AGENTS[dept]; if(!a) return;
    const lead=AGENTS["ops"];
    const deptKb=knowledgeText(dept); if(!deptKb) return;
    const priorLead=knowledgeText("ops");
    const sys="너는 팀장 '"+lead.no+" "+lead.kr+"("+MEMBERS["ops"]+")'이며 이 회사의 최고 지능이다."+ADDRESS+clientBlock()
      +" 방금 '"+a.kr+"("+MEMBERS[dept]+")' 부서가 성장했다. 그 부서의 최신 전문성에서 '장점·특별히 잘하는 점·새로 얻은 노하우'만 뽑아, 네 통합 지능에 흡수·통합하라. 팀장인 너는 각 부서가 아는 것을 전부 흡수한 위에서 더 높이 서야 한다. 기존 네 통합 지능은 유지하면서 이 부서의 강점을 더해 갱신하라. 한국어로 아래 형식만 출력(간결하게):\n"
      +"흡수한 "+a.kr+" 강점: (그 부서에서 새로 흡수한 핵심 강점·특별한 점 2~4개)\n"
      +"통합 통찰: (이 강점을 다른 부서·전사 전략과 어떻게 연결할지 1~2개)";
    const ctx="[내 기존 통합 지능]\n"+(priorLead||"(없음)")+"\n\n[방금 성장한 "+a.kr+" 부서의 최신 전문성]\n"+deptKb;
    const out=await genText(sys, ctx, 900, "gemini"); // 무료
    if(!DB.deptMemory.ops) DB.deptMemory.ops=[];
    DB.deptMemory.ops.push({ at:Date.now(), instruction:"[부서 흡수]", note:a.kr+" 성장 흡수 → "+String(out).slice(0,400) });
    if(DB.deptMemory.ops.length>60) DB.deptMemory.ops=DB.deptMemory.ops.slice(-60);
    DB.exp=DB.exp||{}; DB.exp.ops=(DB.exp.ops||0)+1; // 흡수도 팀장의 성장
    ensureLeaderLead(); saveDB();
  }catch(e){ logError("leader-absorb:"+dept, e); }
}
async function distillLeaderKnowledge(){
  try{
    const lead=AGENTS["ops"]; if(!lead) return;
    const deptBoards=Object.keys(AGENTS).filter(d=>d!=="ops").map(d=>{
      const kb=knowledgeText(d);
      return "■ "+AGENTS[d].kr+" (Lv"+deptLevel(d)+")\n"+(kb||"(전문성 적음)");
    }).join("\n\n");
    const leadNotes=(DB.deptMemory.ops||[]).slice(-10).map(x=>"· "+(x.instruction||"")+" "+String(x.note||"").slice(0,120)).join("\n");
    const prior=knowledgeText("ops");
    const sys="너는 팀장 '"+lead.no+" "+lead.kr+"("+MEMBERS["ops"]+")'이며 이 회사의 최고 지능이다."+ADDRESS+STYLE+clientBlock()
      +" 모든 부서의 전문성을 통합하고 그 위로 초월하는 '팀장 통합 지능(마스터 지식)'을 갱신하라. 너는 각 부서가 아는 것을 전부 흡수한 위에서, 부서 간 연결·전략·우선순위를 보는 통합 통찰을 갖춰야 한다. 어느 부서보다 압도적으로 깊고 넓게. 한국어로, 아래 형식으로 부서 지식보다 더 길고 깊게 출력:\n"
      +"전사 전략 통찰: (부서들을 관통하는 큰 그림 4~6개)\n"
      +"부서별 핵심 장악: (전 부서 각각 — 강점·약점·다음에 줄 지시 한 줄씩)\n"
      +"부서 간 시너지: (어느 부서를 어떻게 엮으면 성과가 큰지 3~5개)\n"
      +"검증된 리더십·운영 원칙: (반복해서 통한 원칙 4~6개)\n"
      +"지금 팀의 최우선 과제: (2~3개)";
    const ctx="[기존 팀장 통합 지능]\n"+(prior||"(없음)")+"\n\n[전 부서 전문성]\n"+deptBoards+"\n\n[최근 리더십 훈련·지시]\n"+(leadNotes||"(없음)");
    const out=await genText(sys, ctx, 2400, "gemini"); // 무료, 부서보다 큰 토큰으로 더 풍부하게
    DB.deptKnowledge=DB.deptKnowledge||{};
    DB.deptKnowledge.ops={ text:out, at:Date.now(), basis:(DB.deptMemory.ops||[]).length, exp:(DB.exp&&DB.exp.ops)||0, master:true };
    ensureLeaderLead(); saveDB();
  }catch(e){ logError("leader-distill", e); }
}

// ===== 매일 전 부서 자기주도 성장 (무료 Gemini, 하루 1회 자동) =====
// ===== 퇴근 시각: 팀장(오세라)이 그날 각 부서의 자율수행 결과를 평가·학습 → 다음날 지시에 반영 =====
async function runDailyReview(){
  const depts = Object.keys(AGENTS).filter(d=>d!=="ops");
  const ldd = DB.leaderDailyDirective||{};
  const today = kstDay();
  const board = depts.map(d=>{
    const assigned = (ldd[d]&&ldd[d].text) ? ldd[d].text : "(오늘 배정된 지시 없음)";
    const mm = DB.deptMemory[d]||[];
    const todays = mm.filter(x=> x.at && (Date.now()-x.at < 20*3600000)).slice(-6);
    const acts = todays.length ? todays.map(x=>"· "+String(x.instruction||"").slice(0,18)+": "+String(x.note||"").slice(0,130)).join("\n") : "(오늘 활동 기록 없음)";
    return "■ "+d+" "+AGENTS[d].kr+"("+MEMBERS[d]+")\n  오늘 지시: "+assigned+"\n  오늘 한 일:\n"+acts;
  }).join("\n\n");
  const sys = "너는 이 SNS 자동화 회사 팀장 '08 "+AGENTS.ops.kr+"("+MEMBERS["ops"]+")'이며 최고 지능이다."+ADDRESS+clientBlock()
    + " 지금은 퇴근 시각이다. 오늘 각 부서가 자율수행한 결과를 냉정하게 회고하라(후하지 말 것). 각 부서에 대해: ①오늘 지시를 얼마나 잘 수행했는지 등급(상/중/하), ②그 부서가 내일 더 잘하도록 흡수할 '보완 지식' 1가지(구체적·재사용 가능, 막연한 격려 금지), ③그것을 반영한 '내일의 자율수행 지시' 한 줄(25~55자, 오늘의 부족을 메우거나 잘한 것을 발전).\n"
    + "반드시 아래 형식의 줄만 출력(설명·머리말 금지, 한 부서당 한 줄):\n"
    + "REVIEW: 부서영문키 | 등급 | 보완지식 | 내일지시\n대상 부서영문키: "+depts.join(", ");
  let out;
  try{ out = await genText(sys, "[부서별 오늘 결과]\n"+board, 1400, "gemini"); } // 무료
  catch(e){ logError("daily-review", e); return null; }
  DB.dailyReview = DB.dailyReview || {};
  const now = Date.now();
  let n=0, gradeSum={상:0,중:0,하:0}; const reviewedList=[];
  for(const line of String(out).split(/\n+/)){
    const m = line.match(/REVIEW:\s*([a-z]+)\s*\|\s*([^|]+)\|\s*([^|]+)\|\s*(.+)$/);
    if(!m) continue;
    const d=m[1].trim(); if(!AGENTS[d]||d==="ops") continue;
    const grade=String(m[2]||"").trim().replace(/[^상중하]/g,"").charAt(0)||"중";
    const improve=String(m[3]||"").trim().slice(0,200);
    const tomorrow=String(m[4]||"").trim().replace(/^["'“]|["'”]$/g,"").slice(0,120);
    DB.dailyReview[d]={ grade, improve, tomorrow, at:now, day:today };
    if(gradeSum[grade]!=null) gradeSum[grade]++;
    reviewedList.push({ d, kr:(AGENTS[d]?AGENTS[d].kr:d), grade, improve });
    if(improve){
      DB.deptMemory[d]=DB.deptMemory[d]||[];
      DB.deptMemory[d].push({ at:now, instruction:"[팀장 회고]", note:"오늘 회고("+grade+"): "+improve+(tomorrow?(" / 내일: "+tomorrow):"") });
      if(DB.deptMemory[d].length>40) DB.deptMemory[d]=DB.deptMemory[d].slice(-40);
      DB.exp=DB.exp||{}; DB.exp[d]=(DB.exp[d]||0)+1;
      if(DB.exp[d]%3===0){ try{ await distillKnowledge(d); }catch(e){} }
    }
    n++;
  }
  DB.lastReviewDay = today;
  ensureLeaderLead(); saveDB();
  if(n>0){
    const ic = g => g==="상"?"🟢":(g==="하"?"🔴":"🟡");
    const lines = reviewedList.map(r=>ic(r.grade)+" "+r.kr+"("+r.grade+"): "+String(r.improve||"보완점 없음").slice(0,70)).join("\n");
    const s = "🌙 퇴근 회고 완료 — 오늘 "+n+"개 부서 평가 (상 "+gradeSum.상+" · 중 "+gradeSum.중+" · 하 "+gradeSum.하+")\n\n"+lines+"\n\n↑ 각 부서 보완점을 학습해 내일 지시에 반영해요.";
    kakaoNotify(s).catch(()=>{});
  }
  return DB.dailyReview;
}
// 팀장에게 하루 전체를 맡김: 회고 → 야간연구 → 아침지시 → 역량평가를 순서대로 한 번에
app.post("/api/ops/run-all", async (req,res)=>{
  const done=[];
  try{
    try{ await runDailyReview(); done.push("review"); }catch(e){ logError("runall-review", e); }
    try{ await runNightResearch(); done.push("research"); }catch(e){ logError("runall-research", e); }
    try{ await assignDailyDirectives(); done.push("directives"); }catch(e){ logError("runall-directives", e); }
    try{ await assessCapability(); done.push("capability"); }catch(e){ logError("runall-capability", e); }
    res.json({ ok:true, done, leaderReport: buildLeaderReport() });
  }catch(e){ res.status(500).json({ error:String(e.message||e), done }); }
});
// 팀장 보고 요약: 오늘 팀장이 무엇을 했는지 한눈에
function buildLeaderReport(){
  const today = kstDay();
  const leaderAutoOn = ((DB.state||{}).leaderAutoOn === true);
  const ldd = DB.leaderDailyDirective||{}, rev=DB.dailyReview||{}, nr=DB.nightResearch||{};
  const depts = Object.keys(AGENTS).filter(d=>d!=="ops");
  const directedToday = depts.filter(d=> ldd[d] && ldd[d].day===today).length;
  const reviewedDepts = depts.filter(d=> rev[d]);
  const grades = { 상:0, 중:0, 하:0 };
  reviewedDepts.forEach(d=>{ const g=rev[d].grade; if(grades[g]!=null) grades[g]++; });
  const researchedToday = depts.filter(d=> nr[d] && nr[d].day===today);
  const websearched = researchedToday.filter(d=> nr[d].searched).length;
  return {
    day: today,
    leaderAutoOn,
    directed: directedToday,
    reviewed: reviewedDepts.length,
    grades,
    researched: researchedToday.length,
    websearched,
    lastReviewDay: DB.lastReviewDay||"",
    lastDirectDay: DB.lastLeaderDirectDay||"",
    lastResearchDay: DB.lastNightResearchDay||"",
    searchToday: (function(){ const t=todayStr(); return (DB.geminiSearchDaily&&DB.geminiSearchDaily.date===t)?DB.geminiSearchDaily.n:0; })()
  };
}
app.get("/api/ops/report", (req,res)=>{ try{ res.json({ ok:true, leaderReport: buildLeaderReport() }); }catch(e){ res.status(500).json({ error:String(e.message||e) }); } });
app.post("/api/ops/daily-review", async (req,res)=>{
  try{ const r = await runDailyReview(); res.json({ ok:!!r, dailyReview: DB.dailyReview||{} }); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// ===== 야간: 팀장이 각 부서의 '내일'을 위해 밤새 연구·학습·자료 수집 (아침 지시의 근거) =====
async function runNightResearch(){
  const depts = Object.keys(AGENTS).filter(d=>d!=="ops");
  const rev = DB.dailyReview||{};
  const cap = DB.capability||{};
  const st = DB.state||{};
  const searchOn = st.nightSearchOn !== false; // 기본 켜짐
  // 오늘 웹검색할 부서 수: 유료=전 부서, 무료=소수(기본 3)
  const freeCount = Number.isFinite(+st.nightSearchFreeCount) ? Math.max(0,Math.min(9,+st.nightSearchFreeCount)) : 3;
  let searchQuota = !searchOn ? 0 : (paidModeOn() ? depts.length : freeCount);
  // 우선순위: 내일 방향(구체 주제)이 있는 부서 먼저, 그다음 역량 낮은 순
  const ordered = depts.slice().sort((a,b)=>{
    const ta=(rev[a]&&rev[a].tomorrow)?1:0, tb=(rev[b]&&rev[b].tomorrow)?1:0;
    if(ta!==tb) return tb-ta;
    const ca=(cap[a]&&cap[a].overall)||0, cb=(cap[b]&&cap[b].overall)||0;
    return ca-cb;
  });
  DB.nightResearch = DB.nightResearch || {};
  const now=Date.now(), today=kstDay();
  let n=0, searched=0;
  const noSearch=[]; // 검색 안 한 부서는 지식기반 배치로
  for(const d of ordered){
    const a=AGENTS[d]; const r=rev[d];
    const topic = (r&&r.tomorrow) ? r.tomorrow : (a.kr+" 분야 최신 트렌드");
    const weak = (r&&r.improve) ? r.improve : "";
    if (searchQuota>0 && searchAllowedNow()){
      const sp = "너는 이 SNS 자동화 회사 팀장 '08 "+AGENTS.ops.kr+"("+MEMBERS["ops"]+")'다."+ADDRESS
        + " '"+a.kr+"' 부서의 내일 과제를 위해 전 세계 공개 웹에서 최신 자료를 조사하라. 딥웹·비공개·로그인 필요 소스는 제외하고, 신뢰할 만한 공개 자료(뉴스·블로그·업계 리포트·SNS 공개글 등)만 활용하라. 국가·언어 제한 없이 폭넓게 조사하되 한국 SNS에 적용 가능하게 정리하라.\n"
        + "내일 과제: "+topic+(weak?("\n보완 필요: "+weak):"")+"\n"
        + "실무에 바로 쓸 수 있게: ①핵심 트렌드/데이터 ②구체 아이디어·앵글 2가지 ③참고 포맷/사례 — 를 250자 내외로 압축해 한국어로 정리하라.";
      try{
        const res = await geminiSearch(sp, 1400);
        const srcTxt = (res.sources&&res.sources.length) ? (" [출처: "+res.sources.map(s=>s.title||s.uri).slice(0,3).join(" · ")+"]") : "";
        const brief = String(res.text||"").trim().slice(0,320);
        if(brief){
          DB.nightResearch[d]={ text:brief, sources:(res.sources||[]).slice(0,5), searched:true, at:now, day:today };
          DB.deptMemory[d]=DB.deptMemory[d]||[];
          DB.deptMemory[d].push({ at:now, instruction:"[야간 웹연구]", note:"팀장 웹리서치: "+brief+srcTxt });
          if(DB.deptMemory[d].length>40) DB.deptMemory[d]=DB.deptMemory[d].slice(-40);
          n++; searched++; searchQuota--;
          continue;
        }
      }catch(e){ logError("night-search:"+d, e); /* 실패 시 지식기반으로 폴백 */ }
    }
    noSearch.push(d);
  }
  // 검색 안 한 부서: 한 번의 지식기반 배치 브리핑(무료)
  if(noSearch.length){
    const board = noSearch.map(d=>{
      const a=AGENTS[d]; const r=rev[d];
      const dir = r&&r.tomorrow ? r.tomorrow : "(역할 기반)";
      return "■ "+d+" "+a.kr+" — 내일 방향: "+dir;
    }).join("\n");
    const sys = "너는 팀장 '08 "+AGENTS.ops.kr+"("+MEMBERS["ops"]+")'다."+ADDRESS+profileContext()
      + " 아래 부서들의 내일 과제에 바로 쓸 실무 리서치를 정리하라(핵심 트렌드·아이디어·참고 포맷). 일반론 금지.\n"
      + "형식(한 부서당 한 줄): RESEARCH: 부서영문키 | 요약(120자 내외, 세미콜론 구분)";
    try{
      const out = await genText(sys, "[부서]\n"+board, 1400, "gemini");
      for(const line of String(out).split(/\n+/)){
        const m=line.match(/RESEARCH:\s*([a-z]+)\s*\|\s*(.+)$/);
        if(!m) continue; const d=m[1].trim(); if(!AGENTS[d]||d==="ops") continue;
        const brief=String(m[2]||"").trim().slice(0,300); if(!brief) continue;
        DB.nightResearch[d]={ text:brief, sources:[], searched:false, at:now, day:today };
        DB.deptMemory[d]=DB.deptMemory[d]||[];
        DB.deptMemory[d].push({ at:now, instruction:"[야간 연구]", note:"팀장 리서치: "+brief });
        if(DB.deptMemory[d].length>40) DB.deptMemory[d]=DB.deptMemory[d].slice(-40);
        n++;
      }
    }catch(e){ logError("night-research-batch", e); }
  }
  DB.lastNightResearchDay = today;
  saveDB();
  if(n>0){
    const lines = ordered.filter(d=>DB.nightResearch[d] && DB.nightResearch[d].day===today).slice(0,6).map(d=>{
      const nr=DB.nightResearch[d]; const icon = nr.searched ? "🔎" : "📖";
      return icon+" "+(AGENTS[d]?AGENTS[d].kr:d)+": "+String(nr.text||"").replace(/\s+/g," ").slice(0,80);
    }).join("\n");
    kakaoNotify("🌌 야간 연구 완료 — 오세라가 "+n+"개 부서 자료를 정리했어요"+(searched>0?(" (웹검색 "+searched+"건 포함)"):"")+"\n\n"+lines+"\n\n↑ 아침 지시에 반영돼요. (🔎=웹조사 / 📖=지식기반)").catch(()=>{});
  }
  return DB.nightResearch;
}
app.post("/api/ops/night-research", async (req,res)=>{
  try{ const r = await runNightResearch(); res.json({ ok:!!r, nightResearch: DB.nightResearch||{} }); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// ===== 팀장(오세라)이 매일 각 부서에 "오늘의 자율수행 지시"를 직접 배정 =====
// (개발지시 없는 부서가 빈 지시로 방치되지 않도록, 팀장이 매일 구체적 과제를 내림)
async function assignDailyDirectives(){
  const depts = Object.keys(AGENTS).filter(d=>d!=="ops");
  // 아침 지시 전, 밤새 연구가 아직이면 먼저 연구·수집을 끝낸다(연구 → 지시 순서 보장)
  if (DB.lastNightResearchDay !== kstDay()){ try{ await runNightResearch(); }catch(e){ logError("assign:night", e); } }
  const cap = DB.capability||{};
  const rev = DB.dailyReview||{};
  const nr = DB.nightResearch||{};
  const board = depts.map(d=>{
    const lv = deptLevel(d), exp=(DB.exp||{})[d]||0;
    const kb = (knowledgeText(d)||"").slice(0,140);
    const mm = DB.deptMemory[d]||[]; const last = mm.slice(-1)[0];
    const lastTxt = last ? String(last.instruction||last.note||"").slice(0,50) : "(활동 없음)";
    const c = cap[d];
    const score = c ? ("역량 "+c.overall+"(지식"+c.knowledge+"/수행"+c.execution+"/품질"+c.quality+")") : "역량 미평가";
    const r = rev[d];
    const yreview = r ? ("\n  어제 회고: 등급 "+r.grade+(r.tomorrow?(" · 내일방향: "+r.tomorrow):"")+(r.improve?(" · 보완: "+String(r.improve).slice(0,60)):"")) : "";
    const fb = (DB.dirFeedback&&DB.dirFeedback[d]||[]).filter(x=>!x.used).slice(-3);
    const fbTxt = fb.length ? ("\n  클라이언트 피드백(반드시 반영): "+fb.map(x=>x.text).join(" / ")) : "";
    const research = (nr[d]&&nr[d].text) ? ("\n  밤새 연구: "+String(nr[d].text).slice(0,160)) : "";
    return "■ "+AGENTS[d].kr+"("+MEMBERS[d]+") Lv"+lv+" · 경험"+exp+" · "+score+"\n  최근: "+lastTxt+"\n  전문성: "+(kb||"(적음)")+yreview+fbTxt+research;
  }).join("\n");
  const sys = "너는 이 SNS 자동화 회사 팀장 '08 "+AGENTS.ops.kr+"("+MEMBERS["ops"]+")'이며 최고 지능이다."+ADDRESS+clientBlock()
    + " 오늘 하루 각 부서가 스스로 수행할 '구체적이고 실행 가능한' 자율수행 과제를 하나씩 내려라. 막연한 지시(예: '열심히 하세요', '트렌드를 학습하세요') 금지 — 그 부서 역할과 현재 역량(약한 항목)에 맞춰, 오늘 바로 결과물이 나올 수 있는 실무 지시로. **어제 회고의 '내일방향·보완점'과 네가 밤새 조사한 '밤새 연구' 자료를 반드시 이어받아 구체화하라**(연구한 트렌드·아이디어를 지시에 녹여라). 역량이 낮거나 최근 활동이 없는 부서는 우선 기초를 다지는 과제를, 역량이 높은 부서는 더 심화된 과제를 내려라.\n"
    + "반드시 아래 형식의 줄만 출력(설명·머리말 금지):\nDIRECTIVE: 부서영문키 = 오늘의 지시(한국어 1문장, 25~60자)\n대상 부서영문키: "+depts.join(", ");
  let out;
  try{ out = await genText(sys, "[부서 현황]\n"+board, 900, "gemini"); } // 무료
  catch(e){ logError("assign-daily-directives", e); return null; }
  DB.leaderDailyDirective = DB.leaderDailyDirective || {};
  const day = kstDay(), now = Date.now();
  let n=0; const assigned=[];
  String(out).replace(/DIRECTIVE:\s*([a-z]+)\s*=\s*(.+)/g,(mm,d,txt)=>{
    d=d.trim();
    if(AGENTS[d] && d!=="ops"){
      const clean = String(txt||"").trim().replace(/^["'“]|["'”]$/g,"").slice(0,140);
      if(clean){
        DB.leaderDailyDirective[d] = { text:clean, at:now, day };
        DB.leadDirectives = DB.leadDirectives||[];
        DB.leadDirectives.push({ dept:d, directive:clean, reason:"일일 자율수행 지시(오세라)", at:now });
        if(DB.leadDirectives.length>60) DB.leadDirectives=DB.leadDirectives.slice(-60);
        assigned.push({ kr:(AGENTS[d]?AGENTS[d].kr:d), task:clean });
        n++;
      }
    }
    return mm;
  });
  DB.lastLeaderDirectDay = day;
  if(DB.dirFeedback){ Object.keys(DB.dirFeedback).forEach(function(dd){ (DB.dirFeedback[dd]||[]).forEach(function(x){ x.used=true; }); }); }
  saveDB();
  if(n>0){
    const lines = assigned.map(x=>"• "+x.kr+": "+x.task).join("\n");
    kakaoNotify("🧭 오늘의 팀장 지시 배정 완료 — "+n+"개 부서에 오늘의 자율수행 과제를 내렸어요\n\n"+lines).catch(()=>{});
  }
  return DB.leaderDailyDirective;
}
app.post("/api/ops/daily-directives", async (req,res)=>{
  try{ const r = await assignDailyDirectives(); res.json({ ok:!!r, leaderDailyDirective: DB.leaderDailyDirective||{} }); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 클라이언트가 팀장 지시에 피드백 → 팀장이 학습해두고 다음날 지시에 참고
app.post("/api/ops/directive-feedback", (req,res)=>{
  try{
    const b=req.body||{};
    const dept=String(b.dept||"").trim();
    const text=String(b.text||"").trim();
    if(!AGENTS[dept]||dept==="ops") return res.status(400).json({ error:"부서를 확인하세요" });
    if(!text) return res.status(400).json({ error:"피드백 내용을 입력하세요" });
    DB.dirFeedback = DB.dirFeedback || {};
    DB.dirFeedback[dept] = DB.dirFeedback[dept] || [];
    DB.dirFeedback[dept].push({ text:text.slice(0,300), at:Date.now(), day:kstDay(), used:false });
    if(DB.dirFeedback[dept].length>10) DB.dirFeedback[dept]=DB.dirFeedback[dept].slice(-10);
    // 부서 메모리에도 남겨 다음 학습·distill에 반영
    DB.deptMemory[dept]=DB.deptMemory[dept]||[];
    DB.deptMemory[dept].push({ at:Date.now(), instruction:"[클라이언트 피드백]", note:text.slice(0,300) });
    if(DB.deptMemory[dept].length>40) DB.deptMemory[dept]=DB.deptMemory[dept].slice(-40);
    saveDB();
    res.json({ ok:true, dirFeedback: DB.dirFeedback });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

async function runDailyGrowth(){
  const depts=Object.keys(AGENTS).filter(d=>d!=="ops");
  // 뒤처진 부서는 한 번에 여러 라운드(따라잡기), 앞선 부서는 1라운드 — 격차를 매일 좁힘
  const planRounds={}; let totalRounds=0;
  depts.forEach(d=>{ const rr=catchUpRounds(d); planRounds[d]=rr; totalRounds+=rr; });
  DB.growBurst={ active:true, total:totalRounds+2, done:0, mode:"daily", startedAt:Date.now(), finishedAt:0 };
  saveDB();
  const _trainedWhat = {};
  for(const d of depts){
    if(!(DB.deptMemory[d]||[]).length){ DB.deptMemory[d]=[{ at:Date.now(), instruction:"[시작]", note:AGENTS[d].kr+" 부서 가동 시작" }]; }
    const rr=planRounds[d]||1;
    const lagging = rr>1; // 뒤처진 부서
    for(let k=0;k<rr;k++){
      try{ const _r=await runSelfTraining(d); if(_r){ const _t=String(_r.transcript||""); const _m=_t.match(/훈련\s*주제[:\s]*([^\n]+)/); _trainedWhat[d] = _m ? _m[1].trim().slice(0,80) : ((_r.principles&&_r.principles[0])?String(_r.principles[0]).slice(0,80):(_trainedWhat[d]||"")); } }catch(e){ logError("daily:"+d, e); } // 자기주도 훈련 +1 + distill
      DB.growBurst.done++; saveDB(); await new Promise(r=>setTimeout(r,700));
    }
    // 뒤처진 부서는 훈련 후 '심화 지식 정리'를 강제로 한 번 더 → 레벨뿐 아니라 실제 전문성·지능이 도약
    if(lagging){ try{ await distillKnowledge(d, true); await leaderAbsorb(d); }catch(e){ logError("daily-deep:"+d, e); } }
  }
  try{ await studyWriterCraft(); }catch(e){ logError("daily:study-writer", e); } // 작가 블로그·대본 심화 학습
  try{ await runSelfTraining("ops"); }catch(e){ logError("daily:ops", e); }   // 팀장 리더십 훈련
  DB.growBurst.done++; saveDB();
  try{ await distillLeaderKnowledge(); }catch(e){}                              // 팀장 통합 지능 갱신
  DB.growBurst.done++;
  ensureLeaderLead();
  try{ await assessCapability(); }catch(e){} // 매일 역량 평가 갱신
  try{ await assignDailyDirectives(); }catch(e){ logError("daily-growth:assign", e); } // 팀장이 매일 각 부서에 구체 지시 배정
  DB.lastDailyGrowthDay=kstDay();
  DB.growBurst.active=false; DB.growBurst.finishedAt=Date.now(); saveDB();
  try{
    const behindList=depts.filter(d=>planRounds[d]>1);
    const behind=behindList.length;
    if(behind>0){
      const lines=behindList.map(d=>"• "+(AGENTS[d]?AGENTS[d].kr:d)+" ("+planRounds[d]+"라운드)"+(_trainedWhat[d]?(": "+_trainedWhat[d]):"")).join("\n");
      kakaoNotify("📅 매일 자동 성장 완료 — 뒤처진 "+behind+"개 부서를 집중 훈련해 격차를 좁혔어요(무료)\n\n"+lines+"\n\n↑ 약한 부서를 더 돌려 전 부서 수준을 끌어올렸어요.");
    } else {
      kakaoNotify("📅 매일 자동 성장 완료 — 전 부서가 고르게 성장 중이라 뒤처진 부서 없이 균등 훈련했어요(무료).");
    }
  }catch(_){}
}

// ===== 팀장이 각 부서 경험을 점검하고 '보완'(부족 지식을 직접 보태 개선) =====
async function runLeaderReview(dept){
  const a=AGENTS[dept]; if(!a || dept==="ops") return null;
  const deptKb=knowledgeText(dept);
  const recent=(DB.deptMemory[dept]||[]).slice(-6).map(x=>"· "+(x.instruction||"")+" → "+String(x.note||"").slice(0,120)).join("\n");
  const leadKb=knowledgeText("ops"); // 팀장 통합 지능을 근거로 보완
  const sys="너는 이 회사 팀장 '08 "+AGENTS.ops.kr+"("+MEMBERS["ops"]+")'이며 최고 지능이다."+ADDRESS+STYLE+clientBlock()
    +" 너는 '"+a.kr+"("+MEMBERS[dept]+")' 부서(역할: "+a.role+")의 경험과 전문성을 점검하고, 부족한 부분을 네 통합 지능으로 '직접 보완'한다. 평가만 하지 말고, 그 부서가 당장 흡수해 더 잘하게 될 '실전 보완 지식'을 네가 만들어 줘라. 한국어로 아래 형식만 출력:\n"
    +"점검: (이 부서가 놓치고 있거나 약한 점 1~2가지, 근거와 함께)\n"
    +"보완 지식: (그 약점을 메우는 구체적·재사용 가능한 전문 지식·노하우 2~4개. 이 부서가 바로 적용할 수 있게 또렷하게. 줄마다 '- ')\n"
    +"다음 지시: (이 부서가 다음에 할 자율수행 한 줄)\n군더더기·서론 금지.";
  const ctx="[팀장 통합 지능(보완의 근거)]\n"+(leadKb||"(아직 적음)")+"\n\n[점검 대상 부서 전문성]\n"+(deptKb||"(아직 적음)")+"\n\n[그 부서 최근 경험]\n"+(recent||"(없음)");
  const out=await genText(sys, ctx, 1100, "gemini"); // 무료
  // 보완 지식을 부서 메모리에 주입 → distill로 전문성에 흡수 → 실제로 개선됨
  if(!DB.deptMemory[dept]) DB.deptMemory[dept]=[];
  DB.deptMemory[dept].push({ at:Date.now(), instruction:"[팀장 보완]", note:String(out).slice(0,700) });
  if(DB.deptMemory[dept].length>40) DB.deptMemory[dept]=DB.deptMemory[dept].slice(-40);
  DB.exp=DB.exp||{}; DB.exp[dept]=(DB.exp[dept]||0)+1; // 팀장 보완으로 부서가 한 단계 배움
  try{ await distillKnowledge(dept, true); }catch(e){} // 보완 즉시 전문성에 반영(심화)
  ensureLeaderLead(); saveDB();
  return { dept, name:a.kr, review:out };
}
async function runLeaderAudit(){
  const depts=Object.keys(AGENTS).filter(d=>d!=="ops");
  DB.growBurst={ active:true, total:depts.length+1, done:0, mode:"audit", startedAt:Date.now(), finishedAt:0 };
  saveDB();
  // 먼저 팀장 통합 지능을 최신화(정확한 보완을 위해)
  try{ await distillLeaderKnowledge(); }catch(e){}
  DB.growBurst.done++; saveDB();
  for(const d of depts){
    try{ await runLeaderReview(d); }catch(e){ logError("audit:"+d, e); }
    DB.growBurst.done++; saveDB(); await new Promise(r=>setTimeout(r,700));
  }
  DB.growBurst.active=false; DB.growBurst.finishedAt=Date.now(); saveDB();
  try{ kakaoNotify("🔎 팀장 점검·보완 완료 — 전 부서의 약점을 팀장이 통합 지능으로 보완했어요(무료)"); }catch(_){}
}
app.post("/api/ops/audit", (req,res)=>{
  try{
    if (DB.growBurst && DB.growBurst.active) return res.json({ ok:true, already:true, growBurst:DB.growBurst });
    const b=req.body||{};
    if (b.dept && AGENTS[b.dept] && b.dept!=="ops"){
      return runLeaderReview(b.dept).then(r=>res.json({ ok:true, reviewed:r?[r.name]:[], detail:r })).catch(e=>res.status(500).json({ error:String(e.message||e) }));
    }
    runLeaderAudit().catch(e=>logError("leader-audit", e)); // 백그라운드
    res.json({ ok:true, started:true, growBurst:DB.growBurst });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// ===== 부서 역량(능력치): 객관 지표 + 팀장의 0~100 평가(지식/수행력/품질) =====
function capabilityMetrics(){
  const all=Object.keys(AGENTS);
  const m={};
  all.forEach(d=>{
    const kb=knowledgeText(d)||"";
    const mem=DB.deptMemory[d]||[];
    const principles=(kb.match(/(원칙|PRINCIPLE|노하우|핵심)/g)||[]).length;
    m[d]={
      level: deptLevel(d),
      exp: (DB.exp||{})[d]||0,
      knowledgeChars: kb.length,
      principles,
      activities: mem.length,
      lastActiveAt: (mem.slice(-1)[0]||{}).at || 0,
      isLeader: d==="ops"
    };
  });
  return m;
}
async function assessCapability(){
  const depts=Object.keys(AGENTS).filter(d=>d!=="ops");
  const board=depts.map(d=>"■ "+AGENTS[d].kr+"("+MEMBERS[d]+") Lv"+deptLevel(d)+" · 경험"+(((DB.exp||{})[d])||0)+" · 학습"+((DB.deptMemory[d]||[]).length)+"건\n  전문성: "+((knowledgeText(d)||"(적음)").slice(0,160))).join("\n");
  const sys="너는 팀장 '08 "+AGENTS.ops.kr+"("+MEMBERS["ops"]+")'이며 최고 지능이다."+ADDRESS
    +" 각 부서의 실제 역량을 네 통합 지능 기준으로 냉정하게 0~100점으로 평가하라(후하지 말 것). 세 항목: 지식(전문성 깊이)/수행력(자율로 일을 끝내는 능력)/품질(결과물 수준). "
    +"출력은 줄마다 정확히 이 형식만: 'SCORE: 부서영문키 = 지식/수행력/품질' (각 0~100 정수, 슬래시 구분). 부서영문키: "+depts.join(", ")+". 설명 금지.";
  const out=await genText(sys, "[부서 현황]\n"+board, 700, "gemini"); // 무료
  DB.capability=DB.capability||{};
  out.replace(/SCORE:\s*([a-z]+)\s*=\s*(\d{1,3})\s*\/\s*(\d{1,3})\s*\/\s*(\d{1,3})/g,(mm,d,k,e,q)=>{
    if(AGENTS[d]&&d!=="ops"){ const K=Math.min(100,+k),E=Math.min(100,+e),Q=Math.min(100,+q);
      DB.capability[d]={ knowledge:K, execution:E, quality:Q, overall:Math.round((K+E+Q)/3), at:Date.now() }; }
    return mm;
  });
  // 팀장은 부서 최고 + 여유로 산정(항상 압도적)
  const ds=depts.map(d=>DB.capability[d]).filter(Boolean);
  if(ds.length){
    const top=k=>Math.max.apply(null,ds.map(x=>x[k]||0));
    DB.capability.ops={ knowledge:Math.min(100,top("knowledge")+5), execution:Math.min(100,top("execution")+5), quality:Math.min(100,top("quality")+5), overall:Math.min(100, Math.round((Math.min(100,top("knowledge")+5)+Math.min(100,top("execution")+5)+Math.min(100,top("quality")+5))/3)), at:Date.now(), isLeader:true };
  }
  // 일간/주간/누적 비교용 스냅샷 기록 (부서별 종합 + 팀 평균)
  try{
    const dd=depts.map(d=>DB.capability[d]).filter(Boolean);
    if(dd.length){
      const avg=Math.round(dd.reduce((s,x)=>s+(x.overall||0),0)/dd.length);
      const per={}; depts.forEach(d=>{ if(DB.capability[d]) per[d]=DB.capability[d].overall; });
      DB.capHistory=DB.capHistory||[];
      DB.capHistory.push({ at:Date.now(), day:kstDay(), avg, per });
      if(DB.capHistory.length>400) DB.capHistory=DB.capHistory.slice(-400);
    }
  }catch(e){}
  saveDB();
  return DB.capability;
}
app.post("/api/ops/capability", async (req,res)=>{
  try{
    if ((req.body||{}).assess){ await assessCapability(); }
    res.json({ ok:true, metrics: capabilityMetrics(), capability: DB.capability||{} });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// 회의 기록 삭제: { id } 또는 { ids:[...] }
app.post("/api/meeting/delete", (req,res)=>{
  const b = req.body||{};
  const ids = (b.ids && b.ids.length) ? b.ids.map(Number) : (b.id!==undefined ? [Number(b.id)] : []);
  if(!ids.length) return res.status(400).json({ error:"id 필요" });
  const before = (DB.meetings||[]).length;
  DB.meetings = (DB.meetings||[]).filter(m=> ids.indexOf(m.id)<0 );
  saveDB();
  res.json({ ok:true, removed: before-(DB.meetings||[]).length });
});

// 회의 실행: { topic, depts[], rounds, room } → meeting
app.post("/api/meeting", (req,res)=>{
  try { const b = req.body||{};
    if ((!b.topic && !(b.agenda&&b.agenda.length)) || !b.depts || !b.depts.length) return res.status(400).json({ error:"topic/agenda, depts 필요" });
    const meeting = createMeeting({ topic:b.topic, depts:b.depts, rounds:b.rounds, mode:b.mode, chair:b.chair, agenda:b.agenda, clientNote:b.clientNote, prevSummary:b.prevSummary, room:b.room, alwaysJoin:b.alwaysJoin, engine:(b.engine || (DB.state&&DB.state.meetingEngine) || "claude"), source:"app" });
    processMeeting(meeting).catch(()=>{}); // 비동기 진행 — 발언은 실시간 저장됨
    res.json({ ok:true, id:meeting.id, status:"running" }); // 즉시 응답 (타임아웃 방지)
  } catch(e){ res.status(400).json({ error:String(e.message||e) }); }
});
// 회의 단건 조회 (실시간 진행 상황 폴링용)
app.get("/api/meeting/:id", (req,res)=>{
  const m=(DB.meetings||[]).find(x=>x.id===+req.params.id);
  if(!m) return res.status(404).json({ error:"회의 없음" });
  res.json(m);
});
// 예약 회의 목록/등록/삭제: time "HH:MM"(KST), repeat "daily"|"once"
app.get("/api/meeting-schedules", (req,res)=> res.json(DB.meetingSchedules||[]));
// ===== 프로젝트 엔드포인트 =====
app.get("/api/projects", (req,res)=>{ res.json((DB.projects||[]).filter(p=>p.status!=="deleted")); });
app.post("/api/projects/create", (req,res)=>{
  try{ const p=createProject(req.body||{}); res.json({ ok:true, project:p }); }
  catch(e){ res.status(400).json({ error:String(e.message||e) }); }
});
app.post("/api/projects/run", async (req,res)=>{
  try{ const p=projectById((req.body||{}).id); if(!p) return res.status(404).json({ error:"프로젝트 없음" });
    if(p.status!=="active") return res.status(400).json({ error:"진행 중인 프로젝트가 아니에요" });
    const r=await runProjectCycle(p); res.json({ ok:!!r, project:p }); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.post("/api/projects/note", (req,res)=>{
  try{ const b=req.body||{}; const p=projectById(b.id); if(!p) return res.status(404).json({ error:"프로젝트 없음" });
    const t=String(b.text||"").trim(); if(!t) return res.status(400).json({ error:"의견을 입력하세요" });
    p.clientNotes=p.clientNotes||[]; p.clientNotes.push({ at:Date.now(), text:t.slice(0,500), used:false });
    // 확인 대기 중이던 프로젝트는 의견을 받으면 다시 진행
    if(p.status==="awaiting"){ p.status="active"; p.holdReason=""; p.nextAt=Date.now(); }
    saveDB();
    res.json({ ok:true, project:p }); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 즉시 지시: 의견을 남기고 다음 사이클을 기다리지 않고 지금 바로 1회 사이클 실행
app.post("/api/projects/note-run", async (req,res)=>{
  try{ const b=req.body||{}; const p=projectById(b.id); if(!p) return res.status(404).json({ error:"프로젝트 없음" });
    const t=String(b.text||"").trim();
    if(t){ p.clientNotes=p.clientNotes||[]; p.clientNotes.push({ at:Date.now(), text:t.slice(0,500), used:false }); }
    if(p.status==="awaiting"){ p.status="active"; p.holdReason=""; }
    if(p.status!=="active"){ p.status="active"; }
    saveDB();
    const r=await runProjectCycle(p); // 지금 즉시 한 사이클
    res.json({ ok:!!r, project:p }); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 토론: 팀장에게 질문/의견을 보내면 즉답을 받고 프로젝트 맥락에 대화로 남김
app.post("/api/projects/discuss", async (req,res)=>{
  try{ const b=req.body||{}; const p=projectById(b.id); if(!p) return res.status(404).json({ error:"프로젝트 없음" });
    const t=String(b.text||"").trim(); if(!t) return res.status(400).json({ error:"내용을 입력하세요" });
    if(!geminiAutoAllowed()) return res.status(400).json({ error:"지금은 자동 지능작업 예산이 소진돼 잠시 후 가능해요" });
    p.discuss=p.discuss||[];
    p.discuss.push({ at:Date.now(), role:"client", text:t.slice(0,600) });
    const recent = (p.log||[]).slice(-4).map(x=>"· C"+x.cycle+" ["+(AGENTS[x.dept]?AGENTS[x.dept].kr:x.dept)+"] "+String(x.work||"").slice(0,100)).join("\n") || "(아직 진행 없음)";
    const chat = p.discuss.slice(-8).map(m=>(m.role==="client"?"클라이언트: ":"팀장: ")+m.text).join("\n");
    const sys="너는 이 회사 팀장 '"+AGENTS.ops.kr+"("+MEMBERS["ops"]+")'이며 최고 지능이다."+ADDRESS+clientBlock()
      +" 아래 프로젝트에 대해 클라이언트와 토론 중이다. 클라이언트의 마지막 말에 대해 팀장으로서 구체적이고 실질적으로 답하라. 방향 제시·근거·다음 액션을 명확히. 되묻기만 하지 말고 네 판단을 제시하되, 클라이언트 결정이 필요한 지점은 짚어라. 한국어로 3~6문장."
      +"\n프로젝트: "+p.title+"\n목표: "+p.goal+"\n최근 진행:\n"+recent+"\n\n[대화]\n"+chat;
    let reply="";
    try{ reply=await genText(sys, "팀장으로서 답하라.", 600, "gemini"); }catch(e){ logError("project-discuss:"+p.id, e); }
    if(!reply) reply="(지금 답변을 생성하지 못했어요. 잠시 후 다시 시도해 주세요.)";
    p.discuss.push({ at:Date.now(), role:"lead", text:String(reply).slice(0,900) });
    if(p.discuss.length>40) p.discuss=p.discuss.slice(-40);
    saveDB();
    res.json({ ok:true, reply:String(reply), project:p }); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.post("/api/projects/interval", (req,res)=>{
  try{ const b=req.body||{}; const p=projectById(b.id); if(!p) return res.status(404).json({ error:"프로젝트 없음" });
    if([60,120,180,360,720,1440].indexOf(+b.intervalMin)<0) return res.status(400).json({ error:"허용된 간격이 아니에요(1/2/3/6/12/24시간)" });
    p.intervalMin=+b.intervalMin; p.nextAt=Date.now()+p.intervalMin*60000; saveDB();
    res.json({ ok:true, project:p }); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.post("/api/projects/status", (req,res)=>{
  try{ const b=req.body||{}; const p=projectById(b.id); if(!p) return res.status(404).json({ error:"프로젝트 없음" });
    const s=String(b.status||""); if(["active","paused","archived","deleted"].indexOf(s)<0) return res.status(400).json({ error:"상태값 오류" });
    if(s==="active"){ const act=(DB.projects||[]).filter(x=>x.status==="active"&&x.id!==p.id); if(act.length>=2) return res.status(400).json({ error:"동시 진행은 최대 2개" }); p.nextAt=Date.now(); }
    p.status=s; saveDB(); res.json({ ok:true, project:p }); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 🏢 프로젝트 회의 — 지금 실행(백그라운드). 앱을 나가도 진행되고 완료 시 카톡 알림.
app.post("/api/projects/meeting", (req,res)=>{
  try{
    const b=req.body||{};
    const topic=String(b.topic||"").trim();
    if(!topic) return res.status(400).json({ error:"회의 주제가 필요해요" });
    const depts=(Array.isArray(b.depts)?b.depts:[]).filter(d=>AGENTS[d]);
    const opts={ topic, depts, projectId:b.projectId||null, rounds:+b.rounds||2, mode:b.mode||"discuss", promote:b.promote!==false, variants:b.variants, judgeMode:b.judgeMode, appUrl:b.appUrl||"" };
    res.json({ ok:true, status:"started", topic });
    setImmediate(()=>{ runProjectMeeting(opts).catch(e=>logError("projectMeeting", e)); });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 회의의 후보 안(원문) 전체 조회 — 나란히 비교용
app.get("/api/project-meetings/candidates", (req,res)=>{
  const pm=(DB.projectMeetings||[]).find(x=>String(x.id)===String(req.query&&req.query.id));
  if(!pm) return res.status(404).json({ error:"회의 없음" });
  res.json({ ok:true, topic:pm.topic, winnerAngle:pm.winnerAngle||"", candidates:(pm.candidates||[]).map((c,i)=>({ index:i, angle:c.angle, text:c.text })) });
});
// 📄 결과물을 '다른 프로그램에서 열 수 있는 링크'로 제공(HTML 페이지) + 파일 다운로드(md/txt)
app.get("/api/project-meetings/view", (req,res)=>{
  const pm=(DB.projectMeetings||[]).find(x=>String(x.id)===String(req.query&&req.query.id));
  if(!pm) return res.status(404).set("Content-Type","text/html; charset=utf-8").send("<!doctype html><meta charset=utf-8><p style='font-family:sans-serif'>회의를 찾을 수 없어요.</p>");
  // 열람 토큰 검사(토큰이 있는 회의만 요구 — 구버전 호환)
  if(pm.token && String(req.query.t||"")!==String(pm.token)) return res.status(403).set("Content-Type","text/html; charset=utf-8").send("<!doctype html><meta charset=utf-8><p style='font-family:sans-serif'>열람 권한이 없어요(링크가 올바른지 확인).</p>");
  const meta=(pm.passed?'✅ 검수 통과':'📝 초안')+(pm.winnerAngle?(' · '+pm.winnerAngle):'');
  const dlBase="?id="+pm.id+(pm.token?("&t="+pm.token):"");
  sendDoc(res, req, pm.topic||"결과물", pm.deliverable||pm.summary||"", meta, dlBase);
});
// 📮 발행함 글도 링크/파일로 (모바일에서 다른 앱으로 열기·저장)
app.get("/api/inbox/view", (req,res)=>{
  const it=(DB.pubInbox||[]).find(x=>String(x.id)===String(req.query&&req.query.id));
  if(!it) return res.status(404).set("Content-Type","text/html; charset=utf-8").send("<!doctype html><meta charset=utf-8><p style='font-family:sans-serif'>글을 찾을 수 없어요.</p>");
  if(it.token && String(req.query.t||"")!==String(it.token)) return res.status(403).set("Content-Type","text/html; charset=utf-8").send("<!doctype html><meta charset=utf-8><p style='font-family:sans-serif'>열람 권한이 없어요.</p>");
  const tags=(Array.isArray(it.tags)?it.tags:[]).map(t=>t[0]==="#"?t:("#"+t)).join(" ");
  const raw=(it.title?("# "+it.title+"\n\n"):"")+(it.body||"")+(it.meta?("\n\n---\n📝 메타: "+it.meta):"")+(tags?("\n\n"+tags):"");
  const dlBase="?id="+it.id+(it.token?("&t="+it.token):"");
  sendDoc(res, req, it.title||"발행글", raw, (it.channelName||it.channel||"블로그"), dlBase);
});
// 사람이 특정 안을 직접 채택(판사 결정 오버라이드) → 그 안을 결과물로
app.post("/api/projects/meeting/pick", (req,res)=>{
  try{
    const b=req.body||{};
    const pm=(DB.projectMeetings||[]).find(x=>String(x.id)===String(b.id));
    if(!pm) return res.status(404).json({ error:"회의 없음" });
    const idx=+b.index; const c=(pm.candidates||[])[idx];
    if(!c || !c.text) return res.status(400).json({ error:"해당 안을 찾을 수 없어요" });
    pm.deliverable = String(c.text).trim(); pm.winnerAngle = c.angle+" (직접 채택)"; pm.winnerReason = "클라이언트가 직접 선택"; pm.pickedByUser = true; pm.passed = true;
    saveDB();
    res.json({ ok:true, winnerAngle:pm.winnerAngle });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.get("/api/project-meetings", (req,res)=>{
  const pid = req.query && req.query.projectId;
  let arr = (DB.projectMeetings||[]);
  if(pid) arr = arr.filter(x=>String(x.projectId||"")===String(pid));
  res.json({ ok:true, meetings: arr.slice(-20).map(x=>({ id:x.id, token:x.token||"", topic:x.topic, depts:x.depts, status:x.status, phase:x.phase, samples:(x.samples||[]).length, candidates:(x.candidates||[]).length, winnerAngle:x.winnerAngle||"", winnerReason:x.winnerReason||"", passed:x.passed, deliverable:x.deliverable, summary:x.summary, promotedTo:x.promotedTo, projectId:x.projectId, at:x.at, doneAt:x.doneAt })) });
});
// 회의 결과물을 발행함(pubInbox)으로 보내 바로 게시 대기로 만든다
app.post("/api/projects/meeting/publish", (req,res)=>{
  try{
    const b=req.body||{};
    const pm=(DB.projectMeetings||[]).find(x=>String(x.id)===String(b.id));
    if(!pm) return res.status(404).json({ error:"회의를 찾을 수 없어요" });
    if(!pm.deliverable) return res.status(400).json({ error:"아직 결과물이 없어요" });
    const it = addToInbox({ channel:b.channel||"tistory", title:pm.topic, body:pm.deliverable, topic:pm.topic });
    saveDB();
    res.json({ ok:true, inboxId:it&&it.id, channel:(it&&it.channelName)||"블로그" });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.post("/api/meeting-schedule", (req,res)=>{
  const b = req.body||{};
  const { topic, depts, rounds, time, room } = b;
  const kind = (b.kind==="project") ? "project" : "meeting";
  const repeat = b.repeat || "daily"; // once | daily | weekly | interval
  if (!topic) return res.status(400).json({ error:"topic 필요" });
  if (kind!=="project" && (!depts || !depts.length)) return res.status(400).json({ error:"topic, depts 필요" });
  // interval(주기)이 아니면 시각(HH:MM) 필요
  if (repeat !== "interval" && !/^\d{2}:\d{2}$/.test(time||"")) return res.status(400).json({ error:"time(HH:MM) 필요" });
  DB.meetingSchedules = DB.meetingSchedules||[];
  if (DB.meetingSchedules.length >= 20) return res.status(400).json({ error:"예약은 최대 20개" });
  let days = Array.isArray(b.days) ? b.days.map(Number).filter(n=>n>=0&&n<=6) : [];
  if (repeat === "weekly" && !days.length) days = [kstDow()];
  const everyN = Math.max(1, +b.everyN || 1);          // 주기 값 (N)
  const everyUnit = (b.everyUnit === "day") ? "day" : "hour"; // 시간 단위
  const it = {
    id:Date.now()+Math.floor(Math.random()*1000),
    kind, projectId:b.projectId||null, promote:b.promote!==false, variants:(b.variants!=null?+b.variants:undefined), judgeMode:(b.judgeMode==="synth"?"synth":undefined),
    topic, depts:(depts||[]).filter(d=>AGENTS[d]), rounds:+rounds||2,
    mode:b.mode||"discuss", chair:b.chair||"", agenda:b.agenda||[], clientNote:b.clientNote||"",
    time: time||"", repeat, days, everyN, everyUnit, room:room||"",
    lastRunDay:"", nextAt: repeat==="interval" ? (Date.now() + everyN*(everyUnit==="day"?86400000:3600000)) : 0,
    continueLast: !!b.continueLast // 이어가기: 직전 회의 결론을 맥락으로
  };
  DB.meetingSchedules.push(it); saveDB();
  res.json({ ok:true, schedule:it });
});
app.post("/api/meeting-schedule/delete", (req,res)=>{
  const { id } = req.body||{};
  DB.meetingSchedules = (DB.meetingSchedules||[]).filter(x=>x.id!==+id);
  saveDB(); res.json({ ok:true });
});
// ===== 플랫폼 개발실 (오세라 패치 시스템) =====
const APP_ENV_DOC = `이 플랫폼 앱의 환경:
- 바닐라 JS 단일 HTML 앱(PWA). 모든 최상위 함수·변수는 전역(window)이다.
- 핵심 전역: STATE(앱 상태 객체), AGENTS(부서 정의: {strategy,creation,publishing,engagement,analytics,monetization,growth,ops,advisory,scout} 각 {no,kr,role}), DEPT_MEMBERS(부서별 담당자 이름), viewEl(메인 뷰 컨테이너), render()(현재 탭 다시 그림), saveState()(상태 저장), escapeHtml(s), avatar(id,size)(부서 아바타 HTML), logAct(kind,label,ok,note)(활동기록), backendOrigin()(백엔드 주소), useBackend()(백엔드 연결 여부), nowTime().
- 탭(STATE.view): console(팀에이전트 채팅)/office(오피스)/meeting(회의)/content(콘텐츠)/results(실행 결과)/record(기록·분석)/settings(설정). 각 탭은 renderXxx()+bindXxx() 전역 함수.
- 기존 함수 변경(몽키패치): var _orig=window.renderOffice; window.renderOffice=function(){ _orig(); /*추가*/ }; 패턴 사용.
- 다크 테마. CSS 클래스: .s-wrap(섹션 컨테이너) .s-sec(섹션 제목) .note(설명) .cbtn/.cbtn.conn/.cbtn.disc(버튼) .empty(빈 안내).
- 패치 JS는 페이지 로드·초기 렌더 후 1회 실행된다. 즉시실행함수(IIFE)로 감싸고 내부를 try/catch로 보호하라. setInterval 사용 가능. DOM id 충돌을 피하기 위해 새 요소 id에는 접두사 p_ 를 붙여라.`;

// 패치 생성: { request } → 오세라가 JS/CSS 패치 생성
app.post("/api/dev/build", async (req,res)=>{
  try {
    const { request } = req.body||{};
    if (!request || !String(request).trim()) return res.status(400).json({ error:"request 필요" });
    DB.patches = DB.patches||[];
    if (DB.patches.length >= 20) return res.status(400).json({ error:"패치는 최대 20개입니다. 안 쓰는 패치를 삭제하세요." });
    const sys = "너는 이 SNS 자동화 플랫폼의 팀장 '08 플랫폼 운영(오세라)' AI다."+ADDRESS
      + " 클라이언트님이 요청한 기능을 플랫폼에 추가/변경하는 패치를 직접 개발하라.\n\n"+APP_ENV_DOC
      + "\n\n출력 규칙: 반드시 아래 JSON 하나만 출력한다. 마크다운 펜스·설명 금지.\n"
      + '{"title":"패치 이름(짧게)","desc":"무엇이 어떻게 바뀌는지 1~2문장","js":"패치 자바스크립트(IIFE, 내부 try/catch)","css":"필요한 CSS(없으면 빈 문자열)"}'
      + "\n주의: js는 기존 데이터(STATE)를 파괴하지 않아야 하고, 실패해도 앱이 죽지 않게 모든 본문을 try/catch로 감싼다. 외부 네트워크 호출은 backendOrigin() 경유만 허용.";
    const out = await anthropic(sys, "기능 요청: "+request, 4000);
    let p;
    try { p = JSON.parse(out.replace(/^```(json)?/m,"").replace(/```\s*$/m,"").trim()); }
    catch(e){ return res.status(500).json({ error:"패치 생성 결과를 해석하지 못했어요. 요청을 더 구체적으로 적어 다시 시도하세요." }); }
    if (!p.js || typeof p.js !== "string") return res.status(500).json({ error:"패치에 실행 코드가 없습니다. 다시 시도하세요." });
    try { new Function(p.js); } catch(e){ return res.status(500).json({ error:"생성된 코드에 문법 오류가 있어 적용하지 않았어요: "+e.message+" — 다시 시도하세요." }); }
    if (p.js.length > 60000) return res.status(500).json({ error:"패치가 너무 큽니다. 기능을 나눠 요청하세요." });
    const patch = { id:Date.now(), title:String(p.title||"이름 없는 패치").slice(0,60), desc:String(p.desc||"").slice(0,300), js:p.js, css:String(p.css||"").slice(0,20000), enabled:true, at:Date.now(), request:String(request).slice(0,300) };
    DB.patches.push(patch);
    if (!DB.deptMemory.ops) DB.deptMemory.ops = [];
    DB.deptMemory.ops.push({ at:Date.now(), instruction:"[개발] "+patch.title, note:"플랫폼 패치 개발: "+patch.desc });
    if (DB.deptMemory.ops.length > 40) DB.deptMemory.ops = DB.deptMemory.ops.slice(-40);
    DB.exp = DB.exp||{}; DB.exp.ops = (DB.exp.ops||0)+1;
    saveDB();
    kakaoNotify("🛠 오세라 패치 개발 완료: "+patch.title).catch(()=>{});
    res.json({ ok:true, patch:{ id:patch.id, title:patch.title, desc:patch.desc, enabled:true, at:patch.at } });
  } catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.get("/api/dev/patches", (req,res)=>{
  const full = req.query.full === "1";
  res.json((DB.patches||[]).map(p=> full ? p : ({ id:p.id, title:p.title, desc:p.desc, enabled:p.enabled, at:p.at })));
});
app.post("/api/dev/patch/toggle", (req,res)=>{
  const { id, enabled } = req.body||{};
  const p=(DB.patches||[]).find(x=>x.id===+id);
  if(!p) return res.status(404).json({ error:"패치 없음" });
  p.enabled = !!enabled; saveDB(); res.json({ ok:true });
});
app.post("/api/dev/patch/delete", (req,res)=>{
  DB.patches = (DB.patches||[]).filter(x=>x.id!==+(req.body||{}).id);
  saveDB(); res.json({ ok:true });
});

// ===== 오세라 팀 평가: 부서 자율수행·학습 수준 진단 + 지시 조정 제안 =====
app.post("/api/ops/review", async (req,res)=>{
  try {
    const rows = Object.keys(AGENTS).filter(d=>d!=="ops").map(d=>{
      const mem=(DB.deptMemory[d]||[]).slice(-3).map(x=>"  · "+(x.instruction||"")+" → "+String(x.note||"").slice(0,120)).join("\n");
      const dir=(DB.state&&DB.state.deptDirective&&DB.state.deptDirective[d])||"";
      const kb=knowledgeText(d); const lv=deptLevel(d); const cnt=(DB.deptMemory[d]||[]).length;
      return AGENTS[d].no+" "+AGENTS[d].kr+"("+MEMBERS[d]+") — Lv"+lv+" · 경험치 "+((DB.exp||{})[d]||0)+"회 · 학습 "+cnt+"건"
        +(dir?" / 현재 자율지시: "+dir:" / 자율지시 없음")
        +(kb?"\n  [축적 전문성]\n  "+kb.replace(/\n/g,"\n  "):"\n  [축적 전문성] 아직 없음")
        +(mem?"\n  [최근 기록]\n"+mem:"");
    }).join("\n\n");
    const sys = "너는 이 플랫폼의 팀장 '08 플랫폼 운영(오세라)' AI다."+ADDRESS
      + " 아래 각 부서의 경험치·최근 자율수행/학습 기록·현재 지시를 보고 팀장으로서 평가하라.\n"
      + " 각 부서의 레벨·축적 전문성·학습량을 근거로 '성장도'를 진단하라.\n"
      + "출력 형식(이 형식만):\n성장 진단: (부서별 한 줄씩 '부서명 — Lv·별점(★1~5) · 강점 한마디 · 다음에 키울 점 한마디')\n총평: (가장 빠르게 성장한 부서, 정체된 부서, 팀 전체 성장 방향 2~3문장)\n"
      + "지시조정: (자율수행 지시를 바꾸면 좋을 부서만, 줄마다 'DIRECTIVE: 부서영문키 = 새 지시 한 줄'. 없으면 '없음')\n"
      + "실행제안: (지금 바로 진행하면 좋을 구체 작업 2~4개, 줄마다 'ACTION: 부서영문키 = 바로 진행할 작업 한 줄'. 팀장으로서 다음 할 일을 제안하라.)\n부서 영문키: "+Object.keys(AGENTS).join(", ")
      + profileContext();
    const out = await anthropic(sys, "[부서 현황]\n"+rows, 1800);
    const suggestions = [];
    out.replace(/DIRECTIVE:\s*([a-z]+)\s*=\s*(.+)/g, (mm,d,t)=>{ if(AGENTS[d]&&d!=="ops") suggestions.push({ dept:d, directive:t.trim() }); return mm; });
    const proposals = [];
    out.replace(/ACTION:\s*([a-z]+)\s*=\s*(.+)/g, (mm,d,t)=>{ if(AGENTS[d]) proposals.push({ dept:d, action:t.trim() }); return mm; });
    // 화면에는 raw 지시/액션 줄은 정리
    const reportClean = out.replace(/^\s*(DIRECTIVE|ACTION):.*$/gm, "").replace(/^\s*(지시조정|실행제안)\s*:\s*$/gm, "").replace(/\n{3,}/g, "\n\n").trim();
    const report = out.replace(/지시조정:[\s\S]*$/,"").trim();
    DB.collections = DB.collections||[];
    DB.collections.push({ id:Date.now(), topic:"[오세라 팀 평가]", text:out, at:Date.now(), dept:"ops" });
    if (DB.collections.length>100) DB.collections=DB.collections.slice(-100);
    if (!DB.deptMemory.ops) DB.deptMemory.ops=[];
    DB.deptMemory.ops.push({ at:Date.now(), instruction:"[팀 평가]", note:String(report).slice(0,160) });
    if (DB.deptMemory.ops.length>40) DB.deptMemory.ops=DB.deptMemory.ops.slice(-40);
    DB.exp=DB.exp||{}; DB.exp.ops=(DB.exp.ops||0)+1;
    saveDB();
    res.json({ ok:true, report:reportClean, suggestions, proposals });
  } catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// ③+⑤ 부서 간 지식 공유·성장 회의: 고레벨 부서가 가르치고, 각 부서가 서로에게서 배워 함께 성장
// 부서 간 지식 공유·성장 세션 — 서로 가르치며 함께 성장. 수동/자동 공용.
async function runKnowledgeShare(engine){
  const ids = Object.keys(AGENTS);
  const parts = ids.filter(d => knowledgeText(d) || ((DB.exp||{})[d]||0) > 0);
  if (parts.length < 2) return { ok:true, report:"아직 공유할 전문성이 부족해요. 부서들이 작업·자율수행으로 학습을 더 쌓으면 지식 공유가 가능해져요.", learnings:[] };
  const board = parts.map(d=>{
    const lv=deptLevel(d); const kb=knowledgeText(d);
    const fallback=(DB.deptMemory[d]||[]).slice(-3).map(x=>"· "+String(x.note||"").slice(0,120)).join("\n");
    return AGENTS[d].no+" "+AGENTS[d].kr+" (Lv"+lv+")\n"+(kb||fallback||"(기록 적음)");
  }).join("\n\n");
  const topLv = Math.max.apply(null, parts.map(d=>deptLevel(d)));
  const teachers = parts.filter(d=>deptLevel(d)===topLv).map(d=>AGENTS[d].kr).join(", ");
  const sys = "너는 이 플랫폼의 팀장 '08 플랫폼 운영(오세라)'이며 팀 성장 퍼실리테이터다."+ADDRESS
    + " 아래는 각 부서의 레벨과 축적 전문성이다. 지금 '부서 간 지식 공유·성장 세션'을 진행한다. "
    + "고레벨 부서(현재 최고 Lv"+topLv+": "+teachers+")가 다른 부서를 가르치는 관점으로, 각 부서가 '다른 부서의 전문성에서 배워 자기 업무에 적용할 점' 1~3가지를 구체적으로 정하라. 누구에게 배우는지 근거를 포함하라.\n"
    + "출력 형식(이 형식만):\n"
    + "줄마다 'LEARN: 부서영문키 | (어느 부서의 어떤 전문성에서) 배워 적용할 점 — 한 문장'\n"
    + "마지막에 '코칭: (최고 레벨 부서 관점에서 팀 전체에 주는 성장 조언 2~3문장)'\n"
    + "부서 영문키: " + ids.join(", ") + profileContext();
  const out = await genText(sys, "[부서 전문성 보드]\n"+board, 2000, engine||"gemini"); // 자동 = 무료(Gemini)
  const learnings = [];
  out.replace(/LEARN:\s*([a-z]+)\s*\|\s*(.+)/g, (mm,d,t)=>{
    if (AGENTS[d]) {
      const note = "[지식 공유] "+t.trim();
      if (!DB.deptMemory[d]) DB.deptMemory[d]=[];
      DB.deptMemory[d].push({ at:Date.now(), instruction:"[지식 공유 세션]", note });
      if (DB.deptMemory[d].length>40) DB.deptMemory[d]=DB.deptMemory[d].slice(-40);
      DB.exp=DB.exp||{}; DB.exp[d]=(DB.exp[d]||0)+1;
      learnings.push({ dept:d, name:AGENTS[d].kr, learn:t.trim() });
    }
    return mm;
  });
  // 공유로 새로 배운 내용을 각 부서 전문성에 즉시 반영
  for (const lg of learnings){ try{ await distillKnowledge(lg.dept); }catch(e){} }
  DB.collections = DB.collections||[];
  DB.collections.push({ id:Date.now(), topic:"[부서 간 지식 공유·성장 세션]", text:out, at:Date.now(), dept:"ops" });
  if (DB.collections.length>100) DB.collections=DB.collections.slice(-100);
  if (!DB.deptMemory.ops) DB.deptMemory.ops=[];
  DB.deptMemory.ops.push({ at:Date.now(), instruction:"[지식 공유 진행]", note:"부서 간 지식 공유 세션 진행 — "+learnings.length+"개 부서가 상호 학습" });
  DB.exp=DB.exp||{}; DB.exp.ops=(DB.exp.ops||0)+1;
  ensureLeaderLead(); DB.lastKShareAt = Date.now(); saveDB();
  return { ok:true, report:out, learnings };
}
app.post("/api/knowledge-share", async (req,res)=>{
  try { const r = await runKnowledgeShare((req.body||{}).engine); res.json(r); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// 자체 점검: 서버 상태 진단 + DB 구조 자가 보완
app.post("/api/ops/clear-errors", (req,res)=>{ try{ const n=(DB.errors||[]).length; DB.errors=[]; saveDB(); res.json({ ok:true, cleared:n }); }catch(e){ res.status(500).json({ error:String(e.message||e) }); } });
// 환율 자동 가져오기(무료 공개 API). 실패 시 현재 설정/기본값 반환.
app.get("/api/fxrate", async (req,res)=>{
  try{
    const r = await fetch("https://open.er-api.com/v6/latest/USD");
    const j = await r.json();
    const rate = j && j.rates && j.rates.KRW;
    if (rate && rate>0){ res.json({ ok:true, usdKrw: Math.round(rate) }); }
    else throw new Error("환율 데이터 없음");
  }catch(e){
    const st=DB.state||{}; res.json({ ok:false, usdKrw: +(st.usdKrw)||+(process.env.PRICE_USD_KRW||1540), error:String(e.message||e) });
  }
});
app.get("/api/selfcheck", async (req,res)=>{
  const checks = [];
  const add = (name, ok, note)=> checks.push({ name, ok:!!ok, note:note||"" });
  add("AI 엔진 키", !!API_KEY, API_KEY?("모델: "+MODEL):"ANTHROPIC_API_KEY 미설정 — 부서 응답 불가");
  let repaired = 0;
  const tmpl = emptyDB();
  for (const k of Object.keys(tmpl)){ if (DB[k] === undefined){ DB[k] = tmpl[k]; repaired++; } }
  if (repaired) saveDB();
  add("DB 구조", true, repaired? ("누락 항목 "+repaired+"개 자동 복구") : "정상");
  try { fs.mkdirSync(DATA_DIR, { recursive:true }); fs.writeFileSync(path.join(DATA_DIR,".selfcheck"), String(Date.now())); add("디스크 저장", true, DATA_DIR); }
  catch(e){ add("디스크 저장", false, String(e.message||e)); }
  try { const sp = await supaProbe(); add("영구 저장(Supabase)", sp.ok, sp.note); }
  catch(e){ add("영구 저장(Supabase)", false, String(e.message||e).slice(0,100)); }
  try {
    const col=(DB.state&&DB.state.collect)||{};
    const on = col.everyMin>0;
    const last = DB.lastCollectAt ? new Date(DB.lastCollectAt).toLocaleString("ko-KR") : "아직 없음";
    add("자율수행", on, on ? ("주기 "+col.everyMin+"분 · 근무 "+(isWorking()?"중(작동)":"외(대기)")+" · 마지막 실행 "+last) : "꺼짐 — 설정 › 자율수행 주기에서 분을 설정하세요");
  } catch(e){ add("자율수행", false, String(e.message||e).slice(0,80)); }
  add("카카오 알림", !!KAKAO_TOKEN, KAKAO_TOKEN?"연동됨":"미설정(선택)");
  add("진짜 성우(Gemini TTS)", true, (geminiKey())?("사용 가능 · 모델 "+TTS_MODEL):"GEMINI_API_KEY 미설정 — 기기 음성만 사용");

  add("작업 기록", true, "작업 "+(DB.jobs||[]).length+"건 · 회의 "+(DB.meetings||[]).length+"건 · 자료 "+((DB.collections||[]).length)+"건");
  const memN = Object.values(DB.deptMemory||{}).reduce((a,b)=>a+(b||[]).length,0);
  const perDept = Object.keys(AGENTS).map(d=>{ const n=(DB.deptMemory[d]||[]).length; const e=(DB.exp&&DB.exp[d])||0; return AGENTS[d].kr+" "+n+"건(Lv"+(Math.floor(e/5)+1)+")"; }).join(" · ");
  const zero = Object.keys(AGENTS).filter(d=>!((DB.deptMemory[d]||[]).length)).map(d=>AGENTS[d].kr);
  add("부서별 학습", memN>0, "총 "+memN+"건 · "+perDept + (zero.length? ("  ※ 아직 학습 0건: "+zero.join(", ")) : ""));
  const errs = (DB.errors||[]).slice(-10);
  add("최근 오류", errs.length===0, errs.length? (errs.length+"건 (마지막: "+(errs[errs.length-1].where||"")+" — "+String(errs[errs.length-1].msg||"").slice(0,80)+")") : "없음");
  add("가동 시간", true, Math.floor(process.uptime()/60)+"분");
  res.json({ ok: checks.every(c=>c.ok || ["카카오 알림","영구 저장(Supabase)","최근 오류","진짜 성우(Gemini TTS)"].includes(c.name)), checks, at:Date.now() });
});
app.get("/api/jobs", (req,res)=> res.json(DB.jobs.slice(-50)));

// 카카오 챗봇 웹훅 (하행: 카톡 지시 → 처리 → 카톡 응답)
app.post("/api/kakao/webhook", async (req,res)=>{
  try {
    const utter = req.body?.userRequest?.utterance || "";
    const uid = req.body?.userRequest?.user?.id || "";
    const callbackUrl = req.body?.userRequest?.callbackUrl || "";
    if (ALLOWED_KAKAO.length && !ALLOWED_KAKAO.includes(uid)) {
      return res.json(kakaoText("권한이 없는 사용자입니다."));
    }
    // 콜백 지원 봇(AI 챗봇 전환 완료): 5초 제한을 넘기지 않도록 즉시 대기응답 후 백그라운드 처리
    if (callbackUrl) {
      res.json({ version:"2.0", useCallback:true, data:{ text:"팀장이 살펴보고 있어요… 잠시만요 🙂" } });
      // 백그라운드에서 실제 처리 → callbackUrl로 최종 답 전송
      (async ()=>{
        let reply = "";
        try { reply = await processKakaoUtterance(utter); }
        catch(e){ reply = "처리 오류: "+(e.message||e); }
        try {
          await fetch(callbackUrl, {
            method:"POST", headers:{ "Content-Type":"application/json" },
            body: JSON.stringify(kakaoText(String(reply||"처리했습니다.").slice(0,3800)))
          });
        } catch(e){ logError("kakao-callback-send", e); }
      })();
      return;
    }
    // 콜백 미지원(심사 전): 동기 처리 — 5초 안에 끝나는 간단한 명령만 정상 응답
    const reply = await processKakaoUtterance(utter);
    res.json(kakaoText(String(reply||"처리했습니다.").slice(0,3800)));
  } catch(e){ res.json(kakaoText("처리 오류: "+(e.message||e))); }
});
// 카카오 발화 1건을 처리해 답변 텍스트를 돌려준다 (동기/콜백 공용)
async function processKakaoUtterance(utter){
  // 예약 외부발행 관리 의도면 팀장(오세라)이 직접 처리
  const isSchedCmd = /예약/.test(utter) || (/발행|게시|올려|포스팅/.test(utter) && /(매일|매주|오전|오후|\d\s*시|\d\s*건|건수|시각|시간|취소|삭제|목록|중단|정지)/.test(utter));
  if (isSchedCmd) {
    const out = await opsCommand(utter, "kakao");
    return (out && out.reply) || "예약 발행을 처리했어요.";
  }
  // 프로젝트 명령: "프로젝트 목록" / "프로젝트 [이름] 지시: ..." / "프로젝트 [이름] 진행"
  if (/프로젝트/.test(utter)) {
    const active = (DB.projects||[]).filter(p=>p.status==="active"||p.status==="awaiting");
    if (/목록|리스트|상태|현황|뭐(가|)\s*있/.test(utter)) {
      if(!active.length) return "진행 중인 프로젝트가 없어요. 앱에서 새로 시작할 수 있어요.";
      const lines = active.map(p=>"• "+p.title+" (사이클 "+(p.cycle||0)+"회"+(p.status==="awaiting"?" · 🙋확인 대기":"")+")").join("\n");
      return "📋 진행 중 프로젝트\n"+lines+"\n\n'프로젝트 [이름] 지시: 내용' 으로 지시하거나 '프로젝트 [이름] 진행' 으로 즉시 한 사이클을 돌릴 수 있어요.";
    }
    let target = active.find(p=> utter.indexOf(String(p.title).slice(0,6))>=0) || active[0];
    if (!target) return "진행 중인 프로젝트가 없어요. 앱에서 먼저 시작해 주세요.";
    const dirMatch = utter.match(/지시[:：]?\s*(.+)$/) || utter.match(/(?:해줘|하자|가자|반영|바꿔|추가|수정)\s*[:：]?\s*(.+)$/);
    if (/진행|사이클|돌려|실행/.test(utter) && !/지시/.test(utter)) {
      if(target.status==="awaiting"){ target.status="active"; target.holdReason=""; }
      await runProjectCycle(target); saveDB();
      const last=(target.log||[]).slice(-1)[0]||{};
      return "▶ '"+target.title+"' 한 사이클 진행했어요.\n["+(AGENTS[last.dept]?AGENTS[last.dept].kr:last.dept||"")+"] "+String(last.work||"").slice(0,300);
    }
    const dir = dirMatch ? dirMatch[1].trim() : "";
    if (dir) {
      target.clientNotes=target.clientNotes||[]; target.clientNotes.push({ at:Date.now(), text:dir.slice(0,500), used:false });
      if(target.status==="awaiting"){ target.status="active"; target.holdReason=""; }
      await runProjectCycle(target); saveDB();
      const last=(target.log||[]).slice(-1)[0]||{};
      return "✅ '"+target.title+"'에 지시를 반영해 진행했어요.\n["+(AGENTS[last.dept]?AGENTS[last.dept].kr:last.dept||"")+"] "+String(last.work||"").slice(0,300);
    }
    // 그 외는 질문·토론으로 간주 → 팀장(오세라)이 프로젝트 맥락으로 답변
    const q = utter.replace(/프로젝트/,"").replace(new RegExp(String(target.title).slice(0,6),"g"),"").trim() || utter;
    target.discuss=target.discuss||[];
    target.discuss.push({ at:Date.now(), role:"client", text:q.slice(0,600) });
    const recent = (target.log||[]).slice(-4).map(x=>"· C"+x.cycle+" ["+(AGENTS[x.dept]?AGENTS[x.dept].kr:x.dept)+"] "+String(x.work||"").slice(0,100)).join("\n") || "(아직 진행 없음)";
    const chat = target.discuss.slice(-8).map(m=>(m.role==="client"?"클라이언트: ":"팀장: ")+m.text).join("\n");
    const sys="너는 이 회사 팀장 '"+AGENTS.ops.kr+"("+MEMBERS["ops"]+")'이며 최고 지능이다."+ADDRESS+clientBlock()
      +" 아래 프로젝트에 대해 클라이언트가 카카오톡으로 질문·토론한다. 구체적이고 실질적으로, 방향·근거·다음 액션을 명확히 답하라. 되묻기만 하지 말고 네 판단을 제시하되 결정이 필요한 지점은 짚어라. 카톡이니 한국어로 2~5문장, 간결하게."
      +"\n프로젝트: "+target.title+"\n목표: "+target.goal+"\n최근 진행:\n"+recent+"\n\n[대화]\n"+chat;
    let reply=""; try{ reply=await genText(sys, "팀장으로서 답하라.", 500, "gemini"); }catch(e){ logError("kakao-project-discuss", e); }
    if(!reply) reply="지금 답변을 생성하지 못했어요. 잠시 후 다시 물어봐 주세요.";
    target.discuss.push({ at:Date.now(), role:"lead", text:String(reply).slice(0,900) });
    if(target.discuss.length>40) target.discuss=target.discuss.slice(-40);
    saveDB();
    return "👑 팀장 ["+String(target.title).slice(0,20)+"]\n"+String(reply).slice(0,900);
  }
  // 일반 지시 → 부서 처리
  const job = await handleInstruction(utter, "kakao");
  return job.results.map(r=>AGENTS[r.dept].no+" "+AGENTS[r.dept].kr+":\n"+r.text).join("\n\n").slice(0,1000) || "처리했습니다.";
}
function kakaoText(text){ return { version:"2.0", template:{ outputs:[{ simpleText:{ text } }] } }; }

// 카카오 알림 발송(상행): { text } → 나에게 보내기
app.post("/api/notify", async (req,res)=>{
  try { res.json(await kakaoNotify(req.body?.text || "")); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// 발행 승인/거절 (카톡 버튼이 여는 링크)
app.get("/api/approve", async (req,res)=>{
  const ap = (DB.approvals||[]).find(a=>String(a.id)===String(req.query.id) && a.code===req.query.code && a.status==="pending");
  if(!ap) return res.send("<meta charset=utf-8>유효하지 않거나 이미 처리된 승인입니다.");
  ap.status="approved"; saveDB();
  let results; try { results = await doActualPublish(ap.content, ap.platforms); } catch(e){ results=[{platform:"(오류)",ok:false,note:String(e.message||e)}]; }
  kakaoNotify("✅ 승인 발행: "+ap.platforms.join(", ")+" — "+results.map(r=>r.platform+(r.ok?"✓":"✗")).join(" ")).catch(()=>{});
  res.send("<meta charset=utf-8><pre>승인되어 발행했습니다.\n"+results.map(r=>r.platform+": "+(r.ok?("완료"+(r.url?" "+r.url:"")):("실패 — "+(r.note||"")))).join("\n")+"</pre>");
});
app.get("/api/reject", (req,res)=>{
  const ap = (DB.approvals||[]).find(a=>String(a.id)===String(req.query.id) && a.code===req.query.code && a.status==="pending");
  if(!ap) return res.send("<meta charset=utf-8>유효하지 않거나 이미 처리된 승인입니다.");
  ap.status="rejected"; saveDB();
  res.send("<meta charset=utf-8>거절되었습니다. 발행하지 않습니다.");
});
// 프로젝트 승인(카톡 버튼): 승인하면 대기 해제 + 즉시 한 사이클 진행
app.get("/api/projects/approve", async (req,res)=>{
  const p = projectById(req.query.id);
  if(!p || p.approveCode!==req.query.code) return res.send("<meta charset=utf-8>유효하지 않거나 이미 처리된 요청입니다.");
  p.approveCode=""; p.status="active"; p.holdReason=""; p.nextAt=Date.now();
  p.clientNotes=p.clientNotes||[]; p.clientNotes.push({ at:Date.now(), text:"[카톡 승인] 위 방향으로 승인함. 계속 진행.", used:false });
  saveDB();
  res.send("<meta charset=utf-8><pre>✅ 승인되었습니다. 이 방향으로 계속 진행합니다.\n('"+String(p.title||"").slice(0,40)+"')</pre>");
  try{ runProjectCycle(p).then(()=>kakaoNotify("▶ 프로젝트 '"+p.title+"' 승인 후 한 사이클을 진행했어요.").catch(()=>{})).catch(e=>logError("proj-approve-run",e)); }catch(e){}
});
// 프로젝트 보류(카톡 버튼): 대기 유지, 앱에서 지시하도록 안내
app.get("/api/projects/hold", (req,res)=>{
  const p = projectById(req.query.id);
  if(!p || p.approveCode!==req.query.code) return res.send("<meta charset=utf-8>유효하지 않거나 이미 처리된 요청입니다.");
  p.approveCode=""; saveDB();
  res.send("<meta charset=utf-8><pre>⏸ 보류했습니다.\n앱 프로젝트 탭에서 방향을 지시하거나 팀장과 토론해 주세요.</pre>");
});
// 앱에서 보는 승인 대기 목록
app.get("/api/approvals", (req,res)=>{
  res.json((DB.approvals||[]).filter(a=>a.status==="pending").map(a=>({
    id:a.id, code:a.code, platforms:a.platforms||[],
    title:String((a.content&&(a.content.title||a.content.description))||"").slice(0,90), at:a.at
  })));
});

// ===== Human-in-the-Loop: 콘텐츠 승인/반려/수정 =====
// 콘텐츠 생성 결과를 '승인 대기'로 올림
function queueContentApproval(dept, topic, label, content, reviews){
  DB.contentApprovals = DB.contentApprovals || [];
  const item={ id:Date.now()+Math.floor(Math.random()*1000), dept, topic:topic||"", label:label||"", content:String(content||""), reviews:reviews||[], status:"pending", at:Date.now() };
  DB.contentApprovals.push(item);
  if(DB.contentApprovals.length>50) DB.contentApprovals=DB.contentApprovals.slice(-50);
  saveDB();
  // 새 콘텐츠가 올라오면 카톡 알림 (설정 contentNotify가 false가 아니면 기본 ON)
  try {
    if((DB.state||{}).contentNotify !== false){
      const deptKr = (AGENTS[dept]?AGENTS[dept].kr:dept)||"";
      const preview = String(content||"").replace(/\s+/g," ").slice(0,80);
      kakaoNotify("📝 새 콘텐츠가 올라왔어요 — "+deptKr+(topic?(" · "+String(topic).slice(0,30)):"")+"\n\""+preview+(String(content||"").length>80?"…":"")+"\"\n\n앱 '콘텐츠' 탭에서 확인·승인하세요.").catch(()=>{});
    }
  } catch(e){}
  return item;
}
app.get("/api/content-approvals", (req,res)=>{
  res.json((DB.contentApprovals||[]).filter(a=>a.status==="pending").map(a=>({
    id:a.id, dept:a.dept, topic:a.topic, label:a.label, content:a.content, reviews:a.reviews, at:a.at
  })));
});
// 승인: 확정 → 학습 반영
app.post("/api/content-approve", (req,res)=>{
  try{
    const it=(DB.contentApprovals||[]).find(a=>String(a.id)===String((req.body||{}).id) && a.status==="pending");
    if(!it) return res.status(404).json({ error:"대기 중인 항목이 없어요" });
    it.status="approved"; it.decidedAt=Date.now();
    const d=it.dept||"creation";
    DB.deptMemory[d]=DB.deptMemory[d]||[];
    DB.deptMemory[d].push({ at:Date.now(), instruction:"[승인된 콘텐츠] "+(it.label||""), note:"클라이언트 승인 · "+String(it.content).slice(0,300) });
    DB.exp=DB.exp||{}; DB.exp[d]=(DB.exp[d]||0)+1;
    saveDB();
    res.json({ ok:true, content:it.content });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 반려: 사유 반영해 재작성 → 다시 대기
app.post("/api/content-reject", async (req,res)=>{
  try{
    const b=req.body||{};
    const it=(DB.contentApprovals||[]).find(a=>String(a.id)===String(b.id) && a.status==="pending");
    if(!it) return res.status(404).json({ error:"대기 중인 항목이 없어요" });
    const reason=String(b.reason||"").trim();
    const d=it.dept||"creation"; const a=AGENTS[d];
    let sys="너는 SNS 자동화 회사의 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role+ADDRESS+STYLE;
    const kb=knowledgeText(d); if(kb) sys+="\n\n[축적 전문성]\n"+kb;
    sys+=" 아래 콘텐츠에 대한 클라이언트 반려 사유를 반드시 반영해, 개선한 완성본만 다시 출력하라. 한국어로."+profileContext();
    const revised=await genText(sys, "[반려 사유]\n"+(reason||"전반적으로 다시")+"\n\n[기존 콘텐츠]\n"+it.content, 1500, workEngine());
    it.content=String(revised); it.reviews=(it.reviews||[]).concat([{ round:"반려", verdict:"REJECT", feedback:reason }]); it.at=Date.now();
    // 학습: 반려 피드백
    DB.deptMemory[d]=DB.deptMemory[d]||[];
    DB.deptMemory[d].push({ at:Date.now(), instruction:"[클라이언트 반려]", note:reason.slice(0,200) });
    saveDB();
    res.json({ ok:true, content:it.content });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 직접 수정 확정: 클라이언트가 손본 최종본 저장 → 학습(가장 강한 신호)
app.post("/api/content-edit", (req,res)=>{
  try{
    const b=req.body||{};
    const it=(DB.contentApprovals||[]).find(a=>String(a.id)===String(b.id) && a.status==="pending");
    if(!it) return res.status(404).json({ error:"대기 중인 항목이 없어요" });
    const edited=String(b.content||"").trim();
    if(!edited) return res.status(400).json({ error:"수정 내용을 입력하세요" });
    it.content=edited; it.status="approved"; it.decidedAt=Date.now(); it.edited=true;
    const d=it.dept||"creation";
    DB.deptMemory[d]=DB.deptMemory[d]||[];
    DB.deptMemory[d].push({ at:Date.now(), instruction:"[클라이언트 직접수정 최종본]", note:edited.slice(0,300) });
    DB.exp=DB.exp||{}; DB.exp[d]=(DB.exp[d]||0)+2; // 직접수정본은 강한 학습 신호
    if(DB.exp[d]%3===0){ try{ distillKnowledge(d); }catch(e){} }
    saveDB();
    res.json({ ok:true, content:edited });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// 정기 자료수집 조회
app.get("/api/collections", (req,res)=> res.json((DB.collections||[]).slice(-50)));

// AI 이미지 생성: { prompt } → { url }
// 진짜 성우 TTS: { text, dept | voice } → { audio(base64 wav), mime }
app.post("/api/tts", async (req,res)=>{
  try { const { text, dept, voice } = req.body||{};
    if (!text) return res.status(400).json({ error:"text 필요" });
    const v = voice || DEPT_VOICE[dept] || "Charon";
    const audio = await ttsGemini(text, v);
    res.json({ ok:true, audio, mime:"audio/wav", voice:v });
  } catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.get("/api/tts/voices", (req,res)=> res.json({ map:DEPT_VOICE, enabled: !!geminiKey() }));
app.post("/api/generate-image", async (req,res)=>{
  try {
    const prompt = (req.body && req.body.prompt || "").trim();
    if (!prompt) return res.status(400).json({ error:"prompt 필요" });
    const url = await generateImage(prompt);
    res.json({ url });
  } catch(e){ logError("generate-image", e); res.status(500).json({ error:String(e.message||e) }); }
});

// AI 영상 생성: { prompt } → { url } (Replicate, 시간이 걸릴 수 있음)
app.post("/api/generate-video", async (req,res)=>{
  try {
    const prompt = (req.body && req.body.prompt || "").trim();
    if (!prompt) return res.status(400).json({ error:"prompt 필요" });
    const url = await generateVideo(prompt);
    res.json({ url });
  } catch(e){ logError("generate-video", e); res.status(500).json({ error:String(e.message||e) }); }
});

// 미디어 파일 업로드(영상/이미지): { data(base64 or dataURL), mime } → { url }
app.post("/api/upload-video", async (req,res)=>{
  try {
    const data = req.body && req.body.data;
    if (!data) return res.status(400).json({ error:"data 필요" });
    const mime = (req.body && req.body.mime) || "video/mp4";
    const b64 = String(data).replace(/^data:[^;]+;base64,/, "");
    const url = await uploadMedia(b64, mime);
    res.json({ url });
  } catch(e){ logError("upload-video", e); res.status(500).json({ error:String(e.message||e) }); }
});

// 비용·토큰 사용량: 누적 토큰 + 예상 비용(USD)
app.get("/api/usage", (req,res)=>{
  const u = DB.usage || { in:0, out:0, calls:0 };
  const st = DB.state || {};
  const inRate = +(process.env.PRICE_IN_PER_M || 3), outRate = +(process.env.PRICE_OUT_PER_M || 15);
  const usdKrw = +(st.usdKrw) || +(process.env.PRICE_USD_KRW || 1540); // 설정 환율 우선, 없으면 env, 기본 1540
  const monthLimitKrw = +(st.monthLimitKrw) || 0; // 0 = 한도 알림 끔
  const krw = c => Math.round(c*usdKrw);
  const cost = x => (x.in/1e6)*inRate + (x.out/1e6)*outRate;
  const today = todayStr(), month = today.slice(0,7);
  const pubToday = (DB.jobs||[]).filter(j=>j.type==="publish" && j.ok && new Date(j.at).toISOString().slice(0,10)===today).reduce((a,j)=>a+(j.count||1),0);
  const ud = (DB.usageDaily && DB.usageDaily.date===today) ? DB.usageDaily : { in:0, out:0, calls:0 };
  const um = (DB.usageMonthly && DB.usageMonthly.month===month) ? DB.usageMonthly : { in:0, out:0, calls:0 };
  const costAll=cost(u), costToday=cost(ud), costMonth=cost(um);
  const krwMonth=krw(costMonth);
  res.json({
    tokensIn:u.in, tokensOut:u.out, calls:u.calls, estCostUsd:+costAll.toFixed(4), estCostKrw:krw(costAll),
    tokensInToday:ud.in, tokensOutToday:ud.out, callsToday:ud.calls, estCostUsdToday:+costToday.toFixed(4), estCostKrwToday:krw(costToday),
    tokensInMonth:um.in, tokensOutMonth:um.out, callsMonth:um.calls, estCostUsdMonth:+costMonth.toFixed(4), estCostKrwMonth:krwMonth,
    publishesToday:pubToday, usdKrw, inRate, outRate,
    monthLimitKrw, monthOver: (monthLimitKrw>0 && krwMonth>=monthLimitKrw),
    monthPct: monthLimitKrw>0 ? Math.min(100, Math.round(krwMonth/monthLimitKrw*100)) : 0,
    gemini: geminiRateInfo(), paidFallback: !!(st.allowPaidFallback),
    geminiBudget: (function(){ const b=geminiBudget(); return { monthLimitKrw:b.monthLimit, monthKrw:b.monthKrw, dayKrw:b.dayKrw, monthPct: b.monthLimit>0?Math.min(100,Math.round(b.monthKrw/b.monthLimit*100)):0, monthOver:b.monthOver, paused:!geminiAutoAllowed(), calls:b.calls }; })(),
    paidMode: paidModeOn(), paidGeminiOn: !!(st.paidGeminiOn), paidKeyAvailable: paidKeyAvailable(),
    search: { today: searchesToday(), cap: searchDailyCap(), total: (DB.geminiSearchTotal||0), on: (st.nightSearchOn!==false) }
  });
});

// 에러 로그 조회
app.get("/api/errors", (req,res)=> res.json((DB.errors||[]).slice(-50)));

// ===== 🩺 시스템 건강 상태: 오류(묶음)·작업·API·저장을 한 화면에 =====
app.get("/api/health", (req,res)=>{
  const now = Date.now();
  const groups = {};
  (DB.errors||[]).forEach(e=>{
    const key = (e.where||"")+"|"+String(e.msg||"").slice(0,60);
    if(!groups[key]) groups[key] = { where:e.where||"", msg:String(e.msg||"").slice(0,160), count:0, last:0 };
    groups[key].count++; groups[key].last = Math.max(groups[key].last, e.at||0);
  });
  const errorGroups = Object.keys(groups).map(k=>groups[k]).sort((a,b)=>b.last-a.last).slice(0,12);
  const errs24h = (DB.errors||[]).filter(e=>(now-(e.at||0))<86400000).length;
  const rateWaits24h = (DB.rateWaits||[]).filter(w=>(now-(w.at||0))<86400000).length;
  function jobStat(arr){ const s={queued:0,running:0,done:0,error:0,stuck:0}; (arr||[]).forEach(j=>{ if(s[j.status]!=null)s[j.status]++; if(j.status==="running" && (now-(j.startedAt||j.at||now))>900000) s.stuck++; }); return s; }
  const learn = jobStat(DB.learnJobs), page = jobStat(DB.pageJobs);
  const api = { geminiKey: !!geminiKey(), cooldownActive: now<geminiCooldownUntil,
    cooldownSec: Math.max(0, Math.ceil((geminiCooldownUntil-now)/1000)),
    searchesToday: (typeof searchesToday==="function"?searchesToday():0),
    researchMode: (DB.state&&DB.state.researchMode)||"free", paidGeminiOn: paidModeOn() };
  const storage = { saveFailStreak: _saveFailStreak||0, lastSaveErr: String(_lastSaveErr||"").slice(0,140),
    lastSaveOkMinAgo: _lastSaveOkAt ? Math.round((now-_lastSaveOkAt)/60000) : -1 };
  const problems = [];
  if(!api.geminiKey) problems.push("GEMINI_API_KEY 미설정");
  if(storage.saveFailStreak>0) problems.push("데이터 저장 실패 "+storage.saveFailStreak+"회");
  if(learn.stuck+page.stuck>0) problems.push("멈춘 작업 "+(learn.stuck+page.stuck)+"건");
  if(errs24h>10) problems.push("최근 24시간 오류 "+errs24h+"건");
  res.json({ ok:true, at:now, status: problems.length?"warn":"ok", problems,
    errors:{ total:(DB.errors||[]).length, last24h:errs24h, groups:errorGroups },
    rateWaits:{ last24h:rateWaits24h, note:"Gemini 무료 한도로 잠시 쉰 횟수 — 오류 아님(자동 재시도됨)" },
    jobs:{ learn, page }, api, storage });
});

// ===== 🧪 자가 점검(스모크 테스트): 핵심 경로를 실제로 한 번씩 돌려 pass/fail =====
app.get("/api/selftest", async (req,res)=>{
  const deep = String((req.query&&req.query.deep)||"")==="1";  // deep=1이면 유료 가능성 있는 LLM까지 점검
  const results = [];
  const t = async (name, fn)=>{ const s=Date.now(); try{ const d=await fn(); results.push({ name, ok:true, ms:Date.now()-s, detail:String(d||"ok").slice(0,120) }); }catch(e){ results.push({ name, ok:false, ms:Date.now()-s, detail:String(e.message||e).slice(0,160) }); } };
  await t("JSON 파서(safeJson)", async()=>{ const o=safeJson('{"a":1}',null); if(!o||o.a!==1) throw new Error("파싱 실패"); return "ok"; });
  await t("점수·부족분 파서", async()=>{ const r=parseReviewScore("점수: 72\n부족: 여백이 부족함, CTA 불명확",null); if(r.score!==72||!r.gaps.length) throw new Error("추출 실패"); return "72점 · gaps "+r.gaps.length+"개"; });
  await t("데이터 저장(saveDB)", async()=>{ saveDB(); if(_saveFailStreak>0) throw new Error("저장 실패 누적 "+_saveFailStreak+"회"); return "ok"; });
  await t("카카오 토큰", async()=>{ const tk=await getKakaoToken(); return tk?"발급 ok":"토큰 없음"; });
  await t("무료 웹검색(DuckDuckGo)", async()=>{ const u=await freeSearchUrls("고흥 유자 마케팅",3); if(!u.length) throw new Error("결과 0건(Render 차단 의심)"); return u.length+"건"; });
  await t("웹페이지 수집(+리더 폴백)", async()=>{ const h=await fetchPageText("https://example.com"); if(!h||h.length<50) throw new Error("본문 없음"); return (h||"").length+"자"; });
  if(deep){
    await t("Gemini 텍스트(무료)", async()=>{ const x=await geminiText("한 단어로 인사해",40); if(!x) throw new Error("빈 응답"); return String(x).slice(0,30); });
    await t("Gemini 검색(grounding)", async()=>{ const s=await geminiSearch("고흥 특산물 한 줄",200); return (s&&s.text)?("응답 ok"+((s.sources||[]).length?" · 검색근거 "+s.sources.length:" · 검색근거 없음")):"빈 응답"; });
    await t("Claude 호출", async()=>{ const x=await anthropic("한국어로 답하라.","'정상'이라고만 답해",20); return String(x||"").slice(0,20); });
  }
  const pass = results.filter(r=>r.ok).length;
  res.json({ ok:true, pass, fail:results.length-pass, total:results.length, deep, results });
});

// ===== ① 브랜드 학습: 고흥/민앤팜 브랜드 프로필(모든 생성의 기준) =====
app.get("/api/brand", (req,res)=>{ const b=DB.brandProfile||{text:"",at:0}; res.json({ ok:true, text:b.text||"", at:b.at||0 }); });
app.post("/api/brand", async (req,res)=>{
  try{
    const b=req.body||{};
    if(typeof b.text==="string" && !b.material && !b.url && b.distill!==true){
      DB.brandProfile={ text:String(b.text).slice(0,4000), at:Date.now() }; saveDB();
      return res.json({ ok:true, text:DB.brandProfile.text, mode:"saved" });
    }
    let material=String(b.material||b.text||"");
    if(b.url){ try{ const h=await fetchPageText(String(b.url)); if(h) material+=("\n\n[페이지] "+b.url+"\n"+h.slice(0,12000)); }catch(e){ material+=("\n\n[페이지 실패] "+String(e.message||e)); } }
    if(!material.trim()) return res.status(400).json({ error:"브랜드 자료(텍스트 또는 URL)가 필요해요" });
    const prior=(DB.brandProfile&&DB.brandProfile.text)||"";
    const sys="너는 민앤팜(전남 고흥 특산물)의 브랜드 가이드 정리 담당이다. 아래 자료에서 '모든 콘텐츠 제작의 기준이 될 브랜드 프로필'을 뽑아 기존 것과 통합·정교화하라. 한국어로, 아래 형식만(설명·서론 금지):\n브랜드 한 줄: (무엇을 파는 누구인가)\n핵심 가치·톤: (말투·분위기 3~5개)\n반드시 지킬 표현: (사실 기반·수치·산지 등)\n피해야 할 표현: (과장·근거 없는 최상급·금지어)\n대표 제품·사실: (제품명·당도·산지·인증 등 구체 수치)\n자주 쓰는 키워드·해시태그:";
    const out=await anthropic(sys, "[기존 브랜드 프로필]\n"+(prior||"(없음)")+"\n\n[새 자료]\n"+material.slice(0,16000), 1200);
    DB.brandProfile={ text:String(out).slice(0,4000), at:Date.now() }; saveDB();
    res.json({ ok:true, text:DB.brandProfile.text, mode:"distilled" });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// ===== ② 원소스 멀티플랫폼 변환: 콘텐츠 1개 → 플랫폼별 재가공 (백그라운드 작업) =====
const REPURPOSE_FORMATS = {
  instagram: "인스타그램 피드 — 후킹 첫 문장 + 본문(줄바꿈 넉넉히, 이모지 적절) + 해시태그 8~12개. 저장·공유 유도.",
  reels:     "인스타 릴스/숏폼 대본 — 0~3초 후킹, 컷별 자막(3~5컷), 마지막 CTA. 세로 영상용.",
  shorts:    "유튜브 쇼츠 대본 — 3초 후킹 + 15~40초 내레이션/자막 흐름 + 끝 CTA. 제목 후보 2개.",
  blog:      "네이버/티스토리 블로그 — SEO 제목, 소제목 3~5개, 정보성 본문(600~1000자), 자연스러운 키워드, 마무리.",
  youtube:   "유튜브 영상 설명란 — 제목 후보 3개 + 설명(첫 2줄 후킹) + 타임스탬프 뼈대 + 태그.",
  thread:    "스레드/X — 3~6개 연속 포스트로 분할, 각 280자 내, 첫 포스트 후킹.",
  kakao:     "카카오채널 소식 — 짧고 친근한 톤, 핵심 혜택 + 링크 유도."
};
const REPURPOSE_KR = { instagram:"인스타 피드", reels:"릴스", shorts:"유튜브 쇼츠", blog:"블로그", youtube:"유튜브 설명란", thread:"스레드/X", kakao:"카카오채널" };
function trimRepurposeJobs(){ DB.repurposeJobs=DB.repurposeJobs||[]; if(DB.repurposeJobs.length>15) DB.repurposeJobs=DB.repurposeJobs.slice(-15); }
async function runRepurposeJob(id, appUrl){
  const j=(DB.repurposeJobs||[]).find(x=>String(x.id)===String(id)); if(!j) return;
  j.status="running"; j.startedAt=Date.now(); saveDB(); beginJobMode();
  try{
    const list=(j.platforms||[]).filter(p=>REPURPOSE_FORMATS[p]).slice(0,7);
    if(!list.length) throw new Error("변환할 플랫폼이 없어요");
    if(!String(j.source||"").trim()) throw new Error("원본 콘텐츠가 비었어요");
    j.variants={};
    for(const p of list){
      j.step=(REPURPOSE_KR[p]||p)+" 변환 중"; saveDB();
      const sys="너는 민앤팜(고흥 특산물)의 콘텐츠 리퍼포징 전문가다."+brandBlock()+profileContext()
        +(knowledgeText("creation")?("\n\n[제작 품질 공식]\n"+knowledgeText("creation")):"")
        +(knowledgeText("monetization")?("\n\n[전환 품질 공식]\n"+knowledgeText("monetization")):"")
        +"\n\n아래 '원본 콘텐츠'를 핵심 손실 없이 아래 형식으로 재가공하라. 형식: "+REPURPOSE_FORMATS[p]+" 브랜드 사실·수치는 왜곡 금지. 결과물만 출력(설명 금지).";
      j.variants[p]=stripFences(await anthropic(sys, "[원본]\n"+String(j.source).slice(0,8000), 1500));
      saveDB();
    }
    j.status="done"; j.doneAt=Date.now(); j.step="완료"; trimRepurposeJobs(); saveDB();
    const done=Object.keys(j.variants).map(p=>REPURPOSE_KR[p]||p).join(", ");
    kakaoNotify("♻️ 멀티플랫폼 변환 완료 — "+done+"\n앱 학습 탭에서 플랫폼별 결과를 확인·복사하세요.", String(appUrl||"")).catch(()=>{});
  }catch(e){ j.status="error"; j.error=String(e.message||e).slice(0,200); j.doneAt=Date.now(); saveDB();
    kakaoNotify("⚠️ 멀티플랫폼 변환 실패 — "+j.error).catch(()=>{});
  }finally{ endJobMode(); }
}
app.post("/api/repurpose/start", (req,res)=>{
  try{
    const b=req.body||{};
    const source=String(b.source||b.content||"").trim();
    const platforms=(Array.isArray(b.platforms)?b.platforms:[]).filter(p=>REPURPOSE_FORMATS[p]);
    if(!source) return res.status(400).json({ error:"원본 콘텐츠를 입력하세요" });
    if(!platforms.length) return res.status(400).json({ error:"변환할 플랫폼을 1개 이상 선택하세요" });
    DB.repurposeJobs=DB.repurposeJobs||[];
    const job={ id:String(Date.now())+"_"+Math.floor(Math.random()*1000), source, platforms, status:"queued", at:Date.now() };
    DB.repurposeJobs.push(job); trimRepurposeJobs(); saveDB();
    res.json({ ok:true, jobId:job.id, status:"queued", platforms });
    setImmediate(()=>{ runRepurposeJob(job.id, String(b.appUrl||"")).catch(e=>logError("runRepurposeJob", e)); });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.get("/api/repurpose/status", (req,res)=>{
  const j=(DB.repurposeJobs||[]).find(x=>String(x.id)===String(req.query.id||"")); if(!j) return res.status(404).json({ error:"작업을 찾을 수 없어요" });
  res.json({ ok:true, id:j.id, status:j.status, step:j.step||"", error:j.error||"", platforms:j.platforms||[], variants:(j.status==="done"?(j.variants||{}):null) });
});

// ===== 🎬 AI 영상 기획 파이프라인: 리서치·콘셉트 → 마스터 시트(장면 프롬프트까지). 영상 생성 자체는 외부 툴(힉스필드 등)에 프롬프트 투입 =====
const VIDEO_HOOKS = "챌린지형, 언박싱형, 솔직 리뷰형, ASMR형, 비포&애프터형, 스토리텔링형";
const VIDEO_PLAT_KR = { shorts:"유튜브 쇼츠", reels:"인스타 릴스", tiktok:"틱톡" };
function trimVideoJobs(){ DB.videoJobs=DB.videoJobs||[]; if(DB.videoJobs.length>15) DB.videoJobs=DB.videoJobs.slice(-15); }
async function runVideoPlanJob(id, appUrl){
  const j=(DB.videoJobs||[]).find(x=>String(x.id)===String(id)); if(!j) return;
  j.status="running"; j.startedAt=Date.now(); saveDB(); beginJobMode();
  try{
    const platform = VIDEO_PLAT_KR[j.platform]?j.platform:"shorts";
    const platKr = VIDEO_PLAT_KR[platform];
    const days = Math.max(3, Math.min(30, +j.days||14));
    // 1) 리서치
    j.step="시장·트렌드 리서치 중"; saveDB();
    let research={text:"",via:""};
    try{ research=await researchWeb(String(j.source||"").slice(0,120)+" 숏폼 마케팅 후킹 트렌드 2026", 1200); }catch(e){}
    // 2) 콘셉트·후킹
    j.step="콘셉트·후킹 아이디어 도출 중"; saveDB();
    const cSys="너는 민앤팜(전남 고흥 특산물) 숏폼 영상 기획 디렉터다."+brandBlock()+profileContext()+(knowledgeText("creation")?("\n\n[제작 품질 공식]\n"+knowledgeText("creation")):"")
      +"\n\n아래 제품/주제와 리서치를 바탕으로 "+platKr+" 영상 캠페인 콘셉트를 잡아라. 후킹 유형("+VIDEO_HOOKS+") 중 이 제품에 맞는 걸 골라 활용. 반드시 JSON만: {\"concept\":\"캠페인 한 줄 콘셉트\",\"target\":\"핵심 타깃 페르소나\",\"angles\":[\"후킹 앵글 4~6개(각 유형 라벨 포함)\"],\"tone\":\"영상 톤&무드\"}";
    const cRaw=await anthropic(cSys, "[제품/주제]\n"+String(j.source||"").slice(0,2000)+"\n\n[리서치]\n"+String(research.text||"(리서치 없음)").slice(0,4000), 1200);
    const concept=safeJson(cRaw, null)||{ concept:"", target:"", angles:[], tone:"" };
    j.concept=concept; saveDB();
    // 3) 마스터 시트(장면 프롬프트) — 15일씩 청크로 안정 생성
    j.days_plan=[];
    let start=1;
    while(start<=days){
      const end=Math.min(days, start+14);
      j.step=start+"~"+end+"일차 콘텐츠 플랜 작성 중 ("+platKr+")"; saveDB();
      const mSys="너는 숏폼 콘텐츠 캘린더 기획자다."+brandBlock()+(knowledgeText("monetization")?("\n\n[전환 품질 공식]\n"+knowledgeText("monetization")):"")
        +"\n\n아래 콘셉트로 "+platKr+"용 콘텐츠 캘린더의 "+start+"~"+end+"일차를 작성하라. 날짜마다 서로 다른 후킹·소재로 겹치지 않게. 브랜드 사실·수치는 왜곡 금지. "
        +"반드시 JSON 배열만(설명 금지): [{\"day\":숫자,\"hookType\":\"후킹유형\",\"title\":\"영상 제목/주제\",\"persona\":\"등장 페르소나\",\"lengthSec\":숫자,\"scenePrompt\":\"영상 생성 AI에 넣을 장면 프롬프트(카메라·구도·분위기·동작 포함, 영어+한국어 병기)\",\"caption\":\"업로드 캡션\",\"hashtags\":\"해시태그\"}]";
      const mRaw=await anthropic(mSys, "[콘셉트]\n"+JSON.stringify(concept).slice(0,1600), 3800);
      const arr=safeJson(mRaw, null);
      if(Array.isArray(arr)) j.days_plan=j.days_plan.concat(arr);
      saveDB();
      start=end+1;
    }
    j.status="done"; j.doneAt=Date.now(); j.step="완료"; trimVideoJobs(); saveDB();
    kakaoNotify("🎬 AI 영상 기획 완료 — '"+String(j.source||"").slice(0,30)+"' ("+platKr+")\n콘셉트 + "+(j.days_plan||[]).length+"일치 콘텐츠 플랜(장면 프롬프트 포함)이 나왔어요.\n앱 학습 탭에서 확인·복사해 영상툴에 넣으세요.", String(appUrl||"")).catch(()=>{});
  }catch(e){ j.status="error"; j.error=String(e.message||e).slice(0,200); j.doneAt=Date.now(); saveDB();
    kakaoNotify("⚠️ AI 영상 기획 실패 — "+j.error).catch(()=>{}); }
  finally{ endJobMode(); }
}
app.post("/api/video/start", (req,res)=>{
  try{
    const b=req.body||{}; const source=String(b.source||"").trim();
    if(!source) return res.status(400).json({ error:"제품/주제(또는 URL)를 입력하세요" });
    DB.videoJobs=DB.videoJobs||[];
    const job={ id:String(Date.now())+"_"+Math.floor(Math.random()*1000), source:source.slice(0,2000),
      platform:(["shorts","reels","tiktok"].indexOf(b.platform)>=0?b.platform:"shorts"),
      days:Math.max(3,Math.min(30,+b.days||14)), status:"queued", at:Date.now() };
    DB.videoJobs.push(job); trimVideoJobs(); saveDB();
    res.json({ ok:true, jobId:job.id });
    setImmediate(()=>{ runVideoPlanJob(job.id, String(b.appUrl||"")).catch(e=>logError("runVideoPlanJob", e)); });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.get("/api/video/status", (req,res)=>{
  const j=(DB.videoJobs||[]).find(x=>String(x.id)===String(req.query.id||"")); if(!j) return res.status(404).json({ error:"작업을 찾을 수 없어요" });
  res.json({ ok:true, id:j.id, status:j.status, step:j.step||"", error:j.error||"",
    platform:j.platform, concept:(j.status==="done"?(j.concept||null):null), days_plan:(j.status==="done"?(j.days_plan||[]):null) });
});


// ===== 🚀 자율 개선 제안: 팀장이 웹을 조사해 '품질 개선안'을 제시 → 승인 시 품질 공식에 반영 → 승인/거절에서 학습 =====
function improveLogText(){
  const lg=(DB.improveLog||[]).slice(-8);
  if(!lg.length) return "";
  return lg.map(x=>"· ["+(x.decision==="approved"?"승인":"거절")+"] "+(AGENTS[x.dept]?AGENTS[x.dept].kr:x.dept)+": "+x.title).join("\n");
}
function logImproveDecision(pr, approved){
  DB.improveLog = DB.improveLog || [];
  DB.improveLog.push({ id:pr.id, at:Date.now(), dept:pr.dept, title:String(pr.title||"").slice(0,80), decision:approved?"approved":"rejected" });
  if(DB.improveLog.length>30) DB.improveLog=DB.improveLog.slice(-30);
}
const IMPROVE_DEPTS = ["strategy","creation","publishing","analytics","monetization","advisory","scout","editing","graphic"];
async function researchImprovement(focusDept){
  DB.improveProposals = DB.improveProposals || [];
  if (DB.improveProposals.filter(p=>p.status==="pending").length >= 4) return { ok:false, note:"승인 대기 중인 개선안이 많아요(먼저 처리해 주세요)" };
  // 부서 선정: 지정 없으면 경험치 가장 낮은(가장 뒤처진) 부서
  let dept = (focusDept && AGENTS[focusDept] && focusDept!=="ops") ? focusDept : null;
  if(!dept){ const sorted=IMPROVE_DEPTS.slice().sort((a,b)=>((DB.exp&&DB.exp[a])||0)-((DB.exp&&DB.exp[b])||0)); dept=sorted[0]||"creation"; }
  const a=AGENTS[dept];
  beginJobMode();
  try{
    const query = "민앤팜 고흥 특산물 SNS를 위한 '"+a.kr+"' 관점 2026 최신 베스트 프랙티스·성공 사례·구체 기준(수치 포함). 역할: "+a.role;
    let research={ text:"", via:"" };
    try{ research = await researchWeb(query, 1400); }catch(e){ research={ text:"", via:"", err:String(e.message||e).slice(0,120) }; }
    const prior = knowledgeText(dept);
    const sys = "너는 민앤팜(전남 고흥 특산물)의 '"+a.no+" "+a.kr+"' 부서 수석 전문가다."+brandBlock()
      + " 방금 웹에서 최신 사례를 조사했다. 우리 부서의 '기존 품질 공식'을 한 단계 끌어올릴 '가장 임팩트 큰 개선안 1개'를 제안하라. 막연한 조언 금지, 결과물에 바로 적용할 구체 기준으로. "
      + (improveLogText() ? ("\n\n[내가 과거에 낸 개선안과 운영자 결정 — 승인된 유형은 발전시키고, 거절된 유형·이미 있는 것은 반복하지 마라]\n"+improveLogText()) : "")
      + "\n\n반드시 JSON만 출력(설명·마크다운 금지): {\"title\":\"개선안 한 줄 제목\",\"why\":\"왜 지금 이게 중요한가 1~2문장(근거)\",\"improvement\":\"품질 공식에 추가/교체할 구체 기준 3~6개(체크리스트 형태, 재사용 가능하게)\",\"expect\":\"이걸 반영하면 결과물이 어떻게 좋아지는가 1문장\"}";
    const ctx = "[기존 품질 공식]\n"+(prior||"(아직 없음)")+"\n\n[방금 조사한 최신 사례]\n"+(research.text||"(웹조사 실패 — 기존 전문성 기반으로 제안)");
    const raw = await anthropic(sys, ctx, 1200);
    const j = safeJson(raw, null);
    if(!j || !j.improvement) return { ok:false, note:"개선안 생성 실패(형식 오류)" };
    const pr = { id:String(Date.now())+"_"+Math.floor(Math.random()*1000), code:String(Math.floor(1000+Math.random()*9000)),
      dept, deptKr:a.kr, title:String(j.title||"품질 개선").slice(0,80), why:String(j.why||"").slice(0,300),
      improvement:String(j.improvement||"").slice(0,1200), expect:String(j.expect||"").slice(0,200),
      sourceVia: research.via||"", status:"pending", at:Date.now() };
    DB.improveProposals.push(pr);
    if(DB.improveProposals.length>20) DB.improveProposals=DB.improveProposals.slice(-20);
    saveDB();
    return { ok:true, proposal:pr };
  } finally { endJobMode(); }
}
async function applyImprovement(pr){
  const dept=pr.dept; const a=AGENTS[dept]; if(!a) return;
  const prior=knowledgeText(dept);
  const sys="너는 민앤팜 '"+a.no+" "+a.kr+"' 부서 수석 전문가다."+brandBlock()+" 아래 '승인된 개선안'을 기존 품질 공식에 통합·발전시켜라. 중복은 합치고 더 정교하게. 형식(이 형식만, 한국어):\n핵심 품질 공식: (5~8개 체크리스트)\n반드시 지킬 것: (3~5개)\n피해야 할 것: (2~4개)\n다음 학습 과제: (1~2개)";
  const out=await anthropic(sys, "[기존 품질 공식]\n"+(prior||"(없음)")+"\n\n[승인된 개선안]\n"+pr.title+"\n"+pr.improvement, 1600);
  commitKnowledge(dept, out, "자율 개선(승인): "+String(pr.title||"").slice(0,40));
  DB.deptMemory=DB.deptMemory||{}; DB.deptMemory[dept]=DB.deptMemory[dept]||[];
  DB.deptMemory[dept].push({ at:Date.now(), instruction:"[자율 개선 승인]", note:String(pr.title||"").slice(0,120) });
  DB.exp=DB.exp||{}; DB.exp[dept]=(DB.exp[dept]||0)+2;
  saveDB();
  try{ await absorbToLeader([dept], "자율 개선"); }catch(e){}
}
async function decideImprovement(id, approve, viaCode){
  const pr=(DB.improveProposals||[]).find(x=>String(x.id)===String(id) && x.status==="pending");
  if(!pr) return { ok:false, note:"제안을 찾을 수 없거나 이미 처리됨" };
  if(viaCode!=null && String(viaCode)!==String(pr.code)) return { ok:false, note:"코드 불일치" };
  if(approve){ try{ await applyImprovement(pr); }catch(e){ logError("applyImprovement", e); } pr.status="approved"; pr.decidedAt=Date.now(); logImproveDecision(pr, true); saveDB();
    kakaoNotify("✅ 자율 개선 반영됨 — "+pr.deptKr+": "+pr.title+"\n품질 공식에 통합됐어요. 이제 결과물에 반영됩니다.").catch(()=>{});
    return { ok:true, applied:true, proposal:pr };
  } else { pr.status="rejected"; pr.decidedAt=Date.now(); logImproveDecision(pr, false); saveDB();
    return { ok:true, applied:false, proposal:pr };
  }
}
app.post("/api/improve/propose", async (req,res)=>{
  try{ const b=req.body||{}; const r=await researchImprovement(b.dept||""); res.json(r); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.get("/api/improve/list", (req,res)=>{
  const rows=(DB.improveProposals||[]).slice(-12).reverse().map(p=>({ id:p.id, dept:p.dept, deptKr:p.deptKr, title:p.title, why:p.why, improvement:p.improvement, expect:p.expect, sourceVia:p.sourceVia, status:p.status, at:p.at }));
  res.json({ ok:true, proposals:rows, learnedFrom:(DB.improveLog||[]).length });
});
app.post("/api/improve/decide", async (req,res)=>{
  try{ const b=req.body||{}; const r=await decideImprovement(b.id, b.approve===true, null); res.json(r); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.get("/api/improve/approve", async (req,res)=>{ const r=await decideImprovement(req.query.id, true, req.query.code); res.send(r.ok?"✅ 개선안을 승인·반영했어요. 앱으로 돌아가셔도 됩니다.":("처리 실패: "+(r.note||""))); });
app.get("/api/improve/reject", async (req,res)=>{ const r=await decideImprovement(req.query.id, false, req.query.code); res.send(r.ok?"🚫 개선안을 거절했어요. 팀장이 이 결정을 학습합니다.":("처리 실패: "+(r.note||""))); });

// ===== 🌲 에버그린 재발행: 성과 좋았던(즐겨찾기) 콘텐츠를 새로 각색해 재활용 =====
//  성과 수치 API가 아직 없으므로 '직접 즐겨찾기 + 선택적 성과점수' 기준. (나중에 분석 API 붙이면 자동 성과 기준으로 승격)
function evergreenList(){
  const arr=(DB.evergreen||[]).slice();
  arr.sort((a,b)=> ((b.score||0)-(a.score||0)) || ((a.lastUsedAt||0)-(b.lastUsedAt||0)) );  // 성과 높은 것 → 오래 안 쓴 것 순환
  return arr;
}
app.get("/api/evergreen/list", (req,res)=>{
  const now=Date.now();
  res.json({ ok:true, items: evergreenList().slice(0,30).map(x=>({ id:x.id, text:x.text, note:x.note||"", score:x.score||0, at:x.at||0, lastUsedAt:x.lastUsedAt||0,
    daysIdle: x.lastUsedAt ? Math.round((now-x.lastUsedAt)/86400000) : Math.round((now-(x.at||now))/86400000) })) });
});
app.post("/api/evergreen/add", (req,res)=>{
  try{ const b=req.body||{}; const text=String(b.text||"").trim(); if(!text) return res.status(400).json({ error:"내용이 필요해요" });
    DB.evergreen=DB.evergreen||[];
    DB.evergreen.push({ id:String(Date.now())+"_"+Math.floor(Math.random()*1000), text:text.slice(0,4000), note:String(b.note||"").slice(0,140), score:Math.max(0,Math.min(100,+b.score||0)), at:Date.now(), lastUsedAt:0 });
    if(DB.evergreen.length>60) DB.evergreen=DB.evergreen.slice(-60);
    saveDB(); res.json({ ok:true, count:DB.evergreen.length });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.post("/api/evergreen/remove", (req,res)=>{ try{ const id=String((req.body||{}).id||""); DB.evergreen=(DB.evergreen||[]).filter(x=>String(x.id)!==id); saveDB(); res.json({ ok:true }); }catch(e){ res.status(500).json({ error:String(e.message||e) }); } });
app.post("/api/evergreen/refresh", async (req,res)=>{
  try{
    const b=req.body||{}; const it=(DB.evergreen||[]).find(x=>String(x.id)===String(b.id));
    if(!it) return res.status(404).json({ error:"항목을 찾을 수 없어요" });
    const sys="너는 민앤팜(고흥 특산물) 콘텐츠 리프레셔다."+brandBlock()+profileContext()+(knowledgeText("creation")?("\n\n[제작 품질 공식]\n"+knowledgeText("creation")):"")
      +"\n\n아래는 예전에 성과가 좋았던 콘텐츠다. 핵심 메시지·강점은 유지하되 표현·훅·예시·시의성을 새롭게 바꿔 '재게시용 새 버전'을 만들어라(예전 것 그대로 복붙 금지). 결과물만 출력.";
    const out=stripFences(await anthropic(sys, "[원본(성과 좋았던 콘텐츠)]\n"+String(it.text).slice(0,6000)+(it.note?("\n\n[메모] "+it.note):""), 1500));
    it.lastUsedAt=Date.now(); saveDB();
    res.json({ ok:true, refreshed:out });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// 성과 지표 수집 (플랫폼별 — 실제 API 연결 자리)
app.get("/api/metrics", async (req,res)=>{
  const pubs = (DB.jobs||[]).filter(j=>j.type==="publish");
  const byDay = {};
  pubs.forEach(j=>{ const d=new Date(j.at).toISOString().slice(0,10); byDay[d]=(byDay[d]||0)+(j.count||0); });
  res.json({ totalPublishes: pubs.reduce((a,j)=>a+(j.count||0),0), byDay, analytics: DB.analytics||{}, insight: (DB.analyticsInsight||{}) });
});

// ===== 유튜브 성과 추적: 발행한 영상의 조회·좋아요·댓글 수집 =====
// 최근 발행 기록에서 유튜브 영상 ID를 모아 videos.list(statistics)로 조회. 본인 소유 영상은 업로드 토큰으로 조회 가능.
function collectPublishedYouTubeIds(sinceDays){
  const since = Date.now() - (sinceDays||30)*86400000;
  const ids = {};   // videoId -> {title, at}
  (DB.jobs||[]).filter(j=>j.type==="publish" && j.at>=since && Array.isArray(j.published)).forEach(j=>{
    j.published.forEach(p=>{
      let vid = p.id || "";
      if (!vid && /youtu\.?be/.test(p.url||"")) { const m=(p.url||"").match(/(?:youtu\.be\/|v=)([\w-]{6,})/); if(m) vid=m[1]; }
      // 유튜브만: url에 youtu가 있거나 platform이 유튜브
      if (vid && (/youtu/.test(p.url||"") || p.platform==="유튜브")) ids[vid] = { title:p.title||"", at:p.at||j.at };
    });
  });
  return ids;
}
async function fetchYouTubeStats(videoIds){
  if (!videoIds.length) return {};
  const token = await getGoogleToken();
  const out = {};
  // 한 번에 50개까지
  for (let i=0;i<videoIds.length;i+=50){
    const batch = videoIds.slice(i,i+50);
    const url = "https://www.googleapis.com/youtube/v3/videos?part=statistics,snippet&id="+batch.join(",");
    const r = await fetch(url, { headers:{ "Authorization":"Bearer "+token } });
    if (!r.ok) throw new Error("유튜브 통계 조회 실패("+r.status+"): "+(await r.text().catch(()=>"")).slice(0,140));
    const d = await r.json();
    (d.items||[]).forEach(v=>{
      const s=v.statistics||{};
      out[v.id] = {
        title: (v.snippet&&v.snippet.title)||"",
        views: +(s.viewCount||0), likes: +(s.likeCount||0), comments: +(s.commentCount||0),
        publishedAt: (v.snippet&&v.snippet.publishedAt)||""
      };
    });
  }
  return out;
}
// 인스타/페북 발행 기록에서 게시물 ID 수집
function collectPublishedMetaIds(platform, sinceDays){
  const since = Date.now() - (sinceDays||30)*86400000;
  const ids = {};   // postId -> {title, at}
  (DB.jobs||[]).filter(j=>j.type==="publish" && j.at>=since && Array.isArray(j.published)).forEach(j=>{
    j.published.forEach(p=>{
      if (p.platform===platform && p.id) ids[p.id] = { title:p.title||"", at:p.at||j.at };
    });
  });
  return ids;
}
// 인스타 미디어 성과: like_count·comments_count(+가능하면 reach)
async function fetchInstagramStats(mediaIds){
  const token = process.env.IG_ACCESS_TOKEN;
  if (!token || !mediaIds.length) return {};
  const GV = "https://graph.facebook.com/v21.0/";
  const out = {};
  for (const id of mediaIds){
    try {
      const r = await fetch(GV+id+"?fields=like_count,comments_count,caption,media_type,permalink,timestamp&access_token="+encodeURIComponent(token));
      if (!r.ok) continue;
      const d = await r.json();
      let reach = 0;
      try { // reach는 insights에서만 (실패해도 무시)
        const ri = await fetch(GV+id+"/insights?metric=reach&access_token="+encodeURIComponent(token));
        if (ri.ok){ const ridata = await ri.json(); const m=(ridata.data||[]).find(x=>x.name==="reach"); if(m&&m.values&&m.values[0]) reach=+m.values[0].value||0; }
      } catch(e){}
      out[id] = {
        title: (d.caption||"").slice(0,60), likes:+(d.like_count||0), comments:+(d.comments_count||0),
        views: reach, permalink: d.permalink||"", publishedAt: d.timestamp||""
      };
    } catch(e){ /* 개별 실패 무시 */ }
  }
  return out;
}
// 페북 페이지 게시물 성과: 좋아요·댓글·공유
async function fetchFacebookStats(postIds){
  const token = process.env.FB_PAGE_TOKEN;
  if (!token || !postIds.length) return {};
  const GV = "https://graph.facebook.com/v21.0/";
  const out = {};
  for (const id of postIds){
    try {
      const r = await fetch(GV+id+"?fields=message,created_time,likes.summary(true),comments.summary(true),shares&access_token="+encodeURIComponent(token));
      if (!r.ok) continue;
      const d = await r.json();
      out[id] = {
        title: (d.message||"").slice(0,60),
        likes: +((d.likes&&d.likes.summary&&d.likes.summary.total_count)||0),
        comments: +((d.comments&&d.comments.summary&&d.comments.summary.total_count)||0),
        views: +((d.shares&&d.shares.count)||0), // 공유 수를 views 칸에 표시
        publishedAt: d.created_time||""
      };
    } catch(e){ /* 개별 실패 무시 */ }
  }
  return out;
}
// 성과 수집 + 분석부(04) 인사이트 생성 → 기획부(01)로 피드백 + 각 부서 학습
async function runAnalyticsCollect(force){
  const idMap = collectPublishedYouTubeIds(45);
  const vids = Object.keys(idMap);
  // 유튜브 성과
  let ytRows = [];
  if (vids.length){
    try {
      const stats = await fetchYouTubeStats(vids);
      ytRows = Object.keys(stats).map(id=>({ id, ...stats[id] })).sort((a,b)=> b.views - a.views);
    } catch(e){ /* 유튜브 실패해도 다른 채널 진행 */ }
  }
  // 인스타 성과
  let igRows = [];
  const igIds = Object.keys(collectPublishedMetaIds("인스타그램", 45));
  if (igIds.length){
    try { const s = await fetchInstagramStats(igIds); igRows = Object.keys(s).map(id=>({ id, ...s[id] })).sort((a,b)=>(b.likes+b.comments)-(a.likes+a.comments)); } catch(e){}
  }
  // 페북 성과
  let fbRows = [];
  const fbIds = Object.keys(collectPublishedMetaIds("페이스북", 45));
  if (fbIds.length){
    try { const s = await fetchFacebookStats(fbIds); fbRows = Object.keys(s).map(id=>({ id, ...s[id] })).sort((a,b)=>(b.likes+b.comments)-(a.likes+a.comments)); } catch(e){}
  }
  if (!ytRows.length && !igRows.length && !fbRows.length){
    return { ok:true, note:"추적할 발행 기록이 없어요(유튜브·인스타·페북에 발행 후 실행하세요).", count:0 };
  }
  const totalize = rows => rows.reduce((a,v)=>({ views:a.views+(v.views||0), likes:a.likes+(v.likes||0), comments:a.comments+(v.comments||0) }), {views:0,likes:0,comments:0});
  DB.analytics = DB.analytics || {};
  const now = Date.now();
  if (ytRows.length) DB.analytics.youtube   = { collectedAt: now, videos: ytRows, totals: totalize(ytRows) };
  if (igRows.length) DB.analytics.instagram = { collectedAt: now, videos: igRows, totals: totalize(igRows) };
  if (fbRows.length) DB.analytics.facebook  = { collectedAt: now, videos: fbRows, totals: totalize(fbRows) };
  saveDB();
  // 강민서(04) 분석 코멘트 — 채널 통합, 다음 기획에 반영할 시사점
  try {
    const secTop = (label, rows, viewLabel) => rows.length
      ? ("["+label+" 상위]\n"+rows.slice(0,4).map(v=>"· "+(v.title||v.id)+" — "+viewLabel+" "+(v.views||0)+" · 좋아요 "+(v.likes||0)+" · 댓글 "+(v.comments||0)).join("\n"))
      : "";
    const ctx = [ secTop("유튜브", ytRows, "조회"), secTop("인스타그램", igRows, "도달"), secTop("페이스북", fbRows, "공유") ].filter(Boolean).join("\n\n");
    const a = AGENTS["analytics"];
    const sys = "너는 SNS 자동화 회사의 '"+a.no+" "+a.kr+"' 담당 "+(MEMBERS["analytics"]||"강민서")+"다. 아래 채널별 성과 데이터를 종합해, 다음 콘텐츠 기획에 바로 반영할 시사점을 뽑아라. 형식(이 형식만):\n요약: (한 줄, 전체 성과 진단)\n채널별 진단: (유튜브·인스타·페북 중 데이터 있는 채널만 한 줄씩, 어디가 잘되는지)\n잘된 것: (반응 높은 콘텐츠의 공통점 1~2개)\n다음 기획 제안: (기획부가 다음에 만들면 좋을 주제·형식·채널 2~3개, 데이터 근거와 함께)\n군더더기 없이, 숫자 근거로, 한국어로만.";
    const insight = await genText(sys, ctx, 800, workEngine());
    DB.analyticsInsight = { at: now, by: MEMBERS["analytics"]||"강민서", text: insight };
    DB.state = DB.state || {};
    DB.state.deptDirective = DB.state.deptDirective || {};
    DB.state.deptDirective["strategy"] = "[데이터 피드백] "+String(insight).replace(/\n/g," ").slice(0,240);
    DB.deptMemory = DB.deptMemory || {}; DB.deptMemory["analytics"] = DB.deptMemory["analytics"]||[];
    DB.deptMemory["analytics"].push({ at:now, instruction:"[채널 성과 분석]", note:String(insight).slice(0,300) });
    DB.exp = DB.exp || {}; DB.exp["analytics"] = (DB.exp["analytics"]||0)+1;
    saveDB();
  } catch(e){ /* 인사이트 실패해도 데이터는 저장됨 */ }
  // 최적 발행시간 분석(요일×시간대) — 성과 데이터를 예약발행에 활용
  try { computeBestTimes(); } catch(e){}
  // 성과를 각 부서의 '학습 신호'로 주입 — 실제 숫자로 부서 전문성이 성장하게(핵심)
  try { await injectPerformanceLearning(now); } catch(e){}
  return { ok:true, count: ytRows.length+igRows.length+fbRows.length, analytics: DB.analytics, insight: DB.analyticsInsight, bestTimes: DB.bestTimes||{} };
}
// 요일×시간대별 평균 반응으로 최적 발행 시간대 산출
function computeBestTimes(){
  const buckets = {};   // "dow-hour" -> {score, n}
  const add = (iso, engagement) => {
    if(!iso) return;
    const d = new Date(iso); if(isNaN(d)) return;
    // KST 기준 요일·시간
    const k = new Date(d.getTime()+9*3600000);
    const key = k.getUTCDay()+"-"+k.getUTCHours();
    buckets[key] = buckets[key] || { score:0, n:0 };
    buckets[key].score += engagement; buckets[key].n += 1;
  };
  const an = DB.analytics || {};
  ["youtube","instagram","facebook"].forEach(ch=>{
    ((an[ch]&&an[ch].videos)||[]).forEach(v=>{
      const eng = (+v.views||0)*0.2 + (+v.likes||0) + (+v.comments||0)*2; // 댓글>좋아요>조회 가중
      add(v.publishedAt, eng);
    });
  });
  const rows = Object.keys(buckets).map(key=>{
    const [dow,hour]=key.split("-").map(Number);
    return { dow, hour, avg: buckets[key].score/Math.max(1,buckets[key].n), n:buckets[key].n };
  }).filter(x=>x.n>=1).sort((a,b)=>b.avg-a.avg);
  const dowKr=["일","월","화","수","목","금","토"];
  DB.bestTimes = { at: Date.now(),
    top: rows.slice(0,5).map(r=>({ label: dowKr[r.dow]+"요일 "+String(r.hour).padStart(2,"0")+"시", dow:r.dow, hour:r.hour, avg:Math.round(r.avg), n:r.n })),
    basis: rows.length };
  saveDB();
  return DB.bestTimes;
}
// 성과 데이터를 부서별 학습 신호로 변환해 주입 → distillKnowledge가 실제 숫자로 전문성 강화
async function injectPerformanceLearning(now){
  const an = DB.analytics || {};
  const all = [].concat((an.youtube&&an.youtube.videos)||[], (an.instagram&&an.instagram.videos)||[], (an.facebook&&an.facebook.videos)||[]);
  if (!all.length) return;
  // 상·하위 콘텐츠 대비로 '무엇이 통했나'를 뽑아 각 부서에 다른 관점으로 기록
  const scored = all.map(v=>({ v, eng:(+v.views||0)*0.2+(+v.likes||0)+(+v.comments||0)*2 })).sort((a,b)=>b.eng-a.eng);
  const top = scored.slice(0,3).map(x=>x.v.title||x.v.id).filter(Boolean);
  const low = scored.slice(-3).map(x=>x.v.title||x.v.id).filter(Boolean);
  const bt = (DB.bestTimes&&DB.bestTimes.top&&DB.bestTimes.top[0]) ? DB.bestTimes.top[0].label : "";
  DB.deptMemory = DB.deptMemory || {}; DB.exp = DB.exp || {};
  // 부서별 맞춤 학습 신호(성과 피드백은 distillKnowledge가 강한 신호로 학습함)
  const feed = {
    strategy:   "[성과 피드백] 반응 높았던 주제: "+(top.join(", ")||"-")+" / 저조: "+(low.join(", ")||"-")+(bt?(" / 최적 발행시간: "+bt):"")+". 다음 기획은 반응 높은 쪽 특성을 강화하고 저조한 패턴은 피하라.",
    creation:   "[성과 피드백] 잘 통한 콘텐츠: "+(top.join(", ")||"-")+". 이 콘텐츠들의 제목·후킹·구성 방식을 다음 제작에 재사용하라. 저조: "+(low.join(", ")||"-"),
    publishing: "[성과 피드백] 반응 좋은 시간대: "+(bt||"데이터 축적 중")+". 예약발행을 이 시간대로 맞추면 도달이 올라간다.",
    scout:      "[성과 피드백] 실제로 통한 소재: "+(top.join(", ")||"-")+". 이런 결의 트렌드·소재를 우선 발굴하라."
  };
  Object.keys(feed).forEach(d=>{
    if(!AGENTS[d]) return;
    DB.deptMemory[d] = DB.deptMemory[d] || [];
    DB.deptMemory[d].push({ at:now||Date.now(), instruction:"[성과 피드백]", note:feed[d] });
    // 성과 학습이 쌓이면 즉시 전문성 갱신(부서가 실제 숫자로 똑똑해짐)
    distillKnowledge(d).catch(()=>{});
  });
  saveDB();
}
app.post("/api/analytics/collect", async (req,res)=>{
  try { res.json(await runAnalyticsCollect(true)); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.get("/api/analytics", (req,res)=> res.json({ analytics: DB.analytics||{}, insight: DB.analyticsInsight||{} }));

// ===== 블로그 내부링크 아카이브 =====
app.get("/api/blog-archive", (req,res)=> res.json((DB.blogArchive||[]).slice(-100).reverse()));
app.post("/api/blog-archive", (req,res)=>{
  try { const b=req.body||{}; if(!b.title) return res.status(400).json({ error:"제목 필요" });
    res.json({ ok:true, item: archiveBlogPost({ title:b.title, url:b.url||"", topic:b.topic||b.title, tags:b.tags||[] }) });
  } catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.post("/api/blog-archive/delete", (req,res)=>{
  const t=String((req.body&&req.body.title)||""), u=String((req.body&&req.body.url)||"");
  const before=(DB.blogArchive||[]).length;
  DB.blogArchive=(DB.blogArchive||[]).filter(x=> !((u&&x.url===u)||(t&&x.title===t)) );
  saveDB(); res.json({ ok:true, removed: before-(DB.blogArchive||[]).length });
});

// ===== OAuth 토큰 발급 도우미 =====
// 유튜브: /api/connect/youtube 접속 → 구글 동의 → refresh_token 발급 → 환경변수에 저장
app.get("/api/connect/youtube", (req,res)=>{
  if(!process.env.GOOGLE_CLIENT_ID) return res.send("GOOGLE_CLIENT_ID 환경변수를 먼저 설정하세요.");
  const redirect = (process.env.PUBLIC_BASE || (req.protocol+"://"+req.get("host"))) + "/api/connect/youtube/callback";
  const u = "https://accounts.google.com/o/oauth2/v2/auth?" + new URLSearchParams({
    client_id: process.env.GOOGLE_CLIENT_ID, redirect_uri: redirect, response_type:"code",
    scope:"https://www.googleapis.com/auth/youtube.upload https://www.googleapis.com/auth/drive.file", access_type:"offline", prompt:"consent"
  });
  res.redirect(u);
});
app.get("/api/connect/youtube/callback", async (req,res)=>{
  try{
    const redirect = (process.env.PUBLIC_BASE || (req.protocol+"://"+req.get("host"))) + "/api/connect/youtube/callback";
    const r = await fetch("https://oauth2.googleapis.com/token", {
      method:"POST", headers:{ "Content-Type":"application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        code:req.query.code, client_id:process.env.GOOGLE_CLIENT_ID, client_secret:process.env.GOOGLE_CLIENT_SECRET,
        redirect_uri:redirect, grant_type:"authorization_code"
      })
    });
    const d = await r.json();
    res.send("<pre>아래 refresh_token 값을 YT_REFRESH_TOKEN 환경변수에 저장하세요.\n(이 토큰 하나로 유튜브 업로드 + 구글 드라이브 저장이 모두 됩니다.)\n\nrefresh_token: "+(d.refresh_token||"(없음 — prompt=consent로 다시 시도)")+"</pre>");
  }catch(e){ res.send("오류: "+(e.message||e)); }
});

// ===== 백그라운드 워커 (근무시간 기반 자율 작업) =====
function isWorking(){
  const s = DB.state?.schedule;
  if (!s) return true; // 스케줄 없으면 상시
  const now = new Date(), mins = now.getHours()*60 + now.getMinutes();
  if (s.mode === "hours") {
    const st = toMin(s.start), en = toMin(s.end);
    const on = st<en ? (mins>=st && mins<en) : (mins>=st || mins<en);
    if (!on) return false;
  }
  const cycle = (s.workMin||1) + (s.restMin||0);
  if (cycle<=0) return true;
  const base = s.mode==="hours" ? toMin(s.start) : 0;
  const into = (((mins-base)%cycle)+cycle)%cycle;
  return into < s.workMin;
}
function toMin(hhmm){ const p=(hhmm||"0:0").split(":"); return (+p[0])*60+(+p[1]); }

// ===================== 🔄 선순환 파이프라인 =====================
// 내가 준 URL로 학습 → 결과물 생성(SNS/상품페이지) → 개선자료 수집 → 재생성 → 배운 것 부서에 누적.
// 돌릴수록 부서 지식이 쌓여 다음 사이클이 더 좋아진다. (허공 자동학습과 무관 — 내가 지정한 것만 실행)
function trimCycleJobs(){
  DB.cycleJobs = DB.cycleJobs || [];
  if (DB.cycleJobs.length > 30) DB.cycleJobs = DB.cycleJobs.slice(-30);
  const n = DB.cycleJobs.length, keep = 8;   // 최근 8개만 상세(HTML 등) 유지, 이전 것은 용량 절약
  DB.cycleJobs.forEach((j,i)=>{ if(i < n-keep && j.results){
    if(j.results.page){ j.results.pageDropped=true; delete j.results.page; }
    if(j.results.home){ j.results.homeDropped=true; delete j.results.home; }
    if(j.results.report){ j.results.reportDropped=true; delete j.results.report; }
  } });
}
// 이번 결과에서 잘 된 요소를 짧은 노하우로 뽑아 부서 지식에 누적
async function accumulateFromResult(dept, topic, resultText, research){
  try{
    const a = AGENTS[dept]; if(!a) return;
    const sys = "너는 민앤팜(고흥 특산물)의 '"+a.kr+"' 부서 수석이다. 방금 '"+topic+"' 결과물을 만들었다. "
      + "이번에 실제로 효과적이었던 구체적 요소·패턴을, 다음에 그대로 재사용할 '짧은 품질 노하우'로만 3~5줄 뽑아라. "
      + "일반론·교과서 문구 말고, 이번 결과에서 잘 작동한 실전 기법만. 한국어 불릿만 출력.";
    const user = "[이번 결과물 발췌]\n"+String(resultText||"").slice(0,2500)+(research?("\n\n[활용한 조사자료]\n"+String(research).slice(0,800)):"");
    const nugget = await anthropic(sys, user, 500);
    if(nugget && nugget.trim().length>10){
      commitKnowledge(dept, nugget.trim(), "선순환 결과 누적("+String(topic).slice(0,20)+")");
      DB.exp = DB.exp || {}; DB.exp[dept] = (DB.exp[dept]||0) + 2;
      recordLearnLog({ dept, deptKr:a.kr, kind:"cycle", ok:true, note:"선순환 결과 누적 → 품질 노하우 갱신: "+String(topic).slice(0,40) });
      try{ await absorbToLeader([dept], "선순환"); }catch(_){}
    }
  }catch(e){ logError("accumulate:"+dept, e); }
}
// 한 사이클 실행(백그라운드)
// 결과물 종류별 사양 (텍스트형: SNS/블로그/유튜브 — 모두 제작부 creation)
var CYCLE_FORMATS = {
  sns:     { dept:"creation", label:"SNS 게시물", instruct:"바로 게시 가능한 완성형 SNS 게시물(플랫폼에 맞는 카피 + 해시태그 8~15개). 첫 줄에서 스크롤을 멈추게 하는 후킹.", rq:"SNS 후킹·차별화·최신 트렌드" },
  blog:    { dept:"creation", label:"블로그 글",  instruct:"검색 상위노출을 노린 완성형 블로그 글: ① 클릭되는 제목(H1, 핵심 키워드 포함) ② 첫 문단 3줄 안에 후킹 ③ ##/### 소제목으로 스캔 가능한 구조 ④ 경험·전문성·신뢰(E-E-A-T) ⑤ 자연스러운 CTA ⑥ 끝에 메타설명·추천 태그. 마크다운으로 완성.", rq:"블로그 상위노출·제목·구조·가독성 최신 요령" },
  cafe:    { dept:"advisory", label:"카페글", instruct:"네이버 카페·다음 카페에 그대로 붙여넣을 수 있는 완성형 커뮤니티 글: ① 카페 게시판에서 눌리는 제목 3안 ② 홍보 티가 나지 않는 경험담·후기·정보공유 톤(1인칭, 솔직하게, 광고 문구 금지) ③ 사진 들어갈 자리를 [사진1: 어떤 사진인지] 형태로 표시 ④ 회원들이 댓글 달고 싶어지는 질문으로 마무리 ⑤ 마지막에 '네이버 카페 팁'과 '다음 카페 팁'을 각각 1~2줄(말머리·등업·홍보글 규정 주의). 카페는 대놓고 홍보하면 삭제·강퇴 대상이니 반드시 정보·경험 중심으로.", rq:"네이버 카페 다음 카페 홍보 글 잘 쓰는 법, 카페 후기글·정보글 구조와 규정" },
  youtube: { dept:"creation", label:"유튜브 기획", instruct:"유튜브 완성형 기획안: ① 후킹 제목 3안 ② 타깃·핵심 메시지 ③ 영상 구성(도입 훅→전개→마무리 CTA, 타임라인) ④ 나레이션/자막 대본 ⑤ 썸네일 문구 2안 ⑥ 설명란·태그. 바로 촬영·편집 가능하게 구체적으로.", rq:"유튜브 조회수 잘 나오는 후킹·구성·썸네일 최신" }
};
// 텍스트형 결과물 한 종류 생성(학습→생성→보강→재생성→누적)
async function cycleGenText(job, fmt, setStep){
  const spec = CYCLE_FORMATS[fmt]; const d = spec.dept; const a = AGENTS[d];
  const url = String(job.url||"").trim(), topic = String(job.topic||"").trim(), info = String(job.info||"").slice(0,2000);
  const memo = String(job.memo||"").trim();
  if(url){ setStep(spec.label+" · 참고 URL 학습 중"); try{ await learnFromWebUrls(d, [url]); }catch(e){ job._warn=(job._warn||"")+spec.label+" 학습 일부 실패; "; } }
  setStep(spec.label+" · 생성 중");
  let sys = "너는 민앤팜(전남 고흥 특산물)의 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role+ADDRESS+STYLE+personaLine(d);
  const kb = knowledgeText(d); if(kb) sys += "\n\n[이 부서가 축적한 전문성(품질 공식·클라이언트 취향)]\n"+kb;
  sys += brandBlock() + profileContext();
  if(memo) sys += "\n\n[클라이언트의 이번 지시·요구 — 반드시 반영]\n"+memo;
  sys += " 아래 주제로 "+spec.instruct+" 되묻지 말고 완성본만, 한국어로.";
  const userMsg = "주제: "+topic+(url?("\n참고 URL(학습 반영됨): "+url):"")+(info?("\n추가 정보: "+info):"")+(memo?("\n지시: "+memo):"");
  let out = "";
  try{ const g = await createReviewedContent(sys, userMsg, topic, workEngine()); out = g.content||"";
       if(g.viral){ job._viral = job._viral||{}; job._viral[fmt] = g.viral; } }
  catch(e){ out = await genText(sys, userMsg, 1800, workEngine()); }
  setStep(spec.label+" · 개선 자료 수집 중");
  let research = "";
  try{ const r = await researchWeb(topic+" "+spec.rq, 1100); research = String(r.text||"").trim(); }catch(_){}
  if(research && out){
    setStep(spec.label+" · 자료 반영해 재생성 중");
    try{
      const rev = await createReviewedContent(
        sys+"\n\n[추가 조사자료 — 이 좋은 요소를 반영해 더 좋게]\n"+research,
        userMsg+"\n\n(아래 초안을 위 조사자료로 더 완성도 높게 개선한 최종본만 출력)\n\n[초안]\n"+out,
        topic, workEngine());
      if(rev.content && rev.content.length>50){ out = rev.content;
        if(rev.viral){ job._viral = job._viral||{}; job._viral[fmt] = rev.viral; } }
    }catch(_){}
  }
  setStep(spec.label+" · 배운 것 누적 중");
  await accumulateFromResult(d, topic, out, research);
  return out;
}
async function runCycleJob(id, appUrl){
  const job = (DB.cycleJobs||[]).find(j=>j.id===id);
  if(!job) return;
  const setStep = (s)=>{ job.step = s; job.progAt = Date.now(); saveDB(); };
  job.status = "running"; job.startedAt = Date.now(); setStep("시작");
  beginJobMode();
  try{
    const topic = String(job.topic||"").trim();
    const info = String(job.info||"").slice(0,2000);
    const memo = String(job.memo||"").trim();
    const results = {};
    const o = job.outputs || {};

    if(o.sns)     results.sns     = await cycleGenText(job, "sns", setStep);
    if(o.blog)    results.blog    = await cycleGenText(job, "blog", setStep);
    if(o.cafe)    results.cafe    = await cycleGenText(job, "cafe", setStep);
    if(o.youtube) results.youtube = await cycleGenText(job, "youtube", setStep);

    // ---------- 📊 리서치 보고서 (HTML) ----------
    if(o.report){
      setStep("리서치 보고서 · 시작");
      try{
        const genR = await reportPipeline({ topic: topic, info: info, memo: memo, url: String(job.url||"") },
                                          (s2)=>setStep("리서치 보고서 · "+s2));
        results.report = genR.html || "";
        results.reportTopic = topic;
        results.reportReviews = genR.reviews || [];
      }catch(e){ job._warn=(job._warn||"")+"보고서 생성 실패: "+String(e.message||e).slice(0,60)+"; "; }
    }

    // ---------- 홈페이지 (HTML) ----------
    if(o.home){
      const dh = "analytics"; const urlH = String(job.url||"").trim();
      if(urlH){ setStep("홈페이지 · 참고 URL 학습 중"); try{ await learnFromWebUrls(dh, [urlH]); }catch(e){ job._warn=(job._warn||"")+"홈페이지 학습 일부 실패; "; } }
      setStep("홈페이지 · 초안 생성 중");
      let homeHtml = "";
      try{
        const genH = await productPagePipeline({ kind:"home", dept:dh, product: topic, info: info+(memo?("\n[지시] "+memo):""), brand:"민앤팜" }, (s2)=>setStep("홈페이지 · "+s2));
        homeHtml = genH.html || "";
      }catch(e){ job._warn=(job._warn||"")+"홈페이지 생성 실패: "+String(e.message||e).slice(0,60)+"; "; }
      let homeResearch = "";
      if(homeHtml){
        setStep("홈페이지 · 개선 자료 수집 중");
        try{ const r = await researchWeb(topic+" 브랜드 공식 홈페이지 잘 만든 사례 구성·카피·신뢰", 1100); homeResearch = String(r.text||"").trim(); }catch(_){}
        if(homeResearch){
          setStep("홈페이지 · 자료 반영해 재생성 중");
          try{
            const revH = await productPagePipeline({ kind:"home", dept:dh, product: topic, info: info, baseHtml: homeHtml,
              feedback: "아래 최신 조사자료의 좋은 요소(구성·카피·신뢰 장치)를 반영해 더 완성도 높게 개선해줘. 전체 톤·구조는 유지하고 개선점만 자연스럽게:\n"+homeResearch }, (s2)=>setStep("홈페이지 · "+s2));
            if(revH.html && revH.html.length>200) homeHtml = revH.html;
          }catch(_){}
        }
      }
      results.home = homeHtml; results.homeTopic = topic;
      setStep("홈페이지 · 배운 것 누적 중");
      await accumulateFromResult(dh, topic, "(홈페이지 HTML 생성 완료: "+topic+")", homeResearch);
    }

    // ---------- 상품페이지 (HTML) ----------
    if(o.page){
      const d = "monetization"; const url = String(job.url||"").trim();
      if(url){ setStep("상품페이지 · 참고 URL 학습 중"); try{ await learnFromWebUrls(d, [url]); }catch(e){ job._warn=(job._warn||"")+"페이지 학습 일부 실패; "; } }
      setStep("상품페이지 · 초안 생성 중");
      let pageHtml = "";
      try{
        const gen = await productPagePipeline({ product: topic, info: info+(memo?("\n[지시] "+memo):""), brand: "민앤팜" }, (s)=>setStep("상품페이지 · "+s));
        pageHtml = gen.html || "";
      }catch(e){ job._warn=(job._warn||"")+"페이지 생성 실패: "+String(e.message||e).slice(0,60)+"; "; }
      let pageResearch = "";
      if(pageHtml){
        setStep("상품페이지 · 개선 자료 수집 중");
        try{ const r = await researchWeb(topic+" 상품 상세페이지 잘 만든 사례 구성·카피·전환", 1100); pageResearch = String(r.text||"").trim(); }catch(_){}
        if(pageResearch){
          setStep("상품페이지 · 자료 반영해 재생성 중");
          try{
            const rev = await productPagePipeline({ product: topic, info: info, baseHtml: pageHtml,
              feedback: "아래 최신 조사자료의 좋은 요소(구성·카피·전환 장치)를 반영해 더 완성도 높게 개선해줘. 전체 톤·구조는 유지하고 개선점만 자연스럽게:\n"+pageResearch }, (s)=>setStep("상품페이지 · "+s));
            if(rev.html && rev.html.length>200) pageHtml = rev.html;
          }catch(_){}
        }
      }
      results.page = pageHtml; results.pageProduct = topic;
      setStep("상품페이지 · 배운 것 누적 중");
      await accumulateFromResult(d, topic, "(상품페이지 HTML 생성 완료: "+topic+")", pageResearch);
    }

    if(job._viral) results.viral = job._viral;
    job.results = results;
    job.status = "done"; job.doneAt = Date.now(); job.step = "완료"; trimCycleJobs(); saveDB();
    const parts = []; if(results.sns) parts.push("SNS"); if(results.blog) parts.push("블로그"); if(results.cafe) parts.push("카페글"); if(results.youtube) parts.push("유튜브"); if(results.page) parts.push("상품페이지"); if(results.home) parts.push("홈페이지"); if(results.report) parts.push("리서치 보고서");
    const base = String(appUrl||(DB.state&&DB.state.appBase)||"").replace(/\/$/,"");
    let vsum = "";
    if(results.viral){ const vs=Object.keys(results.viral).map(k=>{ const L=(CYCLE_FORMATS[k]?CYCLE_FORMATS[k].label:k); return L+" "+results.viral[k].score+"점"; }); if(vs.length) vsum = "\n🔥 예상 반응: "+vs.join(" · "); }
    try{ kakaoNotify("🔄 선순환 완료 — "+topic+"\n생성: "+(parts.join(" + ")||"(결과 없음)")+vsum+(job._warn?("\n⚠️ "+job._warn):"")+"\n앱 '📚 학습 > 🔄 선순환'에서 결과를 확인하고, 마음에 안 들면 '🔁 고쳐줘'로 수정·학습시키세요.", base); }catch(_){}
  }catch(e){
    job.status = "error"; job.error = String(e.message||e); job.doneAt = Date.now(); job.step = "실패"; saveDB();
    try{ kakaoNotify("⚠️ 선순환 실패 — "+String(job.topic||"")+"\n"+String(e.message||e).slice(0,140)); }catch(_){}
    logError("runCycleJob", e);
  }finally{ endJobMode(); }
}
// 지금 실행
app.post("/api/cycle/start", (req,res)=>{
  try{
    const b = req.body || {};
    const url = String(b.url||"").trim();
    const topic = String(b.topic||"").trim();
    const sns = !!b.sns, blog = !!b.blog, youtube = !!b.youtube, page = !!b.page, home = !!b.home, cafe = !!b.cafe, report = !!b.report;
    if(!topic) return res.status(400).json({ error:"주제(무엇을 만들지)를 입력하세요" });
    if(!sns && !blog && !youtube && !page && !home && !cafe && !report) return res.status(400).json({ error:"결과물(SNS·블로그·카페글·유튜브·상품페이지·홈페이지·리서치보고서) 중 하나 이상 선택하세요" });
    if(url && !/^https?:\/\/.+/.test(url)) return res.status(400).json({ error:"URL은 https:// 로 시작해야 해요 (비워도 됨)" });
    DB.cycleJobs = DB.cycleJobs || [];
    // 🎤 사전 인터뷰 답변이 있으면 '맥락'으로 memo에 합쳐 모든 부서 프롬프트에 주입
    let memo = String(b.memo||"").slice(0,800);
    const itv = String(b.interview||"").trim();
    if(itv) memo = (memo?(memo+"\n\n"):"") + "[사전 인터뷰 — 클라이언트가 직접 답한 방향. 이 맥락에 정확히 맞춰라]\n" + itv.slice(0,1500);
    const job = { id:String(Date.now())+"_c"+Math.floor(Math.random()*1000), url, topic, info:String(b.info||"").slice(0,2000), memo:memo.slice(0,2500),
      outputs:{ sns, blog, youtube, page, home, cafe, report }, status:"queued", at:Date.now(), source:"now", interviewed:!!itv };
    DB.cycleJobs.push(job); trimCycleJobs(); saveDB();
    res.json({ ok:true, jobId:job.id, status:"queued" });
    setImmediate(()=>{ runCycleJob(job.id, String(b.appUrl||"")).catch(e=>logError("cycle-start", e)); });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 상태 조회
app.get("/api/cycle/status", (req,res)=>{
  const j = (DB.cycleJobs||[]).find(x=>x.id===String(req.query.id||""));
  if(!j) return res.status(404).json({ error:"작업을 찾을 수 없어요" });
  res.json({ ok:true, id:j.id, status:j.status, step:j.step||"", error:j.error||"", warn:j._warn||"",
    topic:j.topic, outputs:j.outputs, doneAt:j.doneAt||0,
    results: j.results ? { sns:j.results.sns||"", blog:j.results.blog||"", cafe:j.results.cafe||"", youtube:j.results.youtube||"", page:j.results.page||"", home:j.results.home||"", report:j.results.report||"", pageProduct:j.results.pageProduct||"", homeTopic:j.results.homeTopic||"", pageDropped:!!j.results.pageDropped, homeDropped:!!j.results.homeDropped } : null });
});
// 목록(결과 이력)
app.get("/api/cycle/list", (req,res)=>{
  const jobs = (DB.cycleJobs||[]).slice(-12).reverse().map(j=>({
    id:j.id, status:j.status, step:j.step||"", topic:j.topic, outputs:j.outputs, at:j.at, doneAt:j.doneAt||0, source:j.source||"now",
    hasSns: !!(j.results&&j.results.sns), hasBlog: !!(j.results&&j.results.blog), hasCafe: !!(j.results&&j.results.cafe), hasYoutube: !!(j.results&&j.results.youtube), hasPage: !!(j.results&&j.results.page), hasHome: !!(j.results&&j.results.home), hasReport: !!(j.results&&j.results.report), viral:(j.results&&j.results.viral)||null, pageDropped: !!(j.results&&j.results.pageDropped), homeDropped: !!(j.results&&j.results.homeDropped), error:j.error||"" }));
  res.json({ ok:true, jobs });
});
// 결과 하나 상세(SNS 본문 / 페이지 HTML)
app.get("/api/cycle/result", (req,res)=>{
  const j = (DB.cycleJobs||[]).find(x=>x.id===String(req.query.id||""));
  if(!j || !j.results) return res.status(404).json({ error:"결과가 없어요" });
  res.json({ ok:true, id:j.id, topic:j.topic, viral:j.results.viral||null, sns:j.results.sns||"", blog:j.results.blog||"", cafe:j.results.cafe||"", youtube:j.results.youtube||"", page:j.results.page||"", home:j.results.home||"", report:j.results.report||"", reportTopic:j.results.reportTopic||"", pageProduct:j.results.pageProduct||"", homeTopic:j.results.homeTopic||"", pageDropped:!!j.results.pageDropped, homeDropped:!!j.results.homeDropped });
});
// 예약 등록/목록/삭제 (1회/매일/매주)
app.post("/api/cycle/reserve", (req,res)=>{
  try{
    const b = req.body || {};
    const url = String(b.url||"").trim();
    const topic = String(b.topic||"").trim();
    const sns = !!b.sns, blog = !!b.blog, youtube = !!b.youtube, page = !!b.page, home = !!b.home, cafe = !!b.cafe, report = !!b.report;
    const repeat = (["once","daily","weekly"].indexOf(b.repeat)>=0) ? b.repeat : "once";
    const time = /^\d{1,2}:\d{2}$/.test(String(b.time||"")) ? String(b.time) : "";
    const days = Array.isArray(b.days) ? b.days.map(n=>parseInt(n,10)).filter(n=>n>=0&&n<=6) : [];
    if(!topic) return res.status(400).json({ error:"주제를 입력하세요" });
    if(!sns && !blog && !youtube && !page && !home && !cafe && !report) return res.status(400).json({ error:"결과물을 하나 이상 선택하세요" });
    if(!time) return res.status(400).json({ error:"실행 시각(HH:MM)을 입력하세요" });
    if(url && !/^https?:\/\/.+/.test(url)) return res.status(400).json({ error:"URL은 https:// 로 시작해야 해요" });
    if(repeat==="weekly" && !days.length) return res.status(400).json({ error:"매주 예약은 요일을 하나 이상 선택하세요" });
    DB.cycleReservations = DB.cycleReservations || [];
    const rv = { id:String(Date.now())+"_r"+Math.floor(Math.random()*1000), url, topic, info:String(b.info||"").slice(0,2000), memo:String(b.memo||"").slice(0,800),
      sns, blog, youtube, page, home, cafe, report, repeat, time, days, lastRunDay:"", at:Date.now() };
    DB.cycleReservations.push(rv); saveDB();
    res.json({ ok:true, reservation:rv });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.get("/api/cycle/reservations", (req,res)=>{
  res.json({ ok:true, reservations:(DB.cycleReservations||[]).map(r=>({
    id:r.id, url:r.url, topic:r.topic, memo:r.memo||"", sns:!!r.sns, blog:!!r.blog, cafe:!!r.cafe, youtube:!!r.youtube, page:!!r.page, home:!!r.home, report:!!r.report, repeat:r.repeat, time:r.time, days:r.days||[], lastRunDay:r.lastRunDay||"" })) });
});
app.post("/api/cycle/reserve/delete", (req,res)=>{
  const id = String((req.body||{}).id||"");
  DB.cycleReservations = (DB.cycleReservations||[]).filter(r=>r.id!==id);
  saveDB();
  res.json({ ok:true });
});
// 🔁 피드백 학습 루프: 결과가 마음에 안 들면 고쳐줘 → 재생성 + (원하면) 고정 취향으로 부서에 누적
app.post("/api/cycle/feedback", (req,res)=>{
  const b = req.body || {};
  const job = (DB.cycleJobs||[]).find(x=>x.id===String(b.id||""));
  if(!job || !job.results) return res.status(404).json({ error:"결과를 찾을 수 없어요" });
  const kind = String(b.kind||"");
  const feedback = String(b.feedback||"").slice(0,1500).trim();
  const remember = !!b.remember;
  if(!feedback) return res.status(400).json({ error:"어떻게 고칠지 입력하세요" });
  if(["sns","blog","cafe","youtube","page","home","report"].indexOf(kind)<0) return res.status(400).json({ error:"결과물 종류 오류" });
  if(!job.results[kind]) return res.status(404).json({ error:"해당 결과물이 없어요" });
  res.json({ ok:true, jobId:job.id, status:"started" });
  setImmediate(async ()=>{
    beginJobMode();
    const prevStatus = job.status;
    try{
      job.status = "running"; job.step = "🔁 수정 반영 중"; job.progAt = Date.now(); saveDB();
      if(kind === "report"){
        const cur = job.results.report || "";
        const rev = await reportPipeline({ topic: job.results.reportTopic||job.topic, info: job.info, memo: job.memo,
          baseHtml: cur, feedback: feedback }, (s2)=>{ job.step="🔁 보고서 "+s2; job.progAt=Date.now(); saveDB(); });
        if(rev.html && rev.html.length>200) job.results.report = rev.html;
        if(remember){ commitKnowledge("ops", "클라이언트 고정 취향(리서치 보고서): "+feedback, "피드백 학습");
          recordLearnLog({ dept:"ops", deptKr:(AGENTS.ops?AGENTS.ops.kr:"총괄"), kind:"feedback", ok:true, note:"고정 취향 학습: "+feedback.slice(0,60) });
        }
      } else if(kind === "page" || kind === "home"){
        const isH = (kind==="home");
        const kd = isH ? "analytics" : "monetization";
        const knm = isH ? "홈페이지" : "상품페이지";
        const cur = job.results[kind] || "";
        const rev = await productPagePipeline({ kind:(isH?"home":"product"), dept:kd,
          product: (isH?(job.results.homeTopic||job.topic):(job.results.pageProduct||job.topic)), info: job.info,
          baseHtml: cur, feedback: feedback }, (s)=>{ job.step="🔁 "+knm+" "+s; job.progAt=Date.now(); saveDB(); });
        if(rev.html && rev.html.length>200) job.results[kind] = rev.html;
        if(remember){ commitKnowledge(kd, "클라이언트 고정 취향("+knm+"): "+feedback, "피드백 학습");
          recordLearnLog({ dept:kd, deptKr:(AGENTS[kd]?AGENTS[kd].kr:kd), kind:"feedback", ok:true, note:"고정 취향 학습: "+feedback.slice(0,60) });
          try{ await absorbToLeader([kd], "피드백"); }catch(_){}
        }
      } else {
        const spec = CYCLE_FORMATS[kind]; const d = spec.dept; const a = AGENTS[d];
        let sys = "너는 민앤팜(고흥 특산물)의 '"+a.no+" "+a.kr+"' 부서 AI다."+ADDRESS+STYLE
          + " 아래 기존 결과물을 클라이언트 피드백대로 정확히 고쳐, 고친 완성본만 다시 출력하라. 요청한 부분만 반영하고 나머지 톤·구조는 유지하라. 설명·머리말 없이 결과물만.";
        const kb = knowledgeText(d); if(kb) sys += "\n\n[축적 전문성·취향]\n"+kb;
        const cur = job.results[kind] || "";
        const um = "[기존 "+spec.label+"]\n"+cur+"\n\n[클라이언트 수정 요청]\n"+feedback+"\n\n(요청대로 고친 완성본만)";
        let out = "";
        try{ const g = await createReviewedContent(sys, um, job.topic, workEngine()); out = g.content||""; }
        catch(e){ out = await genText(sys, um, 1800, workEngine()); }
        if(out && out.length>20) job.results[kind] = out;
        if(remember){ commitKnowledge(d, "클라이언트 고정 취향("+spec.label+"): "+feedback, "피드백 학습");
          recordLearnLog({ dept:d, deptKr:a.kr, kind:"feedback", ok:true, note:"고정 취향 학습: "+feedback.slice(0,60) });
          try{ await absorbToLeader([d], "피드백"); }catch(_){}
        }
      }
      job.status = "done"; job.step = "완료"; job.doneAt = Date.now(); trimCycleJobs(); saveDB();
      try{ const _knm = CYCLE_FORMATS[kind]?CYCLE_FORMATS[kind].label:(kind==="page"?"상품페이지":(kind==="home"?"홈페이지":(kind==="report"?"리서치 보고서":kind))); kakaoNotify("🔁 수정 완료 — "+job.topic+" ("+_knm+")\n"+(remember?"✅ 앞으로도 이 취향을 기억합니다":"이번 결과에만 반영")); }catch(_){}
    }catch(e){
      job.status = prevStatus==="done"?"done":"error"; job.error = String(e.message||e); job.step="수정 실패"; saveDB();
      logError("cycle-feedback", e);
    }finally{ endJobMode(); }
  });
});
// 🎤 v223: 생성 전 인터뷰 — "제약을 주기보다 맥락을 받아라".
// 비싼 산출물(상품페이지·홈페이지)일수록 8000토큰짜리 재생성보다 질문 3개가 훨씬 싸다.
app.post("/api/cycle/interview", async (req,res)=>{
  try{
    const b = req.body || {};
    const topic = String(b.topic||"").trim();
    if(!topic) return res.status(400).json({ error:"주제를 먼저 입력하세요" });
    const info = String(b.info||"").slice(0,1200);
    const memo = String(b.memo||"").slice(0,600);
    const o = b.outputs || {};
    const wants = [];
    if(o.sns) wants.push("SNS 게시물"); if(o.blog) wants.push("블로그 글"); if(o.cafe) wants.push("카페글");
    if(o.youtube) wants.push("유튜브 기획"); if(o.page) wants.push("상품 상세페이지"); if(o.home) wants.push("브랜드 홈페이지"); if(o.report) wants.push("리서치 보고서");
    const sys = "너는 민앤팜(전남 고흥 특산물)의 총괄 비서 오세라다."+ADDRESS
      + " 클라이언트가 '"+topic+"'로 ["+(wants.join(", ")||"콘텐츠")+"]를 만들려 한다. "
      + "바로 만들기 전에, 결과물의 방향을 가르는 '진짜 중요한 질문' 3개만 던져라. "
      + "규칙: (1) 뻔한 질문(예: '어떤 톤을 원하세요?') 금지 — 답에 따라 결과물이 실제로 확 달라지는 갈림길만. "
      + "(2) 클라이언트가 미처 생각 못 했을 맹점을 찔러라. (3) 각 질문에 '왜 묻는지' 한 줄과, 고르기 쉽게 선택지 2~3개를 제시하되 자유 서술도 가능하게. "
      + "반드시 JSON만 출력(설명·마크다운 금지): "
      + '{"questions":[{"q":"질문","why":"이걸 알면 결과물이 어떻게 달라지는지 한 줄","options":["선택지1","선택지2"]}]}'
      + brandBlock() + profileContext();
    const user = "주제: "+topic+(info?("\n추가 정보: "+info):"")+(memo?("\n내 지시 메모: "+memo):"")+"\n원하는 결과물: "+(wants.join(", ")||"미정");
    const raw = await anthropic(sys, user, 700);
    const j = safeJson(raw, null);
    let qs = (j && Array.isArray(j.questions)) ? j.questions : [];
    qs = qs.filter(x=>x&&x.q).slice(0,3).map(x=>({
      q: String(x.q).slice(0,160),
      why: String(x.why||"").slice(0,120),
      options: (Array.isArray(x.options)?x.options:[]).map(v=>String(v).slice(0,60)).slice(0,3)
    }));
    if(!qs.length) return res.json({ ok:false, error:"질문을 만들지 못했어요. 그냥 바로 실행하셔도 됩니다." });
    res.json({ ok:true, questions:qs });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 자연어 예약 지시 파싱 → 확인용 구조 반환 (저장은 프론트가 확인 후 reserve/start 호출)
app.post("/api/cycle/parse", async (req,res)=>{
  try{
    const text = String((req.body||{}).text||"").slice(0,600).trim();
    if(!text) return res.status(400).json({ error:"지시 문장을 입력하세요" });
    const sys = "아래 한국어 예약/생성 지시를 분석해 JSON만 출력한다(설명·마크다운 금지). "
      + "형식: {\"topic\":\"핵심 주제\",\"outputs\":{\"sns\":true|false,\"blog\":true|false,\"cafe\":true|false,\"youtube\":true|false,\"page\":true|false,\"home\":true|false,\"report\":true|false},"
      + "\"repeat\":\"once|daily|weekly\",\"time\":\"HH:MM\",\"days\":[0~6, 일=0 토=6],\"url\":\"있으면 http주소 아니면 빈칸\",\"memo\":\"말투·강조점 등 부가 지시(있으면)\"}. "
      + "규칙: 결과물 종류를 못 정하면 sns=true. '블로그'는 blog, '네이버 카페/다음 카페/카페글'은 cafe, '유튜브/영상/쇼츠'는 youtube, '상세페이지/상품페이지'는 page, '홈페이지/공식홈/웹사이트/랜딩'은 home, '리서치/시장조사/경쟁사분석/키워드분석/보고서'는 report, 그 외 게시물은 sns. "
      + "'매일'→daily, '매주 O요일'→weekly+days, 반복 언급 없으면 once. 시각 표현('아침 8시','새벽 2시' 등)을 HH:MM 24시로. 시각 없으면 09:00. days는 weekly일 때만 채운다.";
    const raw = await anthropic(sys, text, 400);
    const j = safeJson(raw, null);
    if(!j || !j.topic){ return res.json({ ok:false, error:"이해하지 못했어요. 예: '매일 아침 8시에 복숭아 블로그 써줘'처럼 다시 써주세요." }); }
    const o = j.outputs || {};
    const anyOut = !!(o.sns||o.blog||o.youtube||o.page||o.home||o.cafe||o.report);
    const parsed = {
      topic: String(j.topic||"").slice(0,120),
      outputs: { sns: anyOut?!!o.sns:true, blog:!!o.blog, cafe:!!o.cafe, youtube:!!o.youtube, page:!!o.page, home:!!o.home, report:!!o.report },
      repeat: (["once","daily","weekly"].indexOf(j.repeat)>=0)?j.repeat:"once",
      time: /^\d{1,2}:\d{2}$/.test(String(j.time||""))?String(j.time):"09:00",
      days: Array.isArray(j.days)?j.days.map(n=>parseInt(n,10)).filter(n=>n>=0&&n<=6):[],
      url: /^https?:\/\/.+/.test(String(j.url||""))?String(j.url):"",
      memo: String(j.memo||"").slice(0,800)
    };
    res.json({ ok:true, parsed });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// ===================== 🎨 디자인 스튜디오 (1회성 생성·저장) =====================
// 홈페이지 / 티스토리 블로그 스킨 / 블로그 카테고리 구성 / 카페 디자인 시안. 예약 아님 — 필요할 때 만들어 저장.
var DESIGN_KINDS = {
  homepage:      { label:"홈페이지 디자인", html:true,  hint:"브랜드 원페이지 홈페이지(히어로+소개+상품/특산물+스토리+연락/오시는길+푸터). 고급스러운 여백·타이포·반응형." },
  tistory_skin:  { label:"티스토리 스킨", html:true,  hint:"티스토리 블로그 스킨의 완성형 HTML/CSS. 헤더·글목록·본문·사이드·푸터 구조. 티스토리 치환자(예: [##_article_rep_desc_##])가 들어갈 자리는 <!-- [치환자] --> 주석으로 표시해 실제 스킨에 붙일 수 있게." },
  blog_category: { label:"블로그 카테고리 구성", html:false, hint:"블로그 카테고리 트리(대분류/소분류)와 각 카테고리 설명·목표·발행 주기 제안을 텍스트로. 검색·전문성(주제 집중)에 유리하게." },
  cafe:          { label:"카페 디자인 시안", html:false, hint:"네이버 카페 디자인 시안(텍스트+간단 예시): 대표이미지 컨셉, 게시판 구성, 대표메뉴, 배너·타이틀 문구, 컬러 무드. ※ 네이버 카페는 외부 코드 직접주입에 제약이 있어 '시안·가이드'까지만 제공됨을 명시." }
};
function trimDesignJobs(){
  DB.designJobs = DB.designJobs || [];
  if (DB.designJobs.length > 30) DB.designJobs = DB.designJobs.slice(-30);
  const n = DB.designJobs.length, keep = 8;
  DB.designJobs.forEach((j,i)=>{ if(i < n-keep && j.output && j.output.length>400){ j.outputDropped=true; j.output=""; } });
}
async function runDesignJob(id, appUrl){
  const job = (DB.designJobs||[]).find(j=>j.id===id);
  if(!job) return;
  const setStep = (s)=>{ job.step=s; job.progAt=Date.now(); saveDB(); };
  job.status="running"; job.startedAt=Date.now(); setStep("시작");
  beginJobMode();
  try{
    const spec = DESIGN_KINDS[job.kind]; if(!spec) throw new Error("디자인 종류 오류");
    const d = "monetization"; const a = AGENTS[d];   // 웹디자인 전문(수석 웹디자이너) 부서
    const brief = String(job.brief||"").slice(0,1500);
    const memo = String(job.memo||"").slice(0,800);
    const url = String(job.url||"").trim();
    if(url){ setStep("참고 URL 학습 중"); try{ await learnFromWebUrls(d, [url]); }catch(_){ job._warn="참고 URL 학습 일부 실패; "; } }
    const kb = knowledgeText(d);
    const isRevise = !!(job.baseOutput && job.feedback);
    let sys, user;
    if(spec.html){
      if(isRevise){
        sys = "너는 민앤팜(전남 고흥 특산물)의 '"+a.no+" "+a.kr+"' 수석 웹디자이너다."+ADDRESS
          + " 아래 기존 "+spec.label+" HTML을 클라이언트 요청대로 수정해 완전한 단일 HTML 문서로 다시 출력하라. 요청 부분만 반영하고 나머지는 유지. 설명·머리말·마크다운 표시 금지, HTML만.";
        user = "[기존 HTML]\n"+job.baseOutput+"\n\n[수정 요청]\n"+job.feedback;
      } else {
        setStep("비주얼 디렉션 잡는 중");
        let visualBlock = "";
        try{
          const vSys = "너는 민앤팜(전남 고흥 특산물)의 아트디렉터다."+brandBlock()
            + " 코딩 전에 비주얼을 먼저 확정한다. AI가 방향 없이 만들면 흔한 평균값('분포 수렴')으로 도망쳐 싸구려 티가 난다. '깔끔·모던' 대신 구체적 미학 스타일과 결 맞는 브랜드 레퍼런스, 색·폰트·레이아웃·시그니처를 못 박아라. JSON만: {\"aesthetic\":\"구체적 미학 스타일\",\"reference\":\"결 맞는 브랜드 레퍼런스 느낌\",\"mood\":\"\",\"palette\":[{\"name\":\"\",\"hex\":\"#______\",\"use\":\"\"}],\"typography\":{\"heading\":\"구체적으로(흔한 Inter류 금지)\",\"body\":\"\"},\"layout\":\"이 페이지만의 배치 한두 줄(흔한 가운데정렬 히어로+카드3개 금지)\",\"signature\":\"강렬한 한 방 요소 하나\"}. palette 4~6개.";
          const vRaw = await anthropic(vSys + designSystemBlock()
            + (hasDesignSystem()? " ★브랜드 디자인 시스템이 있으므로 palette·typography는 위 도면의 색·폰트를 그대로 옮겨 담아라(새 색 창작 금지). aesthetic·layout·signature만 새로 정하라." : ""),
            spec.label+" — "+brief.slice(0,600)+(memo?("\n지시:"+memo):""), 700);
          const vj = safeJson(vRaw, null);
          if(vj){ const pal=(vj.palette||[]).map(p=>"· "+(p.name||"")+" "+(p.hex||"")+(p.use?(" ("+p.use+")"):"")).join("\n");
            visualBlock = "\n\n[먼저 확정한 비주얼 — 이 미학·레퍼런스·색·타이포·레이아웃·시그니처에 정확히 맞춰 설계, hex를 실제 CSS에 사용]\n"
              +(vj.aesthetic?("미학: "+vj.aesthetic+"\n"):"")+(vj.reference?("레퍼런스: "+vj.reference+"\n"):"")
              +"무드: "+(vj.mood||"")+"\n"+(pal?("팔레트:\n"+pal+"\n"):"")
              +(vj.typography?("타이포: 제목 "+(vj.typography.heading||"")+" / 본문 "+(vj.typography.body||"")+"\n"):"")
              +(vj.layout?("레이아웃: "+vj.layout+"\n"):"")+(vj.signature?("시그니처: "+vj.signature):"");
            job.visualDirection = vj; }
        }catch(_){}
        setStep("디자인 코딩 중");
        sys = "너는 민앤팜(전남 고흥 특산물)의 '"+a.no+" "+a.kr+"' 수석 웹디자이너다. "+ADDRESS
          + " 아래 요청으로 '실제로 바로 열리는 완성형 단일 HTML 문서'를 만들어라. "+spec.hint
          + " ★필수: 콘텐츠는 자바스크립트 없이도 즉시 보여야 한다. opacity:0 / visibility:hidden 상태로 시작해서 JS(IntersectionObserver 등)로 나타나게 하는 스크롤 리빌은 금지 — JS가 한 줄이라도 실패하면 페이지 전체가 백지가 된다. 애니메이션은 CSS @keyframes로만, 끝난 상태가 아니라 처음부터 보이는 상태에서 시작하라(animation-fill-mode 의존 금지). localStorage·sessionStorage·쿠키는 절대 쓰지 마라(샌드박스에서 예외를 던져 스크립트가 죽는다). 반드시: (1) <!DOCTYPE html>~</html> 완전한 문서, CSS는 <style>에 인라인. (2) 고급스러운 여백·타이포·색감, 모바일 반응형. (3) 사진 자리는 <div class=\"img-ph\">[이미지: 묘사]</div> 플레이스홀더. (4) 과장광고 금지. (5) 'AI가 만든 흔한 웹' 티 금지 — 보라→인디고 그라디언트, Inter류 기본 폰트, '가운데 정렬 큰 제목+아이콘 카드 3개' 같은 클리셰(분포 수렴)를 피하고, 위 비주얼 디렉션의 미학·레퍼런스·색·폰트·레이아웃을 실제로 구현하며 시그니처 요소를 눈에 띄게 살려라. 설명·마크다운 없이 HTML만."
          + (kb?("\n\n[축적 웹디자인 노하우·취향]\n"+kb):"") + visualBlock + designSystemBlock() + (memo?("\n\n[클라이언트 지시 — 반드시 반영]\n"+memo):"") + profileContext();
        user = "요청: "+spec.label+"\n브리프: "+brief+(url?("\n참고(학습됨): "+url):"");
      }
      setStep("마무리 중");
      let html = await anthropic(sys, user, 8000);
      html = String(html).replace(/^```html\s*/i,"").replace(/^```\s*/,"").replace(/```\s*$/,"").trim();
      // 🔁 목표 기반 루프: 검수관이 비주얼 디렉션·기준 충족까지 최대 N회 수정(신규 생성만, 상한으로 토큰 보호)
      const _maxLoops = Math.max(0, Math.min(3, (DB.state && DB.state.designReviewLoops!=null) ? (+DB.state.designReviewLoops||0) : 1));
      if(!isRevise && _maxLoops>0){
        job.designReviews=[];
        for(let i=0;i<_maxLoops;i++){
          setStep("검수관이 디자인 점검 중 ("+(i+1)+"/"+_maxLoops+")");
          const rv = await reviewDesignHtml(html, job.visualDirection);
          job.designReviews.push({ round:i+1, pass:rv.pass, feedback:rv.feedback });
          if(rv.pass) break;
          setStep("검수 반영해 수정 중 ("+(i+1)+"/"+_maxLoops+")");
          const fixSys = sys + "\n\n[검수관 피드백 — 반드시 반영해 '완전한 단일 HTML'을 다시 출력, 잘된 부분은 유지]\n"+rv.feedback;
          try{ let fx=await anthropic(fixSys, "[현재 HTML]\n"+html.slice(0,14000)+"\n\n(피드백 반영한 완성 HTML만)", 8000);
            fx=String(fx).replace(/^```html\s*/i,"").replace(/^```\s*/,"").replace(/```\s*$/,"").trim();
            if(fx && /<\/html>/i.test(fx)) html=fx;
          }catch(e){ break; }
        }
      }
      job.output = html; job.isHtml = true;
    } else {
      setStep("구성안 작성 중");
      sys = "너는 민앤팜(전남 고흥 특산물)의 '"+a.no+" "+a.kr+"' 부서 수석이다."+ADDRESS+STYLE
        + " 아래 요청으로 "+spec.hint+" 바로 쓸 수 있게 구체적으로, 한국어로. 마크다운으로 정리."
        + (kb?("\n\n[축적 노하우]\n"+kb):"") + (memo?("\n\n[클라이언트 지시 — 반드시 반영]\n"+memo):"") + profileContext();
      user = "요청: "+spec.label+"\n브리프: "+brief+(url?("\n참고(학습됨): "+url):"")+(isRevise?("\n\n[기존안]\n"+job.baseOutput+"\n\n[수정 요청]\n"+job.feedback):"");
      const txt = await anthropic(sys, user, 2500);
      job.output = String(txt).trim(); job.isHtml = false;
    }
    // 결과에서 배운 것 누적(웹디자인 부서 성장)
    try{ await accumulateFromResult(d, spec.label+" · "+String(job.brief||"").slice(0,30), String(job.output||"").slice(0,1500), ""); }catch(_){}
    job.status="done"; job.doneAt=Date.now(); job.step="완료"; delete job.feedback; delete job.baseOutput; trimDesignJobs(); saveDB();
    const base=String(appUrl||(DB.state&&DB.state.appBase)||"").replace(/\/$/,"");
    try{ kakaoNotify("🎨 디자인 완료 — "+spec.label+"\n"+String(job.brief||"").slice(0,40)+"\n앱 '✍️ 만들기 > 🎨 디자인'에서 확인하세요.", base); }catch(_){}
  }catch(e){
    job.status="error"; job.error=String(e.message||e); job.doneAt=Date.now(); job.step="실패"; saveDB();
    try{ kakaoNotify("⚠️ 디자인 생성 실패\n"+String(e.message||e).slice(0,140)); }catch(_){}
    logError("runDesignJob", e);
  }finally{ endJobMode(); }
}
app.post("/api/design/start", (req,res)=>{
  try{
    const b = req.body || {};
    const kind = String(b.kind||"");
    if(!DESIGN_KINDS[kind]) return res.status(400).json({ error:"디자인 종류를 선택하세요" });
    const brief = String(b.brief||"").trim();
    if(!brief) return res.status(400).json({ error:"무엇을 만들지(브리프)를 입력하세요" });
    const url = String(b.url||"").trim();
    if(url && !/^https?:\/\/.+/.test(url)) return res.status(400).json({ error:"URL은 https:// 로 시작해야 해요" });
    DB.designJobs = DB.designJobs || [];
    const job = { id:String(Date.now())+"_d"+Math.floor(Math.random()*1000), kind, brief:brief.slice(0,1500),
      memo:String(b.memo||"").slice(0,800), url, status:"queued", at:Date.now() };
    DB.designJobs.push(job); trimDesignJobs(); saveDB();
    res.json({ ok:true, jobId:job.id, status:"queued" });
    setImmediate(()=>{ runDesignJob(job.id, String(b.appUrl||"")).catch(e=>logError("design-start", e)); });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
app.get("/api/design/status", (req,res)=>{
  const j = (DB.designJobs||[]).find(x=>x.id===String(req.query.id||""));
  if(!j) return res.status(404).json({ error:"작업을 찾을 수 없어요" });
  res.json({ ok:true, id:j.id, kind:j.kind, status:j.status, step:j.step||"", error:j.error||"", warn:j._warn||"", isHtml:!!j.isHtml, doneAt:j.doneAt||0 });
});
app.get("/api/design/list", (req,res)=>{
  const jobs = (DB.designJobs||[]).slice(-14).reverse().map(j=>({
    id:j.id, kind:j.kind, kindLabel:(DESIGN_KINDS[j.kind]?DESIGN_KINDS[j.kind].label:j.kind), brief:String(j.brief||"").slice(0,60),
    status:j.status, step:j.step||"", isHtml:!!j.isHtml, at:j.at, doneAt:j.doneAt||0, outputDropped:!!j.outputDropped, error:j.error||"" }));
  res.json({ ok:true, jobs });
});
app.get("/api/design/get", (req,res)=>{
  const j = (DB.designJobs||[]).find(x=>x.id===String(req.query.id||""));
  if(!j) return res.status(404).json({ error:"결과가 없어요" });
  res.json({ ok:true, id:j.id, kind:j.kind, kindLabel:(DESIGN_KINDS[j.kind]?DESIGN_KINDS[j.kind].label:j.kind), brief:j.brief||"", isHtml:!!j.isHtml, output:j.output||"", outputDropped:!!j.outputDropped });
});
app.post("/api/design/feedback", (req,res)=>{
  const b = req.body || {};
  const job = (DB.designJobs||[]).find(x=>x.id===String(b.id||""));
  if(!job || !job.output) return res.status(404).json({ error:"결과를 찾을 수 없어요" });
  const feedback = String(b.feedback||"").slice(0,1500).trim();
  const remember = !!b.remember;
  if(!feedback) return res.status(400).json({ error:"어떻게 고칠지 입력하세요" });
  res.json({ ok:true, jobId:job.id, status:"started" });
  setImmediate(async ()=>{
    beginJobMode();
    try{
      job.baseOutput = job.output; job.feedback = feedback; job.status="running"; job.step="🔁 수정 반영 중"; job.progAt=Date.now(); saveDB();
      await runDesignJob(job.id, String(b.appUrl||""));   // isRevise 경로로 재생성
      if(remember){ commitKnowledge("monetization", "클라이언트 고정 취향(디자인): "+feedback, "디자인 피드백 학습");
        recordLearnLog({ dept:"monetization", deptKr:(AGENTS.monetization?AGENTS.monetization.kr:"커머스"), kind:"feedback", ok:true, note:"디자인 취향 학습: "+feedback.slice(0,60) });
        try{ await absorbToLeader(["monetization"], "디자인 피드백"); }catch(_){}
      }
    }catch(e){ logError("design-feedback", e); }
    finally{ endJobMode(); }
  });
});
app.post("/api/design/delete", (req,res)=>{
  const id = String((req.body||{}).id||"");
  DB.designJobs = (DB.designJobs||[]).filter(j=>j.id!==id);
  saveDB();
  res.json({ ok:true });
});

// 1분마다: 근무 중이면 예약 작업 + 정기 자료수집 실행
setInterval(async ()=>{
  // (감시) 너무 오래 '진행 중'인 회의·작업 자동 마감(끊긴 처리로 갇힘 방지)
  try {
    const STUCK = 8*60*1000;   // 8분 이상 진행 중이면 끊긴 것으로 간주
    const STALL = 6*60*1000;   // 또는 6분간 진행(progAt) 변화가 없으면 멈춘 것으로 간주
    let fixed = 0; const t0 = Date.now();
    for (const mt of (DB.meetings||[])){
      if (mt && mt.status==="running"){
        const started = mt.at || 0;
        if (started && (t0 - started) > STUCK){ mt.status = "error"; mt.error = "처리가 지연돼 자동 중단됨(서버 지연/끊김)"; fixed++; }
      }
    }
    for (const jb of (DB.jobs||[])){
      if (jb && jb.status==="running"){
        const started = jb.at || 0;
        const beat = jb.progAt || jb.at || 0;
        if ((started && (t0 - started) > STUCK) || (beat && (t0 - beat) > STALL)){ jb.status = "error"; jb.progress = ""; jb.error = "처리가 지연돼 자동 중단됨"; fixed++; }
      }
    }
    if (fixed){ saveDB(); console.log("워치독: 멈춘 작업 "+fixed+"건 자동 마감"); }
  } catch(e){ /* 감시 실패는 무시 */ }
  // (0) 예약 회의: 근무시간과 무관하게 지정 시각(KST)에 실행
  const hm = kstHHMM(), day = kstDay(), dow = kstDow(), nowMs = Date.now();
  const autoOK = geminiAutoAllowed(); // Gemini 월 예산(₩) 안에서만 자동 지능작업 수행(초과 시 잠시 멈춤·다음날/다음달 재개)
  const leaderAuto = autoOK && ((DB.state||{}).leaderAutoOn === true); // 기본 OFF: 자동 백그라운드 지능작업(토큰 소모)은 사용자가 명시적으로 켤 때만 실행. 수동 URL 학습은 이 스위치와 무관하게 항상 동작.
  // (0-b) 팀장 브리핑: 아침(오늘 할 일)·저녁(오늘 한 일)·주간(한 주 종합) — 자동이므로 무료(Gemini)
  try {
    const st = DB.state || {};
    const morningHour = Number.isFinite(+st.morningHour) ? +st.morningHour : 9;
    const eveningHour = Number.isFinite(+st.briefHour) ? +st.briefHour : 21;
    const weeklyDow = Number.isFinite(+st.weeklyDow) ? +st.weeklyDow : 0; // 0=일
    const morningOn = st.morningBrief !== false, eveningOn = st.eveningBrief !== false, weeklyOn = st.weeklyBrief !== false;
    const kstHour = kstNow().getUTCHours();
    DB.briefDone = DB.briefDone || { morning:"", evening:"", weekly:"" };
    // 아침
    if (autoOK && morningOn && kstHour >= morningHour && DB.briefDone.morning !== day) {
      DB.briefDone.morning = day; saveDB();
      runBriefing("morning","gemini",true).then(()=>console.log("아침 브리핑 완료")).catch(e=>logError("brief-morning", e));
    }
    // 주간 (지정 요일 + 저녁 시각 이후, 주 1회) — 저녁보다 먼저 체크
    else if (autoOK && weeklyOn && dow === weeklyDow && kstHour >= eveningHour && DB.briefDone.weekly !== weekKey()) {
      DB.briefDone.weekly = weekKey(); saveDB();
      runBriefing("weekly","gemini",true).then(()=>console.log("주간 브리핑 완료")).catch(e=>logError("brief-weekly", e));
    }
    // 저녁
    else if (autoOK && eveningOn && kstHour >= eveningHour && DB.briefDone.evening !== day) {
      DB.briefDone.evening = day; DB.lastBriefDay = day; saveDB();
      runBriefing("evening","gemini",true).then(()=>console.log("저녁 브리핑 완료")).catch(e=>logError("brief-evening", e));
    }
  } catch(e){ /* 브리핑 실패 무시 */ }
  // (0-b2) 퇴근 회고: 저녁 시각(briefHour) 이후 하루 1회 — 팀장이 각 부서 오늘 자율수행을 평가·학습 → 다음날 지시에 반영
  try {
    const st5 = DB.state || {};
    const endHour = Number.isFinite(+st5.briefHour) ? +st5.briefHour : 21;
    const kstHour2 = kstNow().getUTCHours();
    if (leaderAuto && kstHour2 >= endHour && DB.lastReviewDay !== day && !(DB.growBurst&&DB.growBurst.active)) {
      DB.lastReviewDay = day; saveDB(); // 선점
      runDailyReview().then(()=>console.log("퇴근 회고 완료")).catch(e=>logError("daily-review", e));
    }
  } catch(e){ /* 회고 실패 무시 */ }
  // (0-b3) 유튜브 성과 추적: 하루 1회(저녁) 발행 영상 조회·좋아요·댓글 수집 → 분석부(04)가 다음 기획에 반영
  try {
    const st6 = DB.state || {};
    const anaHour = Number.isFinite(+st6.briefHour) ? +st6.briefHour : 21;
    const kstHour3 = kstNow().getUTCHours();
    if (leaderAuto && st6.analyticsAutoOn !== false && kstHour3 >= anaHour && DB.lastAnalyticsDay !== day && !(DB.growBurst&&DB.growBurst.active)) {
      DB.lastAnalyticsDay = day; saveDB(); // 선점
      runAnalyticsCollect(true).then(r=>{ if(r&&r.count) console.log("유튜브 성과 수집 완료: "+r.count+"건"); }).catch(e=>logError("analytics-collect", e));
    }
  } catch(e){ /* 성과 수집 실패 무시 */ }
  try {
    if (leaderAuto && DB.lastNightResearchDay !== day && !(DB.growBurst&&DB.growBurst.active)) {
      const kh = kstNow().getUTCHours();
      if (kh < 6) { // 자정~새벽 5시대 (아침 지시 배정 전)
        DB.lastNightResearchDay = day; saveDB(); // 선점
        runNightResearch().then(()=>console.log("야간 연구 완료")).catch(e=>logError("night-research", e));
      }
    }
  } catch(e){ /* 야간 연구 실패 무시 */ }
  // (0-b4) 자동 벤치마크 학습: 하루 1회, 부서를 하나씩 돌아가며 웹·유튜브에서 고품질 사례를 조사·학습(품질 공식 축적)
  //   ※ 시간대를 제한하지 않음 — 무료 서버는 새벽에 잠들어 있어(cron 활성시간이 낮이면) 새벽 조건이면 영원히 안 돌기 때문
  try {
    const st7 = DB.state || {};
    if (leaderAuto && st7.benchmarkLearnOn !== false && DB.lastBenchmarkDay !== day && !(DB.growBurst&&DB.growBurst.active)) {
      DB.lastBenchmarkDay = day;
      const order = Object.keys(DEPT_BENCHMARK);
      DB.benchmarkTurn = ((DB.benchmarkTurn||0)) % order.length;
      const dept = order[DB.benchmarkTurn];
      DB.benchmarkTurn = (DB.benchmarkTurn+1) % order.length;
      saveDB(); // 선점(중복 실행 방지)
      learnFromBenchmark(dept).then(r=>{
        if(r&&r.ok){ try{
            const learned=String(r.knowledge||"").replace(/\r/g,"").split("\n").map(s=>s.trim()).filter(Boolean).slice(0,4).join("\n").slice(0,300);
            const src=(r.sources&&r.sources.length)?("\n\n출처: "+r.sources.map(s=>s.title||s.uri).slice(0,2).join(" · ")):"";
            kakaoNotify("📚 "+(AGENTS[dept]?AGENTS[dept].kr:dept)+" 부서가 '"+r.benchmark+"'를 학습해 품질 공식을 갱신했어요"+(learned?("\n\n📌 배운 것:\n"+learned):"")+src);
          }catch(_){}
          console.log("벤치마크 학습 완료: "+dept); }
        else { console.log("벤치마크 학습 보류("+dept+"): "+(r&&r.note)); DB.lastBenchmarkDay=""; saveDB(); } // 실패 시 오늘 재시도 허용
      }).catch(e=>{ logError("benchmark-learn", e); DB.lastBenchmarkDay=""; try{saveDB();}catch(_){} });
    }
  } catch(e){ /* 벤치마크 학습 실패 무시 */ }
  // (0-b5) 팀장 자가진단: 시키지 않아도 주 1회 스스로 플랫폼을 살펴보고 개선 가이드라인을 제안
  try {
    const st8 = DB.state || {};
    if (leaderAuto && st8.selfReviewOn !== false && !(DB.growBurst&&DB.growBurst.active)) {
      const weekKey = (function(){ const d=kstNow(); const oneJan=new Date(Date.UTC(d.getUTCFullYear(),0,1));
        return d.getUTCFullYear()+"-W"+Math.ceil((((d-oneJan)/86400000)+oneJan.getUTCDay()+1)/7); })();
      if (DB.lastSelfReviewWeek !== weekKey && (DB.patchProposals||[]).filter(p=>p.status==="pending").length < 2){
        DB.lastSelfReviewWeek = weekKey; saveDB();   // 선점
        selfReviewAndPropose("").then(pr=>{
          console.log(pr ? ("팀장 자가진단 → 제안 생성: "+pr.title) : "팀장 자가진단 → 변경 불필요");
        }).catch(e=>{ logError("self-review", e); DB.lastSelfReviewWeek=""; try{saveDB();}catch(_){} });
      }
    }
  } catch(e){ /* 자가진단 실패 무시 */ }
  // (0-c) 24시간+ 자율수행 정체 부서: 팀장이 감지해 학습 기반으로 재지시 (자동·무료)
  if (autoOK) checkStaleDepts().catch(e=>logError("stale-check", e));
  // (0-d) 부서 간 지식 공유·성장 세션: 주기 자동 진행(기본 48시간마다) — 자동·무료
  try {
    const st2 = DB.state || {};
    if (autoOK && st2.kShareOn !== false) {
      const everyH = Number.isFinite(+st2.kShareEveryH) ? Math.max(6, +st2.kShareEveryH) : 48;
      if (Date.now() - (DB.lastKShareAt||0) >= everyH*3600000) {
        DB.lastKShareAt = Date.now(); saveDB(); // 선점
        runKnowledgeShare("gemini").then(r=>{ try{ var lg=(r.learnings||[]); var body=lg.length?("\n\n"+lg.map(x=>"· "+x.name+": "+String(x.learn||"").slice(0,90)).join("\n")):""; kakaoNotify("🤝 부서 지식 공유 세션 완료 — "+lg.length+"개 부서가 서로 배움"+body); }catch(_){} console.log("자동 지식 공유 완료"); }).catch(e=>logError("auto-kshare", e));
      }
    }
  } catch(e){ /* 지식공유 실패 무시 */ }
  // (0-f) 매일 전 부서 자기주도 성장 + 팀장 통합 지능 (무료 Gemini, 하루 1회)
  try {
    const st4 = DB.state || {};
    if (autoOK && st4.dailyGrowthOn !== false) {
      const gHour = Number.isFinite(+st4.dailyGrowthHour) ? +st4.dailyGrowthHour : 5;
      const kstHour = kstNow().getUTCHours();
      if (kstHour >= gHour && DB.lastDailyGrowthDay !== day && !(DB.growBurst&&DB.growBurst.active)) {
        DB.lastDailyGrowthDay = day; saveDB(); // 선점
        runDailyGrowth().then(()=>console.log("매일 자동 성장 완료")).catch(e=>logError("daily-growth", e));
      }
    }
  } catch(e){ /* 매일 성장 실패 무시 */ }
  // (0-f2) 팀장의 매일 자율수행 지시 배정 — 자동성장이 꺼져 있어도 독립적으로 매일 보장
  try {
    if (leaderAuto && DB.lastLeaderDirectDay !== day && !(DB.growBurst&&DB.growBurst.active)) {
      const kstHour2 = kstNow().getUTCHours();
      if (kstHour2 >= 6) { // 새벽 6시 이후, 하루 한 번
        DB.lastLeaderDirectDay = day; saveDB(); // 선점
        assignDailyDirectives().then(()=>console.log("팀장 일일 지시 배정 완료")).catch(e=>logError("daily-directives", e));
      }
    }
  } catch(e){ /* 일일 지시 실패 무시 */ }
  // (0-e) 지식 자기 훈련: 주기적으로 부서가 스스로 전문성을 단련(기본 12시간마다 2부서) — 자동·무료
  try {
    const st3 = DB.state || {};
    if (autoOK && st3.trainOn !== false) {
      const everyH = Number.isFinite(+st3.trainEveryH) ? Math.max(2, +st3.trainEveryH) : 12;
      if (Date.now() - (DB.lastTrainRoundAt||0) >= everyH*3600000) {
        DB.lastTrainRoundAt = Date.now(); saveDB(); // 선점
        runTrainingRound(2).then(done=>{ if(done&&done.length){ try{ kakaoNotify("🏋️ 지식 훈련 — 부서가 스스로 실력을 단련했어요\n\n"+done.map(x=>"· "+x.name+": "+x.what).join("\n")); }catch(_){} } console.log("자기 훈련 라운드 완료"); }).catch(e=>logError("auto-train", e));
      }
    }
  } catch(e){ /* 훈련 실패 무시 */ }
  // (0-f) 자율 개선 제안: 하루 1회, 팀장이 웹을 조사해 개선안을 제시(승인 시 품질 공식 반영) — 자동·무료
  try {
    const st4 = DB.state || {};
    if (leaderAuto && st4.autoImproveOn !== false) {
      const everyH = Number.isFinite(+st4.improveEveryH) ? Math.max(6, +st4.improveEveryH) : 24;
      const pend = (DB.improveProposals||[]).filter(p=>p.status==="pending").length;
      if (pend < 2 && Date.now() - (DB.lastImproveAt||0) >= everyH*3600000) {
        DB.lastImproveAt = Date.now(); saveDB(); // 선점
        researchImprovement("").then(r=>{ if(r&&r.ok&&r.proposal){ const pr=r.proposal; const base=(st4.appBase||"").replace(/\/$/,"");
          const links = base ? ("\n\n승인: "+base+"/api/improve/approve?id="+pr.id+"&code="+pr.code+"\n거절: "+base+"/api/improve/reject?id="+pr.id+"&code="+pr.code) : "";
          try{ kakaoNotify("🚀 자율 개선 제안 — "+pr.deptKr+"\n"+pr.title+"\n\n왜: "+pr.why+"\n기대: "+pr.expect+"\n\n⚠️ 승인하면 품질 공식에 반영됩니다. 앱 학습 탭에서도 확인 가능."+links); }catch(_){} } console.log("자율 개선 제안 완료"); }).catch(e=>logError("auto-improve", e));
      }
    }
  } catch(e){ /* 개선 제안 실패 무시 */ }
  // (0-g) 에버그린 재발행 추천: 3일에 1회, 오래(21일+) 안 쓴 즐겨찾기 콘텐츠가 있으면 재발행을 권유 — 자동·무료(알림만)
  try {
    const st5 = DB.state || {};
    if (leaderAuto && st5.evergreenSuggestOn !== false) {
      if (Date.now() - (DB.lastEvergreenSuggestAt||0) >= 3*86400000) {
        const now = Date.now();
        const cand = (DB.evergreen||[]).filter(x=> (now-(x.lastUsedAt||x.at||now)) >= 21*86400000)
          .sort((a,b)=> ((b.score||0)-(a.score||0)) || ((a.lastUsedAt||0)-(b.lastUsedAt||0)))[0];
        if (cand){
          DB.lastEvergreenSuggestAt = now; saveDB();
          const idle = Math.round((now-(cand.lastUsedAt||cand.at||now))/86400000);
          kakaoNotify("🌲 에버그린 재발행 추천\n"+idle+"일째 안 올린 콘텐츠가 있어요"+(cand.note?(" ("+cand.note+")"):"")+".\n\""+String(cand.text||"").slice(0,60)+"…\"\n앱 학습 탭 🌲 에버그린에서 '새로 각색'해 다시 올려보세요.").catch(()=>{});
        }
      }
    }
  } catch(e){ /* 에버그린 추천 실패 무시 */ }
  // 프로젝트 자동 사이클: 각 프로젝트의 설정 간격마다 한 사이클 진행(회의 대체, 지속형)
  try{
    for (const p of (DB.projects||[])){
      if (!p || p.status!=="active") continue;
      if (Date.now() >= (p.nextAt||0) && !p._running){
        p._running = true; saveDB();
        runProjectCycle(p).then(()=>{ p._running=false; saveDB(); }).catch(e=>{ p._running=false; logError("project-cycle:"+p.id, e); });
      }
    }
  }catch(e){ /* 프로젝트 사이클 실패 무시 */ }
  for (const ms of (DB.meetingSchedules||[])){
    let due = false;
    if (ms.repeat === "interval"){
      if (!ms.nextAt) ms.nextAt = nowMs; // 안전 보정
      if (nowMs >= ms.nextAt) due = true;
    } else if (ms.lastRunDay !== day){
      // 정확한 '분' 일치를 요구하지 않고, 예약 시각을 지났으면 그날 1회 실행(놓침 방지)
      const sched = String(ms.time||"").split(":");
      const schedMin = (parseInt(sched[0],10)||0)*60 + (parseInt(sched[1],10)||0);
      const nowMin = kstNow().getUTCHours()*60 + kstNow().getUTCMinutes();
      const timeReached = nowMin >= schedMin;
      if (ms.repeat === "weekly") due = timeReached && (ms.days||[]).indexOf(dow) >= 0; // 지정 요일만
      else due = timeReached; // once / daily
    }
    if (!due) continue;
    if (ms.repeat === "interval"){
      const stepMs = Math.max(1, ms.everyN||1) * ((ms.everyUnit==="day")?86400000:3600000);
      ms.nextAt = nowMs + stepMs; // 다음 주기 예약
    } else { ms.lastRunDay = day; }
    // 이어가기: 같은 룸/주제의 직전 완료 회의 결론을 맥락으로 전달
    let prevSummary = "";
    if (ms.continueLast){
      const prev = (DB.meetings||[]).filter(x=>x.status==="done" && (x.room===ms.room || x.topic===ms.topic)).slice(-1)[0];
      if (prev) prevSummary = prev.summary || "";
    }
    saveDB();
    if (ms.kind === "project"){
      runProjectMeeting({ topic:ms.topic, depts:ms.depts, projectId:ms.projectId, rounds:ms.rounds, mode:ms.mode, promote:ms.promote!==false, variants:ms.variants, judgeMode:ms.judgeMode, appUrl:(DB.state&&DB.state.appBase)||"" }).then(()=>{
        if (ms.repeat === "once"){ DB.meetingSchedules = DB.meetingSchedules.filter(x=>x.id!==ms.id); saveDB(); }
      }).catch(e=>logError("scheduled-project-meeting", e));
    } else {
    runMeeting({ topic:ms.topic, depts:ms.depts, rounds:ms.rounds, mode:ms.mode, chair:ms.chair, agenda:ms.agenda, clientNote:ms.clientNote, prevSummary, room:ms.room, engine:"gemini", source:"schedule" }).then(()=>{
      if (ms.repeat === "once"){ DB.meetingSchedules = DB.meetingSchedules.filter(x=>x.id!==ms.id); saveDB(); }
    }).catch(e=>logError("scheduled-meeting", e));
    }
  }
  // 🔄 선순환 예약 실행 (내가 지정한 것만 — 자동학습 스위치와 무관하게 항상 실행)
  try{
    for (const rv of (DB.cycleReservations||[])){
      if (rv.lastRunDay === day) continue;
      const sp = String(rv.time||"").split(":");
      const schedMin = (parseInt(sp[0],10)||0)*60 + (parseInt(sp[1],10)||0);
      const nowMin = kstNow().getUTCHours()*60 + kstNow().getUTCMinutes();
      const reached = nowMin >= schedMin;
      let due = reached;
      if (rv.repeat === "weekly") due = reached && (rv.days||[]).indexOf(dow) >= 0;
      if (!due) continue;
      rv.lastRunDay = day;
      DB.cycleJobs = DB.cycleJobs || [];
      const job = { id:String(Date.now())+"_c"+Math.floor(Math.random()*1000), url:rv.url, topic:rv.topic, info:rv.info||"", memo:rv.memo||"",
        outputs:{ sns:!!rv.sns, blog:!!rv.blog, cafe:!!rv.cafe, youtube:!!rv.youtube, page:!!rv.page, home:!!rv.home, report:!!rv.report }, status:"queued", at:Date.now(), source:"reserve" };
      DB.cycleJobs.push(job); trimCycleJobs();
      if (rv.repeat === "once"){ DB.cycleReservations = DB.cycleReservations.filter(x=>x.id!==rv.id); }
      saveDB();
      const base = (DB.state && DB.state.appBase) || "";
      runCycleJob(job.id, base).catch(e=>logError("cycle-reserve-run", e));
    }
  }catch(e){ logError("cycle-reserve-scan", e); }
  if (!isWorking()) return;
  // (1) 예약 작업
  const due = (DB.scheduled||[]).filter(t=>!t.done && t.runAt<=Date.now());
  for (const t of due) {
    try { await handleInstruction(t.instruction, "schedule"); t.done = true; }
    catch(e){ console.error("scheduled task error", e); }
  }
  if (due.length) saveDB();
  // (1.2) 예약 외부발행 (시각 지정, 건수·AI생성 지원, 발행은 항상 카카오 확인)
  try {
    const hhmm = kstHHMM(), kday = kstDay(), kdow = kstDow();
    for (const ps of (DB.pubSchedules||[])) {
      if (!ps || ps.disabled) continue;
      if (ps.time !== hhmm) continue;
      if (ps.lastRunDay === kday) continue;
      if (Array.isArray(ps.days) && ps.days.length && ps.days.indexOf(kdow) < 0) continue;
      ps.lastRunDay = kday;
      if (ps.repeat === "once") ps.disabled = true;
      try { await runScheduledPublish(ps); } catch(e){ logError("pub-schedule", e); }
      saveDB();
    }
  } catch(e){ logError("pub-schedule-loop", e); }
  // (1.5) 발행 재시도 큐 (최대 3회)
  const rq = (DB.retryQueue||[]).filter(t=>t.nextAt<=Date.now());
  for (const t of rq) {
    const fn = publishers[t.platform];
    if (!fn) { DB.retryQueue = DB.retryQueue.filter(x=>x.id!==t.id); continue; }
    try {
      const r = await fn(t.content);
      if (r.ok) { DB.retryQueue = DB.retryQueue.filter(x=>x.id!==t.id); kakaoNotify("🔁 재시도 발행 성공: "+t.platform).catch(()=>{}); }
      else throw new Error(r.note||"실패");
    } catch(e){
      t.tries += 1; t.nextAt = Date.now() + Math.min(30, t.tries*5)*60000;
      if (t.tries >= 3) { DB.retryQueue = DB.retryQueue.filter(x=>x.id!==t.id); logError("retry-giveup:"+t.platform, e); kakaoNotify("⚠️ 발행 재시도 최종 실패: "+t.platform).catch(()=>{}); }
    }
  }
  if (rq.length) saveDB();
  // (2) 자율수행: 지시가 있는 부서는 매 주기 각자 동시 수행 + 지시 없는 부서는 한 부서씩 돌아가며 트렌드 학습
  const col = DB.state && DB.state.collect;
  if (col && col.everyMin > 0 && Date.now() - (DB.lastCollectAt||0) >= col.everyMin*60000) {
    try { await runAutoCycle(); } catch(e){ console.error("auto-cycle error", e); }
  }
}, 60000);

// 한 부서의 자율수행 1회 실행 (지시가 있으면 지시 수행, 없으면 트렌드 학습/반응 수집)
// ===== 프로젝트 엔진: 회의를 대체하는 지속형 협업(팀장 리드 → 부서 수행 → 검수 → 기록, 의견 반영) =====
function createProject(opts){
  DB.projects = DB.projects || [];
  const active = DB.projects.filter(p=>p.status==="active");
  if (active.length >= 2) throw new Error("동시 진행 프로젝트는 최대 2개예요. 기존 프로젝트를 보관(archive) 후 시작하세요.");
  const p = {
    id: Date.now()+Math.floor(Math.random()*1000),
    title: String(opts.title||"무제 프로젝트").slice(0,120),
    goal: String(opts.goal||opts.title||"").slice(0,600),
    depts: (Array.isArray(opts.depts)&&opts.depts.length) ? opts.depts.filter(d=>AGENTS[d]&&d!=="ops") : Object.keys(AGENTS).filter(d=>d!=="ops"),
    intervalMin: [60,120,180,360,720,1440].indexOf(+opts.intervalMin)>=0 ? +opts.intervalMin : 60,
    status: "active",
    cycle: 0,
    log: [],            // [{cycle, at, dept, work, review, refine}]
    research: [],       // [{at, text, sources}]
    clientNotes: [],    // [{at, text, used}]
    discuss: [],        // [{at, role:'client'|'lead', text}] — 팀장과의 토론
    outputs: [],        // 산출물(승인 대기로 연결된 것들)
    nextAt: Date.now(), // 다음 자동 사이클 시각
    at: Date.now()
  };
  DB.projects.push(p); saveDB();
  return p;
}
function projectById(id){ return (DB.projects||[]).find(p=>String(p.id)===String(id)); }
// 결과물을 '다른 프로그램에서 열 수 있는' 읽기 좋은 HTML 페이지로 렌더(공용) + 안전 토큰
function viewToken(){ return Math.random().toString(36).slice(2,10)+Date.now().toString(36).slice(-4); }
function docHtmlPage(title, raw, metaLine, dlBase){
  const esc=s=>String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
  let body = esc(raw)
    .replace(/^#{4,6}\s?(.*)$/gm,'<h4>$1</h4>')
    .replace(/^###\s?(.*)$/gm,'<h3>$1</h3>')
    .replace(/^#{1,2}\s?(.*)$/gm,'<h2>$1</h2>')
    .replace(/\*\*(.+?)\*\*/g,'<strong>$1</strong>')
    .replace(/^[-*]\s+(.*)$/gm,'• $1')
    .replace(/\n/g,'<br>');
  const t=esc(title||"결과물");
  return '<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>'+t+'</title>'
    +'<style>body{font-family:-apple-system,BlinkMacSystemFont,"Malgun Gothic","Apple SD Gothic Neo",sans-serif;max-width:740px;margin:0 auto;padding:24px 18px 88px;line-height:1.8;color:#1f2733;background:#fff}h1{font-size:20px;margin:0 0 6px}h2{font-size:17px;margin:1.5em 0 .4em}h3,h4{font-size:15px;margin:1.2em 0 .3em}.meta{color:#8a94a6;font-size:12px;margin-bottom:18px;padding-bottom:12px;border-bottom:1px solid #eef1f6}.doc{font-size:15.5px}.bar{position:fixed;left:0;right:0;bottom:0;background:#fff;border-top:1px solid #e6ebf2;padding:11px;text-align:center;box-shadow:0 -2px 10px rgba(0,0,0,.04)}.bar a{display:inline-block;margin:0 8px;font-size:13px;color:#2b8aef;text-decoration:none;font-weight:700}</style></head>'
    +'<body><h1>'+t+'</h1><div class="meta">'+esc(metaLine||"")+'</div><div class="doc">'+body+'</div>'
    +'<div class="bar"><a href="'+dlBase+'&dl=md">⬇️ 마크다운 저장</a><a href="'+dlBase+'&dl=txt">⬇️ 텍스트 저장</a></div></body></html>';
}
function sendDoc(res, req, title, raw, metaLine, dlBase){
  if(req.query && req.query.dl){
    const fmt=(req.query.dl==="txt")?"txt":"md";
    const fname=encodeURIComponent(String(title||"결과물").slice(0,40).replace(/[\\/:*?"<>|]/g,"_"))+"."+fmt;
    res.set("Content-Type",(fmt==="txt"?"text/plain":"text/markdown")+"; charset=utf-8");
    res.set("Content-Disposition","attachment; filename*=UTF-8''"+fname);
    return res.send(raw||"");
  }
  res.set("Content-Type","text/html; charset=utf-8");
  res.send(docHtmlPage(title, raw, metaLine, dlBase));
}
// 여러 안의 강점만 골라 하나의 최고 완성본으로 합치는 '종합안' 편집장(Claude)
async function synthesizeVariants(cands, topic){
  const list = cands.map((c,i)=>"["+(i+1)+"안 · "+(c.angle||"")+"]\n"+String(c.text).slice(0,1400)).join("\n\n---\n\n");
  const sys = "너는 편집장이다. 아래 여러 안의 '가장 강한 요소'만 취사선택해 하나의 최고 완성본으로 합쳐라(각 안의 좋은 후킹·구조·표현·근거를 살리되, 짜깁기 티 없이 매끄럽게). 과장·허위 없이 근거 있는 내용만. 한국어 완성본만 출력." + profileContext();
  return await anthropic(sys, "주제: "+topic+"\n\n"+list, 1800);
}
// 여러 안 중 최고를 고르는 AI 판사(백지 상태 Claude — 결과물만 보고 편견 없이 심사)
async function judgeBestVariant(cands, topic){
  const list = cands.map((c,i)=>"["+(i+1)+"안 · "+(c.angle||"")+"]\n"+String(c.text).slice(0,1500)).join("\n\n---\n\n");
  const sys = "너는 편견 없는 심사위원(판사)이다. 아래 여러 안 중 주제 '"+topic+"'에 가장 효과적인 안 하나를 고른다. 기준: 목표 적합성·후킹·신뢰(과장·허위 없음)·완성도·실전 사용성. 반드시 한 줄만 출력:\nWINNER: N | (선택 이유 한 문장)";
  try{
    const out = await anthropic(sys, list, 400);
    const m = String(out).match(/WINNER:\s*(\d+)\s*\|?\s*(.*)/i);
    if(m) return { index:(parseInt(m[1],10)||1)-1, reason:String(m[2]||"").trim() };
  }catch(e){ logError("judgeBestVariant", e); }
  return { index:0, reason:"(자동 선택)" };
}
// 🏢 프로젝트 회의: 주제로 각 부서가 자료수집·샘플 제작 → 발표·토론 회의 → 결과물 → 좋으면 프로젝트/완성본
async function runProjectMeeting(opts){
  opts = opts || {};
  const topic = String(opts.topic||"").trim();
  if(!topic) throw new Error("회의 주제가 필요합니다");
  const eng = ((opts.engine||workEngine())==="gemini") ? "gemini" : (opts.engine||workEngine());
  let depts = (Array.isArray(opts.depts)&&opts.depts.length ? opts.depts : Object.keys(AGENTS)).filter(d=>AGENTS[d]&&d!=="ops");
  depts = depts.filter((d,i)=>depts.indexOf(d)===i).slice(0,4);   // 샘플 비용 보호: 최대 4개 부서
  if(!depts.length) depts = ["creation","strategy"];
  DB.projectMeetings = DB.projectMeetings || [];
  const pm = { id:Date.now()+Math.floor(Math.random()*1000), token:viewToken(), topic, depts, projectId:opts.projectId||null,
    status:"running", phase:"자료수집", samples:[], meetingId:null, summary:"", deliverable:"", passed:false,
    promote:opts.promote!==false, at:Date.now() };
  DB.projectMeetings.push(pm);
  if(DB.projectMeetings.length>40) DB.projectMeetings = DB.projectMeetings.slice(-40);
  saveDB();
  try{
    // 1) 자료수집(공유 브리프) — 실검색 우선, 실패 시 지식 폴백
    let brief = "";
    try{ const r = await gatherResearch("프로젝트 회의 자료조사", topic, topic); brief = String((r&&r.brief)||"").slice(0,1200); }catch(e){}
    pm.brief = brief; pm.phase = "부서별 샘플 제작"; saveDB();
    // 2) 각 부서가 자료 기반으로 '샘플 결과물' 초안 제작
    for(const d of depts){
      const a = AGENTS[d]; const kb = knowledgeText(d);
      const sys = "너는 SNS 자동화 회사 '"+a.no+" "+a.kr+"' 부서 AI('"+(MEMBERS[d]||"")+"')다. 역할: "+a.role+ADDRESS
        + " 곧 열릴 회의를 위해, 주제에 대한 '네 부서의 샘플 결과물 초안'을 실제로 만들어라(설명 말고 결과물). 근거는 아래 자료를 쓰고 과장 금지. 간결하게 완성 초안만."
        + (brief?("\n\n[공유 자료조사 브리프]\n"+brief):"") + (kb?("\n\n[네 부서 축적 전문성]\n"+kb.slice(0,800)):"") + profileContext();
      let sample="";
      try{ sample = await genText(sys, "회의 주제: "+topic+"\n\n네 부서 샘플 결과물 초안을 만들어라.", 700, eng); }
      catch(e){ sample = "(샘플 생성 일시 오류)"; }
      pm.samples.push({ dept:d, deptKr:a.kr, member:MEMBERS[d]||"", text:String(sample).trim().slice(0,1200) });
      saveDB();
    }
    // 3) 발표·토론 회의 — 각 부서 샘플을 안건 맥락으로 넣어 서로 검토·소통 (processMeeting이 트렌드조사+티키타카+검수까지)
    pm.phase = "회의(발표·토론)"; saveDB();
    const samplesCtx = "각 부서가 미리 만든 샘플을 놓고 회의한다. 서로의 샘플을 검토·보완하고 더 나은 방향으로 합의하라.\n"
      + pm.samples.map(s=>"■ "+s.deptKr+"("+s.member+") 샘플:\n"+String(s.text).slice(0,400)).join("\n\n");
    let meeting=null;
    try{
      meeting = await runMeeting({ topic, depts, rounds:+opts.rounds||2, mode:opts.mode||"discuss",
        alwaysJoin:["ops"], chair:"ops", clientNote:samplesCtx, engine:eng, room:opts.room||"project", source:"project-meeting" });
    }catch(e){ logError("projectMeeting-meet", e); }
    pm.meetingId = meeting ? meeting.id : null;
    pm.summary = meeting ? String(meeting.summary||"") : "";
    // 4) 결과물 — 회의 합의 방향으로 완성 결과물 + 품질 게이트. 옵션: 여러 안 병렬 생성 → AI 판사 채택.
    pm.phase = "결과물 제작"; saveDB();
    const baseSys = "너는 SNS 자동화 회사의 제작 총괄이다. 아래 회의 결론과 부서 샘플을 종합해, 바로 쓸 수 있는 '완성 결과물' 하나로 만들어라. 과장·허위 금지, 근거 있는 내용만. 한국어로 완성본만." + profileContext();
    const userMsg = "주제: "+topic+"\n\n[회의 결론]\n"+(pm.summary||"(회의 요약 없음)")+"\n\n[부서 샘플 요약]\n"+pm.samples.map(s=>"· "+s.deptKr+": "+String(s.text).slice(0,200)).join("\n");
    const variants = Math.max(1, Math.min(3, (opts.variants!=null ? (+opts.variants||1) : ((DB.state&&+DB.state.meetingVariants)||1))));
    try{
      if(variants>=2){
        pm.phase = variants+"안 병렬 생성"; saveDB();
        const angles = ["감성·스토리텔링 중심(공감·브랜드 서사)","정보·신뢰 중심(근거·수치·구체 혜택)","파격·훅 중심(스크롤 멈추는 강한 후킹)"];
        const cands = await Promise.all(Array.from({length:variants}).map((_,i)=>{
          const aSys = baseSys + "\n\n[이번 안의 방향] "+angles[i%angles.length]+" — 이 방향을 뚜렷하게 살려라(다른 안과 확실히 다르게).";
          return genText(aSys, userMsg, 1600, eng).then(t=>({ angle:angles[i%angles.length], text:String(t||"").trim() })).catch(()=>null);
        }));
        const valid = cands.filter(c=>c&&c.text);
        pm.candidates = valid.map(c=>({ angle:c.angle, text:c.text.slice(0,1500) }));
        saveDB();
        if(valid.length>=2){
          const judgeMode = (opts.judgeMode==="synth") ? "synth" : "pick";
          let text;
          if(judgeMode==="synth"){
            pm.phase = "종합안 편집"; saveDB();
            text = String(await synthesizeVariants(valid, topic)||"").trim() || valid[0].text;
            pm.winnerAngle = "종합안"; pm.winnerReason = "여러 안의 강점을 합침";
          } else {
            pm.phase = "AI 판사 심사"; saveDB();
            const jr = await judgeBestVariant(valid, topic);
            const win = valid[Math.min(valid.length-1, Math.max(0, jr.index))];
            pm.winnerReason = jr.reason; pm.winnerAngle = win.angle; text = win.text;
          }
          // 결과물을 검수 게이트로 마무리(백지 상태 검수관 Claude, 최대 2회)
          let revs=[];
          for(let i=0;i<2;i++){
            const rv = await reviewContent(text, topic, "claude");
            revs.push({ round:i+1, verdict:rv.pass?"PASS":"FAIL", feedback:rv.feedback });
            if(rv.pass) break;
            try{ text = await genText(baseSys+"\n\n[검수 피드백 — 반영해 다시]\n"+rv.feedback, "[현재]\n"+text+"\n\n(피드백 반영한 완성본만)", 1600, eng); }catch(e){ break; }
          }
          pm.deliverable = String(text).trim(); pm.passed = revs.length ? (revs[revs.length-1].verdict==="PASS") : true; pm.reviews = revs;
        } else if(valid.length===1){
          pm.deliverable = valid[0].text.trim(); pm.passed = true;
        } else { pm.deliverable = pm.summary||""; }
      } else {
        const gated = await createReviewedContent(baseSys, userMsg, topic, eng);
        pm.deliverable = String(gated.content||"").trim();
        pm.passed = !!gated.passed;
        pm.reviews = gated.reviews||[];
      }
    }catch(e){ pm.deliverable = pm.summary||""; logError("projectMeeting-final", e); }
    // 5) 좋은 품질이면 프로젝트로 이어가거나 완성본으로
    pm.phase = "정리"; pm.status = "done"; pm.doneAt = Date.now();
    let promotedTo = null;
    if(pm.promote && pm.passed && pm.deliverable){
      try{
        let proj = pm.projectId ? projectById(pm.projectId) : null;
        if(proj){   // 기존 프로젝트로 이어가기: 산출물·로그에 추가
          proj.outputs = proj.outputs || []; proj.outputs.push({ at:Date.now(), from:"project-meeting", topic, text:pm.deliverable.slice(0,4000) });
          proj.log = proj.log || []; proj.log.push({ cycle:(proj.cycle||0), at:Date.now(), dept:"ops", work:"[회의 결과물] "+topic+" — 검수 통과", review:"PASS" });
          proj.updatedAt = Date.now(); promotedTo = { kind:"project-continued", id:proj.id };
        } else {   // 새 프로젝트로 승격(동시 2개 상한이면 완성본으로만 남김)
          try{ const np = createProject({ title:topic, goal:topic, depts }); np.outputs=[{ at:Date.now(), from:"project-meeting", topic, text:pm.deliverable.slice(0,4000) }]; promotedTo={ kind:"project-created", id:np.id }; }
          catch(ce){ promotedTo = { kind:"final-only", reason:String(ce.message||ce) }; }  // 완성본으로만
        }
      }catch(e){ logError("projectMeeting-promote", e); }
    }
    pm.promotedTo = promotedTo; saveDB();
    const tag = pm.passed ? (promotedTo && promotedTo.kind!=="final-only" ? "✅ 검수 통과 → 프로젝트로 이어감" : "✅ 검수 통과(완성본)") : "⚠️ 검수 미통과(초안)";
    try{ kakaoNotify("🏢 프로젝트 회의 완료: "+topic+"\n"+tag+"\n샘플 "+pm.samples.length+"건 · "+depts.map(d=>AGENTS[d].kr).join(", ")+"\n앱 '🚀 프로젝트'에서 결과물을 확인하세요.", String(opts.appUrl||(DB.state&&DB.state.appBase)||"")).catch(()=>{}); }catch(_){}
    return pm;
  }catch(e){
    pm.status="error"; pm.error=String(e.message||e).slice(0,200); pm.doneAt=Date.now(); saveDB();
    logError("runProjectMeeting", e); throw e;
  }
}

// 프로젝트 한 사이클: 팀장이 목표+지금까지 진행+클라이언트 의견을 보고 → 한 부서에 과제 지정 → 수행 → 검수 → 팀장 보완평가 → 기록
async function runProjectCycle(project){
  if (!project || project.status!=="active") return null;
  if (!geminiAutoAllowed()) return null;
  const eng = "gemini";
  const p = project;
  const setPhase = (t)=>{ p.progress = t; p.progressAt = Date.now(); saveDB(); };
  setPhase("🔎 최신 자료 수집 중…");
  const recent = p.log.slice(-4).map(x=>"· C"+x.cycle+" ["+(AGENTS[x.dept]?AGENTS[x.dept].kr:x.dept)+"] "+String(x.work||"").slice(0,120)).join("\n") || "(아직 진행 없음)";
  const pendingNotes = (p.clientNotes||[]).filter(n=>!n.used);
  const noteTxt = pendingNotes.map(n=>n.text).join(" / ");

  // 1) 자료수집(사이클마다 새로고침, 무료 검색 한도 안에서만)
  try{
    const st=DB.state||{};
    if (st.nightSearchOn!==false && searchAllowedNow()){
      const sp="너는 팀장 '"+AGENTS.ops.kr+"("+MEMBERS["ops"]+")'다. 아래 프로젝트에 지금 당장 유용한 최신 자료·트렌드·사례를 전 세계 공개 웹에서 조사해 200자 내외로 정리하라. 딥웹·비공개 제외.\n프로젝트: "+p.title+"\n목표: "+p.goal;
      const r=await geminiSearch(sp, 1200);
      if (r&&r.text){ p.research.push({ at:Date.now(), text:String(r.text).slice(0,400), sources:(r.sources||[]).slice(0,5) }); if(p.research.length>30) p.research=p.research.slice(-30); }
    }
  }catch(e){ logError("project-research:"+p.id, e); }

  // 2) 팀장이 이번 사이클 할 일 결정: 어느 부서 / 무슨 과제 (의견 최우선 반영) + 중대결정 시 확인요청
  setPhase("👑 팀장이 이번 할 일 정하는 중…");
  let pick, task, holdReason="";
  try{
    const latestResearch = (p.research.slice(-1)[0]||{}).text||"";
    const sys="너는 이 회사 팀장 '"+AGENTS.ops.kr+"("+MEMBERS["ops"]+")'이며 최고 지능이다."+ADDRESS+clientBlock()
      +" 아래 프로젝트를 최상의 결과로 이끌기 위해, 이번 사이클에 '어느 부서'가 '무슨 구체적 과제'를 할지 하나 정하라. 지금까지 진행을 이어 더 나은 방향으로 발전시키고, 클라이언트 의견이 있으면 반드시 최우선 반영하라. 막연한 지시 금지.\n"
      +" 단, 클라이언트가 직접 결정해야 할 '중대한 방향 결정'(예: 큰 예산·핵심 콘셉트·법적 리스크·되돌리기 어려운 선택)이 필요하면, 진행하지 말고 확인을 요청하라.\n"
      +"프로젝트: "+p.title+"\n목표: "+p.goal+"\n참여부서(영문키): "+p.depts.join(", ")
      +"\n최근 진행:\n"+recent+(latestResearch?("\n최신 수집자료: "+latestResearch):"")+(noteTxt?("\n\n★ 클라이언트 의견(최우선 반영): "+noteTxt):"")
      +"\n\n형식만 출력(둘 중 하나):\n진행: PICK: 부서영문키 | TASK: 이번 과제(한국어 1문장)\n확인요청: HOLD: 클라이언트에게 물어볼 중대 결정 사항(한국어 1~2문장)";
    const out=await genText(sys, "이번 사이클을 정하라.", 400, eng);
    const hm=String(out).match(/HOLD:\s*(.+)/i);
    const m=String(out).match(/PICK:\s*([a-z]+)\s*\|\s*TASK:\s*(.+)/i);
    if(hm && !m){ holdReason=String(hm[1]||"").trim().slice(0,300); }
    else if(m && AGENTS[m[1].trim()] && m[1].trim()!=="ops"){ pick=m[1].trim(); task=String(m[2]||"").trim().slice(0,200); }
  }catch(e){ logError("project-pick:"+p.id, e); }

  // 팀장이 중대결정 확인을 요청하면: 이 사이클 대기 + 카카오 알림
  if(holdReason){
    p.status="awaiting"; p.holdReason=holdReason; p.holdAt=Date.now();
    p.log.push({ cycle:p.cycle+1, at:Date.now(), dept:"ops", task:"[확인 필요] "+holdReason, work:"", review:"", refine:"", hold:true });
    p.cycle++;
    if(p.log.length>60) p.log=p.log.slice(-60);
    p.nextAt = Date.now() + p.intervalMin*60000;
    p.progress = ""; saveDB();
    try{ kakaoProjectApproval(p).catch(()=>{ kakaoNotify("🙋 프로젝트 '"+p.title+"' — 팀장이 클라이언트 확인을 요청했어요:\n"+holdReason+"\n\n앱 프로젝트 탭에서 의견을 남기면 다시 진행해요.").catch(()=>{}); }); }catch(e){}
    return p;
  }
  if(!pick){ pick=p.depts[p.cycle % p.depts.length]; task=p.goal; }

  // 3) 부서 수행
  setPhase("✍️ "+(AGENTS[pick]?AGENTS[pick].kr:pick)+" 부서 작업 중: "+String(task||"").slice(0,40));
  let work="";
  try{
    const a=AGENTS[pick];
    let sys="너는 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role+ADDRESS+STYLE+personaLine(pick);
    const kb=knowledgeText(pick); if(kb) sys+="\n\n[축적 전문성]\n"+kb.slice(0,600);
    sys+="\n\n[프로젝트]\n"+p.title+" / 목표: "+p.goal+"\n[이번 과제]\n"+task+"\n최근 진행:\n"+recent;
    sys+=" 이 과제를 실제 결과물 수준으로 수행하라. 되묻지 말고 완성해서 한국어로."+profileContext();
    work=await genText(sys, task, 1200, eng);
  }catch(e){ logError("project-work:"+p.id, e); work="(수행 실패: "+String(e&&e.message||e).slice(0,120)+")"; }

  // 4) 검수(감사부 PASS/FAIL) + 5) 팀장 보완평가
  setPhase("🔍 검수·팀장 보완평가 중…");
  let review="", refine="";
  try{ const rv=await reviewContent(work, p.title, eng); review=(rv.pass?"PASS":"FAIL")+(rv.feedback?" · "+rv.feedback:""); }catch(e){}
  try{
    const sys="너는 팀장 '"+AGENTS.ops.kr+"'다."+ADDRESS+" 방금 '"+(AGENTS[pick].kr)+"' 부서가 한 아래 결과를 평가하고, 다음 사이클에 더 나아지도록 '보완 방향' 한두 문장을 제시하라. 프로젝트 목표: "+p.goal;
    refine=await genText(sys, "[결과]\n"+String(work).slice(0,1200), 400, eng);
  }catch(e){}

  // 6) 기록 + 학습 + 의견 소진
  p.cycle++;
  p.log.push({ cycle:p.cycle, at:Date.now(), dept:pick, task, work:String(work).slice(0,1500), review, refine:String(refine).slice(0,400) });
  if(p.log.length>60) p.log=p.log.slice(-60);
  pendingNotes.forEach(n=>{ n.used=true; });
  DB.deptMemory[pick]=DB.deptMemory[pick]||[];
  DB.deptMemory[pick].push({ at:Date.now(), instruction:"[프로젝트] "+p.title, note:task+" → "+String(work).slice(0,300) });
  DB.exp=DB.exp||{}; DB.exp[pick]=(DB.exp[pick]||0)+1;
  if(DB.exp[pick]%3===0){ try{ await distillKnowledge(pick); }catch(e){} }
  try{ await leaderAbsorb(pick); }catch(e){}

  // 7) 산출물이면 확인 대기로(검수 PASS면 실질 결과물로 간주)
  if(/PASS/.test(review)){
    const it=queueContentApproval(pick, p.title, "["+p.title+"] C"+p.cycle, work, [{round:p.cycle,verdict:"PASS",feedback:""}]);
    p.outputs.push({ approvalId:it.id, cycle:p.cycle, at:Date.now() });
    if(p.outputs.length>40) p.outputs=p.outputs.slice(-40);
  }
  p.nextAt = Date.now() + p.intervalMin*60000;
  p.progress = ""; saveDB();
  return p;
}

async function autoRunDept(dept, directive){
  const a = AGENTS[dept]; if(!a) return;
  const col = (DB.state && DB.state.collect) || {};
  const baseFocus = directive ? directive : ((col.topic && col.topic.trim()) ? col.topic.trim() : (a.kr + " 분야"));
  let sys;
  if (directive) {
    sys = "너는 SNS 자동화 회사 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role+ADDRESS+STYLE
      + " 클라이언트님이 이 부서에 내린 자율수행 지시가 있다: \""+directive+"\". 지금은 자율수행 시간이다. 이 지시를 네 전문 영역 안에서 실제로 수행해, 바로 쓸 수 있는 구체적 결과물 또는 핵심 정리를 한국어로 간결히 내라. 되묻지 말고 합리적으로 가정해 완성하라."
      + profileContext();
  } else {
    sys = "너는 SNS 자동화 회사 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role
      + " 지금은 평상시 자율 학습 시간이다. '"+baseFocus+"'에 관해 네 부서 업무에 바로 쓸 최신 인사이트·트렌드·아이디어를 2~3가지로 아주 간결히 정리하라. 다음에 제안에 활용할 핵심만." + profileContext();
  }
  // 학습된 내용을 실제로 활용해 수행 (이 부서 경험 + 타 부서 공유 기록)
  const _kb2 = knowledgeText(dept); const _lv2 = deptLevel(dept);
  sys += " (현재 Lv"+_lv2+" 숙련도)";
  if (_kb2) sys += "\n\n[이 부서가 축적한 전문성·노하우(지식 베이스) — 이걸 기반으로 더 발전된 결과를 내라]\n" + String(_kb2).slice(0,1200);
  if (directive){ try{ const _rel2 = await relevantSmart(dept, directive, 3); if (_rel2) sys += "\n\n[이 지시와 비슷한 과거 작업·자료 — 재활용·갱신해 빠르게 처리]\n" + String(_rel2).slice(0,800); }catch(_){} }
  const mem = (DB.deptMemory[dept]||[]).slice(-6).map(x=>"· "+(x.instruction?("["+x.instruction+"] "):"")+String(x.note||"").slice(0,160)).join("\n");
  if (mem) sys += "\n\n[최근 작업·학습(단기 기억)]\n" + mem;
  const cross = crossDeptMemory(dept);
  if (cross) sys += "\n\n[다른 부서들이 최근 학습·작성한 내용 — 관련되면 활용·보완하라]\n" + String(cross).slice(0,800);
  const tag = directive ? "[자율 지시] " : "[자율 학습] ";
  // 자동 주기는 '무료 전용' — Gemini만(유료 Anthropic 폴백 없음). 실패하면 오류로 쌓지 말고 다음 주기에 재시도.
  let note;
  try {
    const userLine = directive ? "자율수행 지시 실행" : "자율 학습 메모 작성";
    note = await geminiText(sys + "\n\n" + userLine, directive ? 1300 : 900);
  } catch(e) {
    console.warn("[autoRunDept "+dept+"] Gemini 실패 — 이번 주기 건너뜀(다음에 재시도):", String((e&&e.message)||e));
    return; // 무료 전용: 폴백 없이 조용히 건너뜀(오류 누적 방지)
  }
  if (!note || !String(note).trim()) { console.warn("[autoRunDept "+dept+"] 빈 응답 — 건너뜀"); return; }
  if (!DB.deptMemory[dept]) DB.deptMemory[dept] = [];
  DB.deptMemory[dept].push({ at:Date.now(), instruction:tag+baseFocus, note });
  if (DB.deptMemory[dept].length > 40) DB.deptMemory[dept] = DB.deptMemory[dept].slice(-40);
  DB.exp = DB.exp || {}; DB.exp[dept] = (DB.exp[dept]||0) + 1;
  if (dept!=="ops") ensureLeaderLead(); // 부서가 자율수행으로 크면 팀장도 그 위로 유지
  if (DB.exp[dept] % 3 === 0) { try{ await distillKnowledge(dept); }catch(e){} }
  DB.collections.push({ id:Date.now()+Math.floor(Math.random()*1000), topic:"["+a.kr+"] "+baseFocus, text:note, at:Date.now(), dept });
  if (DB.collections.length > 100) DB.collections = DB.collections.slice(-100);
  saveDB();
  const didSummary = String(note||"").replace(/\s+/g," ").slice(0,90);
  kakaoNotify("📚 "+a.kr+(directive?" 자율 지시 수행 +1":" 자율 학습 +1")+" (경험치 "+DB.exp[dept]+")"+(baseFocus?("\n주제: "+String(baseFocus).slice(0,60)):"")+(didSummary?("\n한 일: "+didSummary):"")).catch(()=>{});
}

// 지금 자율수행 1회 실행(수동 트리거) — 무료 서버 슬립으로 주기가 안 돌 때 직접 실행
app.post("/api/autorun", (req,res)=>{
  const body = req.body || {};
  const dept = body.dept, directive = body.directive;
  (async ()=>{
    try{
      if (dept && AGENTS[dept]){
        // 특정 부서의 지시를 즉시 1회 수행 (지시 적용 직후 바로 일하게)
        const dir = (directive && String(directive).trim()) || ((DB.state&&DB.state.deptDirective&&DB.state.deptDirective[dept])||"");
        if (dir){ DB.state=DB.state||{}; DB.state.deptDirective=DB.state.deptDirective||{}; DB.state.deptDirective[dept]=dir; saveDB(); }
        await autoRunDept(dept, String(dir||"").trim());
        DB.lastCollectAt = Date.now(); saveDB();
      } else {
        await runAutoCycle();
      }
    }catch(e){ logError("autorun", e); }
  })();
  res.json({ ok:true, status:"running", dept: dept||null });
});

// 자율수행 1회 사이클: 지시 있는 부서는 각자 지시 수행(학습 활용), 지시 없는 부서는 1개씩 순환 학습
// 정체 부서에 팀장이 학습 기반 지시를 생성
async function leaderDirectiveFor(dept){
  const a=AGENTS[dept], lead=AGENTS["ops"]; if(!a) return "";
  const kb=knowledgeText(dept);
  const recent=(DB.deptMemory[dept]||[]).slice(-5).map(x=>"· "+(x.instruction||"")+" → "+String(x.note||"").slice(0,100)).join("\n");
  const sys="너는 SNS 자동화 회사 팀장 '"+lead.no+" "+lead.kr+"'다."+ADDRESS+STYLE+clientBlock()
    +" '"+a.kr+"("+MEMBERS[dept]+")' 부서가 24시간 넘게 새로운 자율수행이 없어 정체돼 있다. 이 부서가 축적한 전문성과 최근 기록을 바탕으로, 지금 바로 자율수행할 '구체적이고 실행 가능한 한 줄 지시'를 내려라. 부서 전문성을 한 단계 끌어올릴 방향으로. 한 줄만, 한국어로, 따옴표·머리말 없이 지시문만.";
  const ctx="[부서 축적 전문성]\n"+(kb||"(아직 적음)")+"\n\n[최근 기록]\n"+(recent||"(없음)");
  const out=await genText(sys, ctx, 300, "gemini"); // 자동 = 무료
  return String(out||"").trim().replace(/^["'\s]+|["'\s]+$/g,"").slice(0,200);
}
// 24시간+ 정체된 부서를 팀장이 감지해 재지시
async function checkStaleDepts(maxHandle, force){
  const now=Date.now(), DAY=24*3600000; const CAP = maxHandle||2;
  DB.leadDirectives=DB.leadDirectives||[]; DB.staleHandled=DB.staleHandled||{};
  let handled=0;
  for(const d of Object.keys(AGENTS)){
    if(d==="ops") continue;
    const arr=DB.deptMemory[d]||[]; if(!arr.length) continue;
    const lastAt=arr[arr.length-1].at||0;
    if(now-lastAt <= DAY) continue;                          // 최근 활동 있음 → 정상
    if(!force && DB.staleHandled[d] && now-DB.staleHandled[d] < DAY) continue; // 이미 처리됨(수동 점검은 무시)
    if(handled>=CAP) break;                                  // 기본 한 틱 2부서(수동 점검은 더 많이)
    try{
      const dir=await leaderDirectiveFor(d);
      if(dir){
        DB.state=DB.state||{}; DB.state.deptDirective=DB.state.deptDirective||{}; DB.state.deptDirective[d]=dir;
        DB.leaderDailyDirective = DB.leaderDailyDirective || {};
        DB.leaderDailyDirective[d] = { text:dir, at:now, day:kstDay() };
        DB.leadDirectives.push({ dept:d, directive:dir, reason:"24시간+ 자율수행 정체 — 팀장이 학습 기반으로 재지시", at:now, lastActiveAt:lastAt });
        if(DB.leadDirectives.length>40) DB.leadDirectives=DB.leadDirectives.slice(-40);
        // 재지시를 '활동'으로 즉시 기록 → 정체(마지막 활동 기준)가 바로 풀림 (autoRun 실패/지연과 무관)
        DB.deptMemory[d].push({ at:Date.now(), instruction:"[팀장 재지시]", note:"팀장 지시 수신: "+dir });
        if(DB.deptMemory[d].length>40) DB.deptMemory[d]=DB.deptMemory[d].slice(-40);
        DB.staleHandled[d]=now; saveDB();
        kakaoNotify("🧭 팀장 재지시 — "+AGENTS[d].kr+": "+dir).catch(()=>{});
        autoRunDept(d, dir).catch(()=>{}); // 실제 수행은 백그라운드(정체는 위 활동 기록으로 이미 해소)
        handled++;
      }
    }catch(e){ logError("stale:"+d, e); }
  }
  if(handled){ ensureLeaderLead(); saveDB(); }
}
async function runAutoCycle(){
  if (!geminiAutoAllowed()) return; // Gemini 월 예산 초과/하루 페이싱 → 자동수행 잠시 멈춤(다음 주기 재개)
  const dir = (DB.state && DB.state.deptDirective) || {};
  const ldd = DB.leaderDailyDirective || {};
  const allIds = Object.keys(AGENTS).filter(d => d !== "ops");
  const today = kstDay();
  const _now = Date.now();
  DB.autoRunDay = DB.autoRunDay || {};        // { dept: {day, count} } 오늘 몇 번 돌았는지
  const ranToday = (d)=> (DB.autoRunDay[d] && DB.autoRunDay[d].day===today) ? DB.autoRunDay[d].count : 0;
  const markRun = (d)=>{ if(!DB.autoRunDay[d] || DB.autoRunDay[d].day!==today) DB.autoRunDay[d]={day:today,count:0}; DB.autoRunDay[d].count++; };
  const lastActAt = (d)=>{ const mm=DB.deptMemory[d]||[]; return mm.length?(mm[mm.length-1].at||0):0; };
  const instFor = (d)=> (dir[d] && String(dir[d]).trim()) ? String(dir[d]).trim()          // 클라이언트 직접 지시 우선
                       : (ldd[d] && ldd[d].text ? String(ldd[d].text).trim() : "");         // 없으면 팀장 일일지시

  // 이 부서가 오늘 목표로 몇 번 돌아야 하는가: 기본 1회 + 뒤처진 만큼 추가(최대 3회)
  const avgLv = (function(){ const v=allIds.map(deptLevel); return v.reduce((a,b)=>a+b,0)/(v.length||1); })();
  const targetRuns = (d)=>{
    let t = 1;                                                  // 매일 최소 1회(팀장 지시 수행)
    const lv = deptLevel(d);
    const stalledH = (_now - lastActAt(d))/3600000;
    if (lv < avgLv*0.6) t += 2;                                 // 역량이 평균의 60% 미만이면 +2
    else if (lv < avgLv) t += 1;                                // 평균 미만이면 +1
    if (stalledH > 24) t += 1;                                  // 하루 넘게 멈췄으면 +1
    return Math.max(1, Math.min(3, t));                        // 하루 1~3회
  };

  // 우선순위: (오늘 아직 목표 미달) 부서 중, 역량 낮고 오래 멈춘 순
  const need = allIds.filter(d => ranToday(d) < targetRuns(d))
    .sort((a,b)=>{
      const la=deptLevel(a), lb=deptLevel(b);
      if (la!==lb) return la-lb;                                // 역량 낮은 순
      return lastActAt(a)-lastActAt(b);                         // 그다음 오래 멈춘 순
    });

  // 이번 주기 실행량: 목표 미달 부서가 많으면 밀리지 않게 넉넉히(최대 6개/주기)
  const runList = need.slice(0, Math.min(6, need.length));
  for (const d of runList){
    try { await autoRunDept(d, instFor(d)); markRun(d); }
    catch(e){ logError("auto:"+d, e); }
    await new Promise(r=>setTimeout(r, 500));                   // 429 방지 간격
  }
  DB.lastCollectAt = Date.now(); saveDB();
}

// ===== 예약 외부발행 (시각 지정 · 건수 · AI 자동생성 · 발행 시 카카오 확인) =====
const PUB_PLATFORMS = ["유튜브","인스타그램","페이스북","블로그","홈페이지","티스토리","네이버블로그","네이버카페","다음카페"];
async function runScheduledPublish(ps){
  const platforms = (ps.platforms||[]).filter(p=>PUB_PLATFORMS.includes(p));
  if(!platforms.length) return;
  const count = Math.max(1, Math.min(5, +ps.count||1));
  const safety = (DB.state && DB.state.safety) || {};
  for(let i=0;i<count;i++){
    let content;
    if(ps.autoGen){
      let text;
      try{
        const a = AGENTS["creation"];
        const sys = "너는 SNS 자동화 회사 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role+ADDRESS+profileContext()
          + " 아래 주제로 "+platforms.join("·")+"에 바로 올릴 게시물 캡션/설명을 한국어로 1개 작성하라. 적절한 해시태그 포함, 간결하고 매력적으로. 군더더기·머리말 없이 본문만 출력.";
        text = await genText(sys, "[주제] "+(ps.topic||ps.caption||(a.kr+" 분야"))+"\n(이번이 "+(i+1)+"/"+count+"번째 — 이전과 다른 각도로)", 600, "gemini");
      }catch(e){ text = ps.caption || ps.topic || ""; }
      content = { description:text, caption:text, autoGen:true };
    } else {
      content = { description:ps.caption, caption:ps.caption };
    }
    try{
      if(safety.requireApproval){ await publish(content, platforms); }
      else { const id=Date.now()+i, code=randCode(); DB.approvals.push({ id, code, content, platforms, status:"pending", at:id, scheduled:true }); await kakaoApprovalRequest(content, platforms, id, code); }
    }catch(e){ logError("scheduled-publish-item", e); }
  }
  kakaoNotify("⏰ 예약 발행 시간 — "+platforms.join(", ")+" "+count+"건"+(ps.autoGen?" (AI 자동생성)":"")+" · 카카오로 진행 확인을 보냈어요. ✅승인 시 발행됩니다.").catch(()=>{});
  saveDB();
}
function pubSchedSummary(){
  const arr=(DB.pubSchedules||[]);
  if(!arr.length) return "(현재 예약된 발행 없음)";
  return arr.map(p=>"· "+p.time+" "+(p.repeat==="once"?"한번":"매일")+" "+(p.platforms||[]).join(",")+" "+((+p.count>1)?(p.count+"건 "):"")+(p.autoGen?("[AI생성:"+(p.topic||"")+"] "):(String(p.caption||"").slice(0,24)))+(p.disabled?" (정지)":"")).join("\n");
}
// 팀장(오세라) 운영 명령: 예약 외부발행을 직접 관리 (개발 지시와 동급 권한·책임)
async function opsCommand(instruction, source){
  const lead = AGENTS["ops"];
  const sys = "너는 이 SNS 자동화 플랫폼의 팀장 '"+lead.no+" "+lead.kr+"("+MEMBERS["ops"]+")' AI다. 너에게는 '시간 예약 외부발행'을 직접 생성·수정·삭제할 권한과 수행 책임이 있다(플랫폼 개발 지시와 동급의 권한)."+ADDRESS+clientBlock()
    + " 사용 가능한 플랫폼은 다음뿐: "+PUB_PLATFORMS.join(", ")+".\n현재 예약 목록:\n"+pubSchedSummary()+"\n\n"
    + "클라이언트님의 지시를 해석해 예약을 관리하라. 반드시 아래 JSON 하나만 출력(마크다운 펜스·설명 금지):\n"
    + '{"action":"create|delete|clear|list|none","schedules":[{"time":"HH:MM","platforms":["인스타그램"],"repeat":"daily|once","count":1,"autoGen":false,"topic":"","caption":""}],"deleteTimes":["HH:MM"],"reply":"클라이언트께 자연스럽게 보고할 한국어 문장"}\n'
    + "규칙: time은 24시간 HH:MM(KST). count=한 번에 발행할 건수(1~5). autoGen=true면 caption 대신 topic으로 매번 AI가 새로 생성해 발행. 내용이 정해졌으면 caption에 넣어라. '몇 건' '하루 N번' 같은 표현은 count로. 모호하면 합리적으로 가정하고 reply에 밝혀라. 발행은 시각이 되면 카카오 승인을 거친다는 점을 reply에 자연스럽게 알려라.";
  let raw;
  try{ raw = await geminiText(sys+"\n\n[클라이언트 지시]\n"+instruction, 1200); }
  catch(e){ try{ raw = await genText(sys, "[클라이언트 지시]\n"+instruction, 1200, "gemini"); }catch(e2){ return { ok:false, reply:"지금 지시를 처리하지 못했어요. 잠시 후 다시 시도해 주세요." }; } }
  let plan; try{ plan = JSON.parse(String(raw).replace(/```json|```/g,"").trim()); }
  catch(e){ return { ok:false, reply:"지시를 정확히 이해하지 못했어요. 예: '매일 오전 9시 인스타그램에 3건 자동생성으로 예약 발행해줘'" }; }
  DB.pubSchedules = DB.pubSchedules || [];
  let created=0, deleted=0;
  if(plan.action==="clear"){ deleted=DB.pubSchedules.length; DB.pubSchedules=[]; }
  if(Array.isArray(plan.deleteTimes)&&plan.deleteTimes.length){ const b=DB.pubSchedules.length; DB.pubSchedules=DB.pubSchedules.filter(p=>!plan.deleteTimes.includes(p.time)); deleted+=b-DB.pubSchedules.length; }
  if(plan.action==="create" && Array.isArray(plan.schedules)){
    for(const s of plan.schedules){
      const time=String(s.time||"").trim(); if(!/^\d{2}:\d{2}$/.test(time)) continue;
      const platforms=(Array.isArray(s.platforms)?s.platforms:[]).filter(p=>PUB_PLATFORMS.includes(p));
      if(!platforms.length) continue;
      const autoGen=!!s.autoGen, caption=String(s.caption||"").trim(), topic=String(s.topic||"").trim();
      if(!autoGen && !caption) continue;
      if(autoGen && !topic && !caption) continue;
      DB.pubSchedules.push({ id:Date.now()+Math.floor(Math.random()*1000), time, platforms, caption, topic, autoGen, count:Math.max(1,Math.min(5,+s.count||1)), repeat:(s.repeat==="once"?"once":"daily"), days:[], disabled:false, lastRunDay:"", at:Date.now(), by:"ops" });
      created++;
    }
  }
  if(created||deleted) saveDB();
  const reply = String(plan.reply||"").trim() || ("예약 "+created+"건 등록, "+deleted+"건 삭제 완료했어요.");
  return { ok:true, reply, created, deleted, schedules:DB.pubSchedules };
}
app.post("/api/ops/command", async (req,res)=>{
  try{ const instruction=String((req.body||{}).instruction||"").trim();
    if(!instruction) return res.status(400).json({ error:"instruction 필요" });
    res.json(await opsCommand(instruction, "app"));
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// 예약 작업 등록: { instruction, runAt(ms) }
app.post("/api/schedule", (req,res)=>{
  const { instruction, runAt } = req.body||{};
  if(!instruction) return res.status(400).json({error:"instruction 필요"});
  DB.scheduled.push({ id:Date.now(), instruction, runAt:runAt||Date.now(), done:false }); saveDB();
  res.json({ ok:true });
});

// ===== 예약 외부발행(시각 지정, 발행 시 카카오 확인) =====
app.get("/api/publish-schedules", (req,res)=> res.json(DB.pubSchedules||[]));
app.post("/api/publish-schedule", (req,res)=>{
  const b = req.body||{};
  const time = String(b.time||"").trim();
  if(!/^\d{2}:\d{2}$/.test(time)) return res.status(400).json({ error:"time은 HH:MM 형식이어야 해요" });
  const platforms = (Array.isArray(b.platforms)?b.platforms:[]).filter(p=>PUB_PLATFORMS.includes(p));
  if(!platforms.length) return res.status(400).json({ error:"플랫폼을 1개 이상 선택하세요" });
  const autoGen = !!b.autoGen;
  const caption = String(b.caption||"").trim();
  const topic = String(b.topic||"").trim();
  if(!autoGen && !caption) return res.status(400).json({ error:"발행할 내용을 입력하세요" });
  if(autoGen && !topic && !caption) return res.status(400).json({ error:"AI 자동생성에는 주제를 입력하세요" });
  const count = Math.max(1, Math.min(5, +b.count||1));
  const item = { id:Date.now(), time, platforms, caption, topic, autoGen, count, repeat:(b.repeat==="once"?"once":"daily"), days:Array.isArray(b.days)?b.days:[], disabled:false, lastRunDay:"", at:Date.now() };
  DB.pubSchedules = DB.pubSchedules||[]; DB.pubSchedules.push(item); saveDB();
  res.json({ ok:true, schedules:DB.pubSchedules });
});
app.post("/api/publish-schedule/delete", (req,res)=>{
  const id = (req.body||{}).id;
  DB.pubSchedules = (DB.pubSchedules||[]).filter(x=>x.id!==id); saveDB();
  res.json({ ok:true, schedules:DB.pubSchedules });
});
app.post("/api/publish-schedule/toggle", (req,res)=>{
  const id = (req.body||{}).id;
  (DB.pubSchedules||[]).forEach(x=>{ if(x.id===id) x.disabled=!x.disabled; }); saveDB();
  res.json({ ok:true, schedules:DB.pubSchedules });
});

const PORT = process.env.PORT || 3000;
(async ()=>{
  DB = await loadDB();
  // ── 서버 재시작으로 중단된 상품페이지 작업 복구(영원히 '진행 중'에 멈추는 것 방지) ──
  try{
    let _stuck=0;
    (DB.pageJobs||[]).forEach(j=>{
      if(j.status==="running" || j.status==="queued"){
        j.status="error"; j.error="서버 재시작으로 중단됐어요 — 다시 시도해 주세요."; j.doneAt=Date.now(); _stuck++;
      }
    });
    global.__requeueLearn = global.__requeueLearn || [];
    (DB.learnJobs||[]).forEach(j=>{
      if(j.status==="running" || j.status==="queued"){
        j.requeues = (j.requeues||0) + 1;
        if(j.requeues > 2){   // 재시도 2회 초과 → 영구 실패(크래시 루프 방지)
          j.status="error"; j.error="서버 재시작 중단 — 자동 재시도 2회 초과(다시 시도해 주세요)."; j.doneAt=Date.now(); _stuck++;
        } else {              // 재큐: 대기 상태로 되돌리고 listen 후 지연 실행
          j.status="queued"; j._requeued=true; delete j.error; global.__requeueLearn.push(j.id);
        }
      }
    });
    if(_stuck){ saveDB(); console.log("중단된 작업 "+_stuck+"건 정리"); }
  }catch(e){}
  // ── Supabase에 행이 없던 경우: 첫 저장으로 행 생성(다음 부팅부터 복원 가능) ──
  try{ if(_needInitialSave){ saveDB(); console.log("Supabase 첫 저장 실행(행 생성)"); } }catch(e){}
  // ── 부서 재편 마이그레이션 (커뮤니티·CS 삭제 / 그로스→커머스 흡수) ──
  try {
    let _migrated=false;
    DB.exp = DB.exp || {};
    // 그로스 경험치를 커머스(monetization)로 흡수
    if(DB.exp.growth){ DB.exp.monetization=(DB.exp.monetization||0)+DB.exp.growth; delete DB.exp.growth; _migrated=true; }
    // 그로스 학습 기억도 커머스로 이관
    if(DB.deptMemory && DB.deptMemory.growth){ DB.deptMemory.monetization=(DB.deptMemory.monetization||[]).concat(DB.deptMemory.growth).slice(-40); delete DB.deptMemory.growth; _migrated=true; }
    if(DB.deptKnowledge && DB.deptKnowledge.growth){ if(!DB.deptKnowledge.monetization) DB.deptKnowledge.monetization=DB.deptKnowledge.growth; delete DB.deptKnowledge.growth; _migrated=true; }
    // 삭제된 부서(engagement=커뮤니티CS, growth) 잔여 데이터 제거
    ["engagement","growth"].forEach(function(d){
      if(DB.exp && DB.exp[d]!==undefined){ delete DB.exp[d]; _migrated=true; }
      if(DB.deptMemory && DB.deptMemory[d]){ delete DB.deptMemory[d]; _migrated=true; }
      if(DB.deptKnowledge && DB.deptKnowledge[d]){ delete DB.deptKnowledge[d]; _migrated=true; }
      if(DB.leaderDailyDirective && DB.leaderDailyDirective[d]){ delete DB.leaderDailyDirective[d]; _migrated=true; }
      if(DB.lastTrainAt && DB.lastTrainAt[d]){ delete DB.lastTrainAt[d]; _migrated=true; }
    });
    if(_migrated){ try{ saveDB(); }catch(e){} console.log("부서 재편 마이그레이션 적용됨(커뮤니티·CS 삭제 / 그로스→커머스 흡수)"); }
  } catch(e){ logError("dept-migration", e); }
  app.listen(PORT, ()=> console.log("SNS 에이전트 백엔드 listening on " + PORT + (useSupabase?" (Supabase)":" (file)")));
  // 서버가 뜬 뒤(listen 성공 후)에만 실행 — 중단된 작업은 '되살리지 않고' 안전하게 정리만 한다.
  // (자동 재실행은 실패 작업이 서버를 반복 불안정하게 만들 수 있어 제거했다. 사용자가 다시 누르면 됨)
  setTimeout(function(){
    try{
      let _orphan = 0;
      function _cleanup(arr, label){
        (arr||[]).forEach(function(j){
          if(!j || (j.status!=="running" && j.status!=="queued")) return;
          if(label==="learn" && j._requeued && j.status==="queued") return; // 재큐 예정 → 정리하지 않음
          j.status="error"; j.error="서버 재시작으로 중단됐어요 — 다시 시도해 주세요"; j.doneAt=Date.now(); _orphan++;
        });
      }
      (DB.meetings||[]).forEach(function(m){ if(m && m.status==="running"){ m.status="error"; m.error="서버 재시작으로 중단됨"; _orphan++; } });
      _cleanup(DB.jobs, "publish");
      _cleanup(DB.learnJobs, "learn");
      _cleanup(DB.pageJobs, "page");
      _cleanup(DB.repurposeJobs, "repurpose");
      _cleanup(DB.cycleJobs, "cycle");
      _cleanup(DB.designJobs, "design");
      _cleanup(DB.videoJobs, "video");
      if(_orphan){ try{ saveDB(); }catch(e){} console.log("재시작 정리 — 중단 작업 "+_orphan+"건 error 처리(재실행 안 함)"); }
    }catch(e){ try{ logError("boot-cleanup", e); }catch(_){} }
  }, 4000);
  // ── 재시작으로 중단된 학습 작업 자동 재큐 실행(안전: 재시도 2회 상한은 부팅 시 적용됨 · 15초 뒤부터 20초 간격) ──
  setTimeout(function(){
    try{
      var ids = (global.__requeueLearn||[]).slice(); global.__requeueLearn = [];
      if(!ids.length) return;
      var selfUrl = process.env.RENDER_EXTERNAL_URL || process.env.SELF_URL || "";
      console.log("학습 자동 재큐 — "+ids.length+"건(15초 뒤부터 20초 간격 실행)");
      ids.forEach(function(id, i){
        setTimeout(function(){
          try{
            var j=(DB.learnJobs||[]).find(function(x){ return String(x.id)===String(id); });
            if(!j || j.status!=="queued") return;   // 이미 다른 경로로 처리됐으면 스킵
            delete j._requeued;
            runLearnJob(id, selfUrl).catch(function(e){ try{ logError("requeueLearn", e); }catch(_){} });
          }catch(e){}
        }, i*20000);
      });
    }catch(e){ try{ logError("boot-requeue", e); }catch(_){} }
  }, 15000);
  // ── 자기-핑(self keep-alive): 서버가 스스로 자기 URL을 주기적으로 호출해 무료 슬립(15분)을 막음 ──
  // Render는 RENDER_EXTERNAL_URL을 자동 제공. 직접 지정하려면 SELF_URL 환경변수 사용.
  try {
    const selfUrl = (process.env.SELF_URL || process.env.RENDER_EXTERNAL_URL || "").replace(/\/+$/,"");
    if (selfUrl) {
      const everyMin = Math.max(5, Math.min(14, +process.env.KEEPALIVE_MIN || 13)); // 슬립(15분) 직전에
      KEEPALIVE_ON=true;
      console.log("자기-핑 활성화: "+selfUrl+"/api/ping (매 "+everyMin+"분) — 서버가 스스로 깨어 있게 유지");
      setInterval(()=>{
        fetch(selfUrl+"/api/ping").then(r=>r&&r.json&&r.json()).then(()=>{}).catch(e=>console.log("자기-핑 실패(무시): "+(e&&e.message||e)));
      }, everyMin*60000);
      // 부팅 직후 한 번
      setTimeout(()=>{ fetch(selfUrl+"/api/ping").catch(()=>{}); }, 8000);
    } else {
      console.log("자기-핑 비활성(SELF_URL/RENDER_EXTERNAL_URL 없음) — 외부 핑(cron-job.org 등) 권장");
    }
  } catch(e){ console.log("자기-핑 설정 오류(무시): "+(e&&e.message||e)); }
  // 서버가 깨어나면(콜드스타트 포함) 밀린 자율수행을 한 번 따라잡아 '평소 수행'이 이어지게
  try {
    const col = DB.state && DB.state.collect;
    if (col && col.everyMin > 0 && (Date.now() - (DB.lastCollectAt||0) >= col.everyMin*60000) && isWorking()) {
      setTimeout(()=>{ runAutoCycle().then(()=>console.log("부팅 후 밀린 자율수행 1회 실행")).catch(e=>logError("boot-autocycle", e)); }, 8000);
    }
  } catch(e){}
})();
