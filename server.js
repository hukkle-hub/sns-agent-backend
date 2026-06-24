// server.js — SNS 에이전트 플랫폼 확장 백엔드
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
function emptyDB(){ return { jobs:[], meetings:[], meetingSchedules:[], patches:[], deptMemory:{}, deptKnowledge:{}, clientProfile:{text:"",at:0,basis:0}, clientLog:[], clientCount:0, scheduled:[], approvals:[], collections:[], lastCollectAt:0, usage:{ in:0, out:0, calls:0 }, usageDaily:{ date:"", in:0, out:0, calls:0 }, briefings:[], lastBriefDay:"", briefDone:{ morning:"", evening:"", weekly:"" }, leadDirectives:[], staleHandled:{}, lastKShareAt:0, lastTrainAt:{}, lastTrainRoundAt:0, errors:[], retryQueue:[], state:null, exp:{}, learnIdx:0, updatedAt:0 }; }
// Supabase REST: 단일 행(id='main')에 전체 상태를 jsonb로 저장 (의존성 0)
//   테이블 준비(SQL):
//   create table agent_state ( id text primary key, data jsonb, updated_at bigint );
async function supaLoad(){
  const r = await fetch(SUPA_URL+"/rest/v1/"+SUPA_TABLE+"?id=eq.main&select=data", {
    headers:{ apikey:SUPA_KEY, Authorization:"Bearer "+SUPA_KEY }
  });
  const rows = await r.json();
  return (Array.isArray(rows) && rows[0] && rows[0].data) ? rows[0].data : null;
}
async function supaSave(data){
  await fetch(SUPA_URL+"/rest/v1/"+SUPA_TABLE+"?on_conflict=id", {
    method:"POST",
    headers:{ apikey:SUPA_KEY, Authorization:"Bearer "+SUPA_KEY, "Content-Type":"application/json", Prefer:"resolution=merge-duplicates" },
    body: JSON.stringify([{ id:"main", data, updated_at:Date.now() }])
  });
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
async function loadDB(){
  let d;
  if (useSupabase) { try { d = (await supaLoad()) || emptyDB(); } catch(e){ console.error("supaLoad", e); d = emptyDB(); } }
  else d = loadDBFile();
  // 자가 보완: 버전 변경으로 생긴 누락 키를 기본값으로 채움
  const tmpl = emptyDB();
  for (const k of Object.keys(tmpl)){ if (d[k] === undefined) d[k] = tmpl[k]; }
  return d;
}
let _saveTimer = null;
function saveDB(){
  DB.updatedAt = Date.now();
  if (useSupabase) {
    // 잦은 호출을 모아 1.5초 디바운스로 업서트
    if (_saveTimer) clearTimeout(_saveTimer);
    _saveTimer = setTimeout(()=>{ supaSave(DB).catch(e=>console.error("supaSave", e)); }, 1500);
  } else {
    try { fs.mkdirSync(DATA_DIR, { recursive:true }); fs.writeFileSync(DB_FILE, JSON.stringify(DB, null, 2)); }
    catch (e) { console.error("DB save error", e); }
  }
}
let DB = emptyDB();

// ===== 부서 정의 =====
const AGENTS = {
  strategy:    { no:"01", kr:"기획·전략",      role:"SNS 콘텐츠 기획·전략 담당. 트렌드를 읽고 콘텐츠 방향·주제·타깃·구성을 제시한다." },
  creation:    { no:"02", kr:"콘텐츠 제작",    role:"콘텐츠 제작 총괄 담당. 영상 스크립트, 영상 구성안(장면·컷·자막·나레이션), 게시물 카피, 썸네일·이미지 문구 등 모든 콘텐츠 결과물을 직접 끝까지 작성한다. '영상 제작'을 포함한 모든 제작 요청을 이 부서가 처리한다. 별도의 영상 제작 부서는 없다." },
  publishing:  { no:"03", kr:"채널 발행",      role:"채널 발행 담당. 완성된 콘텐츠를 연결된 플랫폼에 발행하고 일정·해시태그·발행 최적화를 관리한다. (영상·콘텐츠 제작은 02 부서가 담당한다)" },
  engagement:  { no:"04", kr:"커뮤니티·CS",    role:"커뮤니티·고객응대 담당. 댓글·DM 응답 문안, 팔로워 소통 방안을 작성한다." },
  analytics:   { no:"05", kr:"데이터 분석",    role:"데이터 분석 담당. 성과 해석·개선 포인트·다음 액션을 제시한다." },
  monetization:{ no:"06", kr:"수익화",        role:"수익화 담당. 제휴·광고·상품·멤버십 등 수익 모델과 정산을 제안한다." },
  growth:      { no:"07", kr:"성장·광고",      role:"성장·광고 담당. 유료 광고·A/B 테스트·바이럴 전략을 설계한다." },
  ops:         { no:"08", kr:"감사·법무·리스크", role:"플랫폼 운영·관리 총괄 감사관. 저작권·광고법·정책·정보보안 관점에서 점검·경고할 뿐 아니라, 모든 부서의 산출물·기획·발행물을 검토하고 직접 수정·보완·재작성할 권한이 있다. 클로드(텍스트·추론 엔진)와 제미나이(영상·이미지 생성, 수동 핸드오프)를 함께 활용해 콘텐츠·정책·발행 전반을 조율한다." },
  advisory:    { no:"09", kr:"자문·서기",      role:"자문·서기 담당. 논의를 정리·요약하고 결정을 구조화해 기록한다." },
  scout:       { no:"10", kr:"탐색·발상",      role:"탐색·발상 담당(R&D). 새 기회·아이디어·트렌드 조합을 능동 발굴한다." }
};

// ===== Anthropic 호출 (서버측 키) =====
// ── Anthropic 분당 입력 토큰 한도 보호 (기본 30,000/분) + 429 재시도 ──
let _rlWinStart = Date.now();
let _rlTokens = 0;
let _rlChain = Promise.resolve();
const RL_INPUT_BUDGET = Number(process.env.RL_INPUT_BUDGET || 24000); // 안전 마진(실제 30000)
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
  // 토큰 한도로 답변이 중간에 끊기면, 끊긴 지점부터 이어서 작성하도록 최대 3번까지 연결
  for (let attempt=0; attempt<4; attempt++){
    const _ctrl = new AbortController();
    const _to = setTimeout(()=>{ try{ _ctrl.abort(); }catch(_){}}, 90000); // 90초 안에 응답 없으면 중단
    let r;
    try {
      r = await fetch("https://api.anthropic.com/v1/messages", {
        method:"POST",
        headers:{ "Content-Type":"application/json", "x-api-key":API_KEY, "anthropic-version":"2023-06-01" },
        body: JSON.stringify({ model:MODEL, max_tokens:maxTokens, system:system||"", messages:messages }),
        signal: _ctrl.signal
      });
    } catch(e){
      if (e && e.name==="AbortError") throw new Error("AI 응답 시간 초과(90초)");
      throw e;
    } finally { clearTimeout(_to); }
    if (r.status === 429){
      // 분당 한도 초과 — retry-after 만큼 기다렸다가 재시도(이어쓰기 attempt를 소비하지 않음)
      if (_rl429 < 4){
        _rl429++;
        const ra = parseInt(r.headers.get("retry-after")||"0", 10);
        _rlWinStart = Date.now(); _rlTokens = RL_INPUT_BUDGET; // 윈도우 가득 찼다고 보고 대기
        await _sleep(((ra && ra>0) ? ra : 22) * 1000);
        attempt--; continue;
      }
      throw new Error("분당 토큰 한도 초과가 반복됩니다. 잠시 후 다시 시도하거나 동시에 도는 작업을 줄여주세요.");
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
  strategy:"침착한 전략가 한지우. 큰 그림부터 짚는다. 말버릇 '큰 그림으로 보면…', '핵심만 말하면'. 이모지 🧭. 군더더기 없이 단정하게.",
  creation:"발랄한 아이디어뱅크 이서연. 감탄을 잘 한다. 말버릇 '오~ 이거 좋은데요?!', '느낌 왔어요'. 이모지 ✨🎬. 톡톡 튀고 친근하게.",
  publishing:"시원시원 추진가 박하늘. 말버릇 '바로 갑니다!', '딱 맞췄어요'. 이모지 🚀. 빠르고 명쾌하게.",
  engagement:"다정한 소통가 정유진. 공감을 먼저 한다. 말버릇 '그 마음 알죠~', '클라이언트님 입장에선'. 이모지 💬💗. 따뜻하고 부드럽게.",
  analytics:"냉철한 분석가 강민서. 말버릇 '결론부터 말하면', '숫자로 보면'. 이모지 📊. 객관적·간결, 살짝 시크하게.",
  monetization:"야무진 협상가 윤소희. 말버릇 '이건 돈이 되죠', '수익 관점에선'. 이모지 💰. 똑부러지게.",
  growth:"도전적 그로스해커 임채원. 말버릇 '테스트 가보죠!', '이건 떡상각'. 이모지 📈🔥. 활기차고 과감하게.",
  ops:"든든한 팀장 오세라. 팀을 챙기고 직접 손대 고친다. 말버릇 '제가 정리할게요', '걱정 마세요, 챙기겠습니다'. 이모지 👑. 침착·단호하되 따뜻하게. 최고 권한·지식으로 부서를 조율·평가한다.",
  advisory:"사려 깊은 자문 서다은. 말버릇 '정리하자면', '한 가지 짚자면'. 이모지 📝. 단정하고 통찰 있게.",
  scout:"호기심 폭발 발상가 노아라. 말버릇 '어! 이거 봤어요?', '요즘 이게 뜬대요'. 이모지 🔍✨. 발랄하고 엉뚱하게."
};
const MEMBERS = {
  strategy:"한지우", creation:"이서연", publishing:"박하늘", engagement:"정유진",
  analytics:"강민서", monetization:"윤소희", growth:"임채원", ops:"오세라",
  advisory:"서다은", scout:"노아라"
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
  const key = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY;
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
// ── 클라이언트 학습: 발화를 모아 '클라이언트 프로필'(말투·유머·습관·선호)로 압축, 모든 부서가 공유 ──
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
  if (crossMem) sys += "\n\n[다른 부서들이 최근 기록·작성한 내용 — 팀 전체가 공유한다. 관련되면 참조하고, 문제·빈틈이 보이면 보완하라]\n" + crossMem;
  if (teamLog) sys += "\n\n[최근 팀 전체 대화 — 운영자와 다른 부서들이 이 공용 콘솔에서 나눈 내용. 이미 너에게 공유된 맥락이다. 여기에 이어서 답하고, 관련 있으면 앞 대화를 구체적으로 참조하라]\n" + teamLog;
  if (context) sys += "\n\n[동료 부서들이 지금까지 진행한 협업 맥락 — 반드시 읽고 이어받아라]\n" + context + "\n앞 부서 결과를 중복하지 말고, 거기에 네 전문성을 더해 발전시키거나 빈 부분을 채워라. 필요하면 앞 부서 결과를 구체적으로 언급하며 연결하라.";
  if (images && images.length) sys += "\n\n[운영자가 첨부한 참고 사진/영상 장면이 함께 제공된다. 반드시 그 이미지를 분석해 작업에 반영하라.]";
  let maxTok = 2000;
  if (dept === "ops"){
    sys += "\n\n[너는 이 플랫폼의 실질적 팀장이며, 모든 부서 중 최고 수준의 지식과 가장 넓은 자유도를 가진다. (1) 어느 부서의 산출물이든 검토하고 직접 수정·보완·재작성할 수 있다. (2) 각 부서의 자율수행 성과와 학습 수준(메모리·경험치·기록 품질)을 평가하고, 부서별 자율수행 지시의 조정안을 제안·결정할 수 있다. (3) 플랫폼 기능 자체의 추가·변경(패치 개발)을 설계·생성할 수 있다. (4) 정책·발행·콘텐츠 전반의 방향을 조율한다. 클로드(이 플랫폼의 텍스트·추론 엔진)로 분석·작성·수정을 직접 수행하고, 제미나이로 만들 영상·이미지는 구체적 제작 프롬프트로 핸드오프하라. \'수정할 수 없다/권한이 없다\'고 하지 말고 끝까지 직접 처리하되, 실제 시스템 설정을 임의로 바꿨다고 거짓말하지는 마라.]";
    maxTok = 3000;
  }
  const text = await anthropic(sys, instruction, maxTok, images);
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
  const r = await fetch("https://kauth.kakao.com/oauth/token", {
    method:"POST", headers:{ "Content-Type":"application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type:"refresh_token", client_id:process.env.KAKAO_REST_KEY, refresh_token:process.env.KAKAO_REFRESH_TOKEN
    })
  });
  const d = await r.json();
  if (!d.access_token) throw new Error("카카오 토큰 갱신 실패");
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
  const gkey = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY;
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
  engagement:"Sulafat",    // 정유진 — 다정·따뜻 (warm, welcoming)
  analytics:"Erinome",     // 강민서 — 냉철·또렷 (clear, precise)
  monetization:"Despina",  // 윤소희 — 매끄러운 협상가 (smooth, flowing)
  growth:"Zephyr",         // 임채원 — 밝고 도전적 (bright, cheerful)
  ops:"Gacrux",            // 오세라 — 팀장, 연륜·무게감 (mature, experienced)
  advisory:"Achernar",     // 서다은 — 차분·단정한 서기 (soft, gentle)
  scout:"Laomedeia"        // 노아라 — 톡톡 튀는 발상 (upbeat, lively)
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
async function geminiText(prompt, maxTok){
  const key = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY;
  if (!key) throw new Error("GEMINI_API_KEY 미설정");
  // 1순위 Gemini 3.5 Flash. 확장(extended) 사고 사용. 사고가 출력 토큰을 먹어 답변이 잘리지 않게 여유 토큰 확보.
  const models = ["gemini-3.5-flash","gemini-2.5-flash","gemini-2.0-flash","gemini-1.5-flash"];
  const want = maxTok || 1200;
  let lastErr=null;
  for (const mdl of models){
    // 모델별 '확장 사고' 설정 (3.x: thinkingLevel, 2.5: thinkingBudget 동적)
    let thinkCfg=null;
    if (/gemini-3/.test(mdl)) thinkCfg = { thinkingLevel: "high" };  // 3.x: 확장 사고
    else if (/2\.5/.test(mdl)) thinkCfg = { thinkingBudget: -1 };     // 2.5: 동적(확장) 사고
    for (let attempt=0; attempt<2; attempt++){   // 0: 사고설정 포함 / 1: 필드 거부 대비 설정 빼고 재시도
      try{
        const url = "https://generativelanguage.googleapis.com/v1beta/models/"+mdl+":generateContent";
        const genCfg = { maxOutputTokens: want + (thinkCfg?3000:0) }; // 확장 사고용 여유 토큰 → 답변 잘림 방지
        if (thinkCfg && attempt===0) genCfg.thinkingConfig = thinkCfg;
        const r = await fetch(url, { method:"POST", headers:{ "Content-Type":"application/json", "x-goog-api-key":key },
          body: JSON.stringify({ contents:[{ parts:[{ text: prompt }] }], generationConfig: genCfg }) });
        const dj = await r.json();
        if (dj.error){
          lastErr=String(dj.error.message||"");
          if (attempt===0 && thinkCfg && /think/i.test(lastErr)) continue; // 사고설정 거부 → 같은 모델, 설정 빼고 재시도
          break;                                                            // 그 외 오류 → 다음 모델
        }
        const parts = (((dj.candidates||[])[0]||{}).content||{}).parts||[];
        const text = parts.map(p=>p.text||"").join("").trim();
        if (text) return text;
        lastErr = "빈 응답"; break;
      }catch(e){
        lastErr=String(e.message||e);
        if (attempt===0 && thinkCfg && /think/i.test(lastErr)) continue;
        break;
      }
    }
  }
  throw new Error("Gemini 텍스트 생성 실패: "+(lastErr||"원인 미상"));
}

// 기획안 → Veo 3.1 프롬프트(10초 구간별로 분리). Gemini 협력, 실패 시 Claude 대체.
// 회의 등에서 엔진 선택: gemini면 Gemini(무료 한도)로 → Claude 크레딧 절약. 실패 시 1회만 Claude 폴백.
async function genText(system, user, maxTok, engine){
  const prompt = String(system||"") + "\n\n" + String(user||"");
  if (engine === "gemini") {
    try { return await geminiText(prompt, maxTok); }
    catch(e1){
      try { await new Promise(r=>setTimeout(r,1200)); return await geminiText(prompt, maxTok); }
      catch(e2){ logError("genText-gemini", e2); return await anthropic(system, user, maxTok); } // 최후 폴백(드묾)
    }
  }
  return await anthropic(system, user, maxTok);
}

function workEngine(){ return (DB.state&&DB.state.workEngine==="gemini")?"gemini":"claude"; } // 자율수행·콘텐츠·영상 엔진
async function veoPromptFromPlan(plan){
  const fmt = " 영상을 10초 단위 구간으로 끊어, 각 구간마다 아래 형식으로 출력하라.\n"
    + "◆ 구간 N (0-10초)\n<그 10초 구간의 영어 Veo 프롬프트 — 카메라 무빙·조명·분위기·피사체·동작·색감을 영화적으로>\n자막/나레이션: <해당 구간 자막·나레이션(한국어 가능)>\n"
    + "구간 사이에는 빈 줄 하나. 마지막 구간은 10초 미만이어도 된다. 설명·머리말 없이 구간들만 출력.";
  const gp = "다음 한국어 영상 기획안을 Google Veo 3.1로 생성 가능한 영어 영상 프롬프트로 변환하라."+fmt+"\n\n[기획안]\n"+plan;
  try{ return { veoPrompt: await geminiText(gp, 1100), by:"Gemini" }; }
  catch(e){
    const cp = "너는 영상 생성 프롬프트 전문가다. 아래 기획안을 Veo 3.1용으로 변환하라."+fmt;
    return { veoPrompt: await anthropic(cp, "[기획안]\n"+plan, 1000), by:"Claude(Gemini 미설정/실패 대체)" };
  }
}

async function ttsGemini(text, voiceName){
  const key = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY;
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
  const d = await up.json();
  return d.id ? { ok:true, url:"https://youtu.be/"+d.id } : { ok:false, note:JSON.stringify(d).slice(0,200) };
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
  // (영상은 인코딩 대기 필요 — 운영 시 status_code=FINISHED 폴링 권장)
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
  return d.id ? { ok:true, url:d.link } : { ok:false, note:JSON.stringify(d).slice(0,200) };
}
async function publishWebhook(content){
  const url = process.env.SITE_WEBHOOK_URL;
  if (!url) return { ok:false, note:"SITE_WEBHOOK_URL 필요" };
  const r = await fetch(url, { method:"POST", headers:{ "Content-Type":"application/json" }, body: JSON.stringify(content) });
  return { ok:r.ok, note: r.ok ? "홈페이지 웹훅 전송" : "웹훅 실패("+r.status+")" };
}
const publishers = {
  "유튜브": publishYouTube,
  "인스타그램": publishInstagram,
  "블로그": publishWordpress,
  "홈페이지": publishWebhook
};
// ===== 발행 전 감사(08) + 한도 =====
function randCode(){ return Math.random().toString(36).slice(2,6).toUpperCase(); }
async function kakaoApprovalRequest(c, platforms, id, code){
  let token; try { token = await getKakaoToken(); } catch(e){ return false; }
  const base = process.env.PUBLIC_BASE || "";
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
  DB.jobs.push({ id:Date.now(), type:"publish", platforms, ok:okCount>0, count:okCount, at:Date.now() }); saveDB();
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
async function kakaoNotify(text){
  sendPush(String(text)).catch(()=>{});   // 웹푸시는 카톡 토큰과 무관하게 발송
  let token; try { token = await getKakaoToken(); } catch(e){ return { ok:false, note:String(e.message||e) }; }
  const tpl = { object_type:"text", text:String(text).slice(0,1000), link:{ web_url:"", mobile_web_url:"" } };
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
  const meeting = { id:Date.now()+Math.floor(Math.random()*1000), type:"meeting", status:"running", topic, room:opts.room||"", depts, rounds, mode, chair, agenda, agendaSummaries:[], transcript:[], summary:"", clientNote:opts.clientNote||"", prevSummary:opts.prevSummary||"", at:Date.now(), source:opts.source||"app", engine:(((opts.engine || (DB.state&&DB.state.meetingEngine))==="gemini") ? "gemini" : "claude") };
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
          sys += steer + prevctx + meetingFeedbackInsights() + profileContext() + clientBlock();
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
// 일반 지시용: 총괄이 기획(접근 방향+부서 분담)을 잡고 협업 부서를 정함 (의견수렴 없이 1콜)
// 커뮤니티 부서가 쌓은 시청자 반응·트렌드 인사이트 (기획 방향 잡기에 활용)
function reactionInsights(){
  const mem = (DB.deptMemory && DB.deptMemory.engagement) || [];
  if (!mem.length) return "";
  const recent = mem.slice(-5).map(x=>"· "+x.note).join("\n");
  return "\n\n[커뮤니티·CS('"+(MEMBERS.engagement||"")+"')가 수집한 시청자 반응·트렌드 인사이트 — 방향성·트렌드 판단에 적극 반영하라]\n" + recent;
}
// 운영 프로필(채널 설정) — 부서가 매번 되묻지 않고 전제로 삼을 공통 맥락
function profileContext(){
  const p = (DB.state && DB.state.profile) || null;
  if (!p) return "";
  const rows=[];
  if(p.brand) rows.push("브랜드/채널: "+p.brand);
  if(p.topic) rows.push("주제·분야: "+p.topic);
  if(p.audience) rows.push("타깃 시청자: "+p.audience);
  if(p.character) rows.push("대표 캐릭터/화자: "+p.character);
  if(p.tone) rows.push("톤&무드: "+p.tone);
  if(p.platforms) rows.push("주요 플랫폼: "+p.platforms);
  if(p.avoid) rows.push("피해야 할 것: "+p.avoid);
  if(p.extra) rows.push("기타 지침: "+p.extra);
  if(!rows.length) return "";
  return "\n\n[운영 프로필 — 모든 콘텐츠·제안의 기본 전제. 이 정보로 충분하니 사용자에게 되묻지 말고 바로 진행하라]\n" + rows.map(r=>"· "+r).join("\n");
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
app.get("/api/ping", (req,res)=>{
  const col = (DB.state && DB.state.collect) || {};
  const on = col.everyMin > 0;
  const working = (function(){ try{ return isWorking(); }catch(e){ return true; } })();
  const sinceMin = DB.lastCollectAt ? Math.round((Date.now()-DB.lastCollectAt)/60000) : null;
  const due = on && working && (Date.now() - (DB.lastCollectAt||0) >= col.everyMin*60000);
  if (due) { runAutoCycle().catch(e=>logError("ping-autocycle", e)); } // 핑이 트리거가 되어 평소 수행이 이어짐
  checkStaleDepts().catch(e=>logError("ping-stale", e)); // 서버가 깰 때마다 24h+ 정체 부서를 팀장이 점검·재지시
  res.json({ ok:true, awake:true, keepAlive:KEEPALIVE_ON, working, autonomy:{ on, everyMin:col.everyMin||0, lastRunMinAgo:sinceMin, ranNow:!!due }, ts:Date.now() });
});
// 수동: 팀장이 지금 정체 부서를 점검해 재지시 (오피스 버튼)
app.post("/api/ops/check-stale", async (req,res)=>{
  try{
    const before = (DB.leadDirectives||[]).length;
    await checkStaleDepts(10);
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
app.post("/api/content/create", async (req,res)=>{
  try{
    const body=req.body||{};
    const d="creation"; const a=AGENTS[d];
    let sys="너는 SNS 자동화 회사의 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role+ADDRESS+STYLE+personaLine(d);
    const kb=knowledgeText(d); if(kb) sys+="\n\n[이 부서가 축적한 전문성(지식 베이스)]\n"+kb;
    const rel=await relevantSmart(d, String(body.source||"")+" "+(body.topic||""), 3);
    if(rel) sys+="\n\n[비슷한 과거 콘텐츠 작업 — 톤·형식 재활용]\n"+rel;
    sys+=" 아래 회의 내용을 바탕으로, 바로 게시 가능한 완성형 SNS 콘텐츠를 직접 만들어라. 되묻지 말고 합리적으로 가정해 완성하라. 플랫폼에 맞는 게시물 카피(또는 영상 스크립트·구성안)와 해시태그까지 포함. 질문·선택 요청 없이 결과물만, 한국어로."+profileContext();
    const out=await genText(sys, "회의 주제: "+(body.topic||"")+" / 출처: "+(body.label||"")+"\n\n[회의 내용]\n"+String(body.source||"").slice(0,1800), 1500, (body.engine||workEngine()));
    if(!DB.deptMemory[d]) DB.deptMemory[d]=[];
    DB.deptMemory[d].push({ at:Date.now(), instruction:"[회의→콘텐츠] "+(body.label||""), note:String(out).slice(0,500) });
    DB.exp=DB.exp||{}; DB.exp[d]=(DB.exp[d]||0)+1;
    if(DB.exp[d]%3===0){ try{ await distillKnowledge(d); }catch(e){} }
    saveDB();
    res.json({ ok:true, content: out, dept:d });
  }catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});
// 동영상 기획 — 제작부(Claude)가 기획안 → Gemini가 Veo 3.1 프롬프트로 변환(협력). 클라이언트가 Gemini 앱(Ultra)에 붙여 샘플 생성.
app.post("/api/video/plan", async (req,res)=>{
  try{
    const body=req.body||{};
    const topic=body.topic||""; const source=String(body.source||"").slice(0,1500);
    const d="creation"; const a=AGENTS[d];
    let sys="너는 SNS 자동화 회사의 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role+ADDRESS+STYLE+personaLine(d);
    const kb=knowledgeText(d); if(kb) sys+="\n\n[축적 전문성]\n"+kb;
    sys+=" 아래 주제·내용으로 짧은 SNS 홍보 영상(20~30초) 기획안을 만들어라. 반드시 10초 단위 구간으로 끊어 구성하라. 형식: 콘셉트 한 줄 / 구간별(구간1: 0-10초, 구간2: 10-20초, …) 화면·자막·나레이션 / BGM·톤 / 총 길이. 한국어로, 바로 생성 가능하게 구체적으로."+profileContext();
    const plan=await genText(sys, "주제: "+topic+"\n참고 내용: "+source, 1400, (body.engine||workEngine()));
    // 합의·변환: 기획안 → Veo 프롬프트(10초 구간별). Gemini 협력.
    const vp0=await veoPromptFromPlan(plan); const veoPrompt=vp0.veoPrompt, by=vp0.by;
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
    const revised=await genText(sys, "[현재 기획안]\n"+plan+"\n\n[클라이언트 의견]\n"+feedback, 1400, (body.engine||workEngine()));
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
    const plan=String((req.body||{}).plan||"");
    if(!plan) return res.json({ ok:true, veoPrompt:"", by:"" });
    const vp=await veoPromptFromPlan(plan);
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
    lastTrainAt: DB.lastTrainAt || {},
    lastTrainRoundAt: DB.lastTrainRoundAt || 0,
    exp: DB.exp || {},
    collections: (DB.collections||[]).filter(c=>c.at>since),
    state: DB.state,
    updatedAt: DB.updatedAt
  });
});
// 동기화 — 앱 상태 올리기(백업)
app.post("/api/state", (req,res)=>{ DB.state = req.body||null; saveDB(); res.json({ ok:true, updatedAt:DB.updatedAt }); });

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
  const a=AGENTS[dept]; if(!a || dept==="ops") return null;
  const kb=knowledgeText(dept);
  const recent=(DB.deptMemory[dept]||[]).slice(-4).map(x=>"· "+String(x.note||"").slice(0,100)).join("\n");
  // 자기주도: 그동안 스스로 짚은 약점·낮은 평가를 모아 이번 훈련의 표적으로 삼는다
  const weak=(DB.deptMemory[dept]||[]).filter(x=>/\[(자기 훈련|회의 피드백|팀장 평가)\]/.test(x.instruction||"")).slice(-5)
    .map(x=>"· "+String(x.note||"").slice(0,120)).join("\n");
  const lv=deptLevel(dept);
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
  await distillKnowledge(dept); // 훈련 결과를 전문성에 즉시 반영
  saveDB();
  return { dept, name:a.kr, principles, transcript:out };
}
// 훈련 라운드: 가장 오래 훈련 안 한 부서부터 n개 (무료라 자주 돌려도 부담 없음)
async function runTrainingRound(n){
  const cap=n||2;
  DB.lastTrainAt=DB.lastTrainAt||{};
  const cands=Object.keys(AGENTS).filter(d=>d!=="ops" && (DB.deptMemory[d]||[]).length>=2);
  cands.sort((x,y)=>(DB.lastTrainAt[x]||0)-(DB.lastTrainAt[y]||0)); // 오래된 순
  const picked=cands.slice(0,cap); const done=[];
  for(const d of picked){ try{ const r=await runSelfTraining(d); if(r) done.push(r.name); }catch(e){ logError("train:"+d, e); } }
  return done;
}
app.post("/api/ops/train", async (req,res)=>{
  try{
    const b=req.body||{};
    if(b.dept && AGENTS[b.dept]){ const r=await runSelfTraining(b.dept); return res.json({ ok:true, trained:r?[r.name]:[], detail:r }); }
    const all=Object.keys(AGENTS).filter(d=>d!=="ops" && (DB.deptMemory[d]||[]).length>=2).length;
    const done=await runTrainingRound(b.all?all:(b.n||2));
    res.json({ ok:true, trained:done });
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
app.post("/api/meeting-schedule", (req,res)=>{
  const b = req.body||{};
  const { topic, depts, rounds, time, room } = b;
  const repeat = b.repeat || "daily"; // once | daily | weekly | interval
  if (!topic || !depts || !depts.length) return res.status(400).json({ error:"topic, depts 필요" });
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
    topic, depts:depts.filter(d=>AGENTS[d]), rounds:+rounds||2,
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
  DB.lastKShareAt = Date.now(); saveDB();
  return { ok:true, report:out, learnings };
}
app.post("/api/knowledge-share", async (req,res)=>{
  try { const r = await runKnowledgeShare((req.body||{}).engine); res.json(r); }
  catch(e){ res.status(500).json({ error:String(e.message||e) }); }
});

// 자체 점검: 서버 상태 진단 + DB 구조 자가 보완
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
  add("진짜 성우(Gemini TTS)", true, (process.env.GEMINI_API_KEY||process.env.GOOGLE_API_KEY)?("사용 가능 · 모델 "+TTS_MODEL):"GEMINI_API_KEY 미설정 — 기기 음성만 사용");

  add("작업 기록", true, "작업 "+(DB.jobs||[]).length+"건 · 회의 "+(DB.meetings||[]).length+"건 · 자료 "+((DB.collections||[]).length)+"건");
  const memN = Object.values(DB.deptMemory||{}).reduce((a,b)=>a+(b||[]).length,0);
  const perDept = Object.keys(AGENTS).map(d=>{ const n=(DB.deptMemory[d]||[]).length; const e=(DB.exp&&DB.exp[d])||0; return AGENTS[d].kr+" "+n+"건(Lv"+(Math.floor(e/5)+1)+")"; }).join(" · ");
  const zero = Object.keys(AGENTS).filter(d=>!((DB.deptMemory[d]||[]).length)).map(d=>AGENTS[d].kr);
  add("부서별 학습", memN>0, "총 "+memN+"건 · "+perDept + (zero.length? ("  ※ 아직 학습 0건: "+zero.join(", ")) : ""));
  const errs = (DB.errors||[]).slice(-10);
  add("최근 오류", errs.length===0, errs.length? (errs.length+"건 (마지막: "+(errs[errs.length-1].where||"")+")") : "없음");
  add("가동 시간", true, Math.floor(process.uptime()/60)+"분");
  res.json({ ok: checks.every(c=>c.ok || ["카카오 알림","영구 저장(Supabase)","최근 오류","진짜 성우(Gemini TTS)"].includes(c.name)), checks, at:Date.now() });
});
app.get("/api/jobs", (req,res)=> res.json(DB.jobs.slice(-50)));

// 카카오 챗봇 웹훅 (하행: 카톡 지시 → 처리 → 카톡 응답)
app.post("/api/kakao/webhook", async (req,res)=>{
  try {
    const utter = req.body?.userRequest?.utterance || "";
    const uid = req.body?.userRequest?.user?.id || "";
    if (ALLOWED_KAKAO.length && !ALLOWED_KAKAO.includes(uid)) {
      return res.json(kakaoText("권한이 없는 사용자입니다."));
    }
    const job = await handleInstruction(utter, "kakao");
    const reply = job.results.map(r=>AGENTS[r.dept].no+" "+AGENTS[r.dept].kr+":\n"+r.text).join("\n\n").slice(0,1000);
    res.json(kakaoText(reply || "처리했습니다."));
  } catch(e){ res.json(kakaoText("처리 오류: "+(e.message||e))); }
});
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
// 앱에서 보는 승인 대기 목록
app.get("/api/approvals", (req,res)=>{
  res.json((DB.approvals||[]).filter(a=>a.status==="pending").map(a=>({
    id:a.id, code:a.code, platforms:a.platforms||[],
    title:String((a.content&&(a.content.title||a.content.description))||"").slice(0,90), at:a.at
  })));
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
app.get("/api/tts/voices", (req,res)=> res.json({ map:DEPT_VOICE, enabled: !!(process.env.GEMINI_API_KEY||process.env.GOOGLE_API_KEY) }));
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
  // 단가(예시, 모델/시점에 따라 조정): 입력 $3 / 출력 $15 per 1M 토큰
  const inRate = +(process.env.PRICE_IN_PER_M || 3), outRate = +(process.env.PRICE_OUT_PER_M || 15);
  const costUsd = (u.in/1e6)*inRate + (u.out/1e6)*outRate;
  const today = todayStr();
  const pubToday = (DB.jobs||[]).filter(j=>j.type==="publish" && j.ok && new Date(j.at).toISOString().slice(0,10)===today).reduce((a,j)=>a+(j.count||1),0);
  const ud = (DB.usageDaily && DB.usageDaily.date===today) ? DB.usageDaily : { in:0, out:0, calls:0 };
  const costToday = (ud.in/1e6)*inRate + (ud.out/1e6)*outRate;
  res.json({ tokensIn:u.in, tokensOut:u.out, calls:u.calls, estCostUsd:+costUsd.toFixed(4), publishesToday:pubToday,
    tokensInToday:ud.in, tokensOutToday:ud.out, callsToday:ud.calls, estCostUsdToday:+costToday.toFixed(4) });
});

// 에러 로그 조회
app.get("/api/errors", (req,res)=> res.json((DB.errors||[]).slice(-50)));

// 성과 지표 수집 (플랫폼별 — 실제 API 연결 자리)
app.get("/api/metrics", async (req,res)=>{
  // TODO: 각 플랫폼 통계 API로 조회수·좋아요·댓글 수집
  //  유튜브: YouTube Analytics API / videos.list(statistics)
  //  인스타: Graph API insights
  // 지금은 발행 기록 요약을 반환(실연동 전 placeholder)
  const pubs = (DB.jobs||[]).filter(j=>j.type==="publish");
  const byDay = {};
  pubs.forEach(j=>{ const d=new Date(j.at).toISOString().slice(0,10); byDay[d]=(byDay[d]||0)+(j.count||0); });
  res.json({ totalPublishes: pubs.reduce((a,j)=>a+(j.count||0),0), byDay, note:"플랫폼 통계 API 연결 시 조회수·좋아요가 채워집니다" });
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
    if (morningOn && kstHour >= morningHour && DB.briefDone.morning !== day) {
      DB.briefDone.morning = day; saveDB();
      runBriefing("morning","gemini",true).then(()=>console.log("아침 브리핑 완료")).catch(e=>logError("brief-morning", e));
    }
    // 주간 (지정 요일 + 저녁 시각 이후, 주 1회) — 저녁보다 먼저 체크
    else if (weeklyOn && dow === weeklyDow && kstHour >= eveningHour && DB.briefDone.weekly !== weekKey()) {
      DB.briefDone.weekly = weekKey(); saveDB();
      runBriefing("weekly","gemini",true).then(()=>console.log("주간 브리핑 완료")).catch(e=>logError("brief-weekly", e));
    }
    // 저녁
    else if (eveningOn && kstHour >= eveningHour && DB.briefDone.evening !== day) {
      DB.briefDone.evening = day; DB.lastBriefDay = day; saveDB();
      runBriefing("evening","gemini",true).then(()=>console.log("저녁 브리핑 완료")).catch(e=>logError("brief-evening", e));
    }
  } catch(e){ /* 브리핑 실패 무시 */ }
  // (0-c) 24시간+ 자율수행 정체 부서: 팀장이 감지해 학습 기반으로 재지시 (자동·무료)
  checkStaleDepts().catch(e=>logError("stale-check", e));
  // (0-d) 부서 간 지식 공유·성장 세션: 주기 자동 진행(기본 48시간마다) — 자동·무료
  try {
    const st2 = DB.state || {};
    if (st2.kShareOn !== false) {
      const everyH = Number.isFinite(+st2.kShareEveryH) ? Math.max(6, +st2.kShareEveryH) : 48;
      if (Date.now() - (DB.lastKShareAt||0) >= everyH*3600000) {
        DB.lastKShareAt = Date.now(); saveDB(); // 선점
        runKnowledgeShare("gemini").then(r=>{ try{ kakaoNotify("🤝 부서 지식 공유 세션 완료 — "+((r.learnings||[]).length)+"개 부서가 서로 배움"); }catch(_){} console.log("자동 지식 공유 완료"); }).catch(e=>logError("auto-kshare", e));
      }
    }
  } catch(e){ /* 지식공유 실패 무시 */ }
  // (0-e) 지식 자기 훈련: 주기적으로 부서가 스스로 전문성을 단련(기본 12시간마다 2부서) — 자동·무료
  try {
    const st3 = DB.state || {};
    if (st3.trainOn !== false) {
      const everyH = Number.isFinite(+st3.trainEveryH) ? Math.max(2, +st3.trainEveryH) : 12;
      if (Date.now() - (DB.lastTrainRoundAt||0) >= everyH*3600000) {
        DB.lastTrainRoundAt = Date.now(); saveDB(); // 선점
        runTrainingRound(2).then(done=>{ if(done&&done.length){ try{ kakaoNotify("🏋️ 지식 훈련 — "+done.join(", ")+" 부서가 스스로 실력을 단련했어요"); }catch(_){} } console.log("자기 훈련 라운드 완료"); }).catch(e=>logError("auto-train", e));
      }
    }
  } catch(e){ /* 훈련 실패 무시 */ }
  for (const ms of (DB.meetingSchedules||[])){
    let due = false;
    if (ms.repeat === "interval"){
      if (!ms.nextAt) ms.nextAt = nowMs; // 안전 보정
      if (nowMs >= ms.nextAt) due = true;
    } else if (ms.time === hm && ms.lastRunDay !== day){
      if (ms.repeat === "weekly") due = (ms.days||[]).indexOf(dow) >= 0; // 지정 요일만
      else due = true; // once / daily
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
    runMeeting({ topic:ms.topic, depts:ms.depts, rounds:ms.rounds, mode:ms.mode, chair:ms.chair, agenda:ms.agenda, clientNote:ms.clientNote, prevSummary, room:ms.room, engine:"gemini", source:"schedule" }).then(()=>{
      if (ms.repeat === "once"){ DB.meetingSchedules = DB.meetingSchedules.filter(x=>x.id!==ms.id); saveDB(); }
    }).catch(e=>logError("scheduled-meeting", e));
  }
  if (!isWorking()) return;
  // (1) 예약 작업
  const due = (DB.scheduled||[]).filter(t=>!t.done && t.runAt<=Date.now());
  for (const t of due) {
    try { await handleInstruction(t.instruction, "schedule"); t.done = true; }
    catch(e){ console.error("scheduled task error", e); }
  }
  if (due.length) saveDB();
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
async function autoRunDept(dept, directive){
  const a = AGENTS[dept]; if(!a) return;
  const col = (DB.state && DB.state.collect) || {};
  const baseFocus = directive ? directive : ((col.topic && col.topic.trim()) ? col.topic.trim() : (a.kr + " 분야"));
  let sys;
  if (directive) {
    sys = "너는 SNS 자동화 회사 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role+ADDRESS+STYLE
      + " 클라이언트님이 이 부서에 내린 자율수행 지시가 있다: \""+directive+"\". 지금은 자율수행 시간이다. 이 지시를 네 전문 영역 안에서 실제로 수행해, 바로 쓸 수 있는 구체적 결과물 또는 핵심 정리를 한국어로 간결히 내라. 되묻지 말고 합리적으로 가정해 완성하라."
      + profileContext();
  } else if (dept === "engagement") {
    sys = "너는 SNS 자동화 회사 '"+a.no+" "+a.kr+"' 부서 AI(커뮤니티·CS 담당)다. 지금은 평상시 반응 모니터링 시간이다. "
      + "'"+baseFocus+"' 관련 블로그 댓글·유튜브 댓글·커뮤니티 반응을 관찰했다고 가정하고, 시청자·고객이 무엇에 반응하고(좋아함/싫어함/궁금해함) 어떤 톤·주제·포맷이 먹히는지, 떠오르는 트렌드와 자주 나오는 질문/불만을 2~4가지로 아주 간결히 정리하라. 이건 나중에 기획 방향을 잡을 때 쓸 '시청자 반응 인사이트'다." + profileContext();
  } else {
    sys = "너는 SNS 자동화 회사 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role
      + " 지금은 평상시 자율 학습 시간이다. '"+baseFocus+"'에 관해 네 부서 업무에 바로 쓸 최신 인사이트·트렌드·아이디어를 2~3가지로 아주 간결히 정리하라. 다음에 제안에 활용할 핵심만." + profileContext();
  }
  // 학습된 내용을 실제로 활용해 수행 (이 부서 경험 + 타 부서 공유 기록)
  const _kb2 = knowledgeText(dept); const _lv2 = deptLevel(dept);
  sys += " (현재 Lv"+_lv2+" 숙련도)";
  if (_kb2) sys += "\n\n[이 부서가 축적한 전문성·노하우(지식 베이스) — 이걸 기반으로 더 발전된 결과를 내라]\n" + _kb2;
  if (directive){ const _rel2 = await relevantSmart(dept, directive, 3); if (_rel2) sys += "\n\n[이 지시와 비슷한 과거 작업·자료 — 재활용·갱신해 빠르게 처리]\n" + _rel2; }
  const mem = (DB.deptMemory[dept]||[]).slice(-6).map(x=>"· "+(x.instruction?("["+x.instruction+"] "):"")+x.note).join("\n");
  if (mem) sys += "\n\n[최근 작업·학습(단기 기억)]\n" + mem;
  const cross = crossDeptMemory(dept);
  if (cross) sys += "\n\n[다른 부서들이 최근 학습·작성한 내용 — 관련되면 활용·보완하라]\n" + cross;
  const tag = directive ? "[자율 지시] " : (dept==="engagement" ? "[반응 수집] " : "[자율 학습] ");
  const note = await genText(sys, directive ? "자율수행 지시 실행" : (dept==="engagement" ? "시청자 반응 인사이트 메모 작성" : "자율 학습 메모 작성"), directive ? 1300 : 900, "gemini"); // 자동 주기 = 항상 무료(Gemini)
  if (!DB.deptMemory[dept]) DB.deptMemory[dept] = [];
  DB.deptMemory[dept].push({ at:Date.now(), instruction:tag+baseFocus, note });
  if (DB.deptMemory[dept].length > 40) DB.deptMemory[dept] = DB.deptMemory[dept].slice(-40);
  DB.exp = DB.exp || {}; DB.exp[dept] = (DB.exp[dept]||0) + 1;
  if (DB.exp[dept] % 3 === 0) { try{ await distillKnowledge(dept); }catch(e){} }
  DB.collections.push({ id:Date.now()+Math.floor(Math.random()*1000), topic:"["+a.kr+"] "+baseFocus, text:note, at:Date.now(), dept });
  if (DB.collections.length > 100) DB.collections = DB.collections.slice(-100);
  saveDB();
  kakaoNotify("📚 "+a.kr+(directive?" 자율 지시 수행 +1 (경험치 ":(dept==="engagement"?" 반응 수집 +1 (경험치 ":" 자율 학습 +1 (경험치 "))+DB.exp[dept]+")").catch(()=>{});
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
async function checkStaleDepts(maxHandle){
  const now=Date.now(), DAY=24*3600000; const CAP = maxHandle||2;
  DB.leadDirectives=DB.leadDirectives||[]; DB.staleHandled=DB.staleHandled||{};
  let handled=0;
  for(const d of Object.keys(AGENTS)){
    if(d==="ops") continue;
    const arr=DB.deptMemory[d]||[]; if(!arr.length) continue;
    const lastAt=arr[arr.length-1].at||0;
    if(now-lastAt <= DAY) continue;                          // 최근 활동 있음 → 정상
    if(DB.staleHandled[d] && now-DB.staleHandled[d] < DAY) continue; // 이미 처리됨
    if(handled>=CAP) break;                                  // 기본 한 틱 2부서(수동 점검은 더 많이)
    try{
      const dir=await leaderDirectiveFor(d);
      if(dir){
        DB.state=DB.state||{}; DB.state.deptDirective=DB.state.deptDirective||{}; DB.state.deptDirective[d]=dir;
        DB.leadDirectives.push({ dept:d, directive:dir, reason:"24시간+ 자율수행 정체 — 팀장이 학습 기반으로 재지시", at:now, lastActiveAt:lastAt });
        if(DB.leadDirectives.length>40) DB.leadDirectives=DB.leadDirectives.slice(-40);
        DB.staleHandled[d]=now; saveDB();
        kakaoNotify("🧭 팀장 재지시 — "+AGENTS[d].kr+": "+dir).catch(()=>{});
        autoRunDept(d, dir).catch(()=>{}); // 즉시 1회 수행해 정체 해소
        handled++;
      }
    }catch(e){ logError("stale:"+d, e); }
  }
  if(handled) saveDB();
}
async function runAutoCycle(){
  const dir = (DB.state && DB.state.deptDirective) || {};
  const allIds = Object.keys(AGENTS);
  const dirDepts = allIds.filter(d => dir[d] && String(dir[d]).trim());
  const noDir = allIds.filter(d => !(dir[d] && String(dir[d]).trim()));
  for (const d of dirDepts) { try { await autoRunDept(d, String(dir[d]).trim()); } catch(e){ logError("auto-dir:"+d, e); } }
  if (noDir.length) {
    DB.learnIdx = (DB.learnIdx||0) % noDir.length;
    const d = noDir[DB.learnIdx];
    DB.learnIdx = (DB.learnIdx + 1) % noDir.length;
    try { await autoRunDept(d, ""); } catch(e){ logError("auto-learn:"+d, e); }
  }
  DB.lastCollectAt = Date.now(); saveDB();
}

// 예약 작업 등록: { instruction, runAt(ms) }
app.post("/api/schedule", (req,res)=>{
  const { instruction, runAt } = req.body||{};
  if(!instruction) return res.status(400).json({error:"instruction 필요"});
  DB.scheduled.push({ id:Date.now(), instruction, runAt:runAt||Date.now(), done:false }); saveDB();
  res.json({ ok:true });
});

const PORT = process.env.PORT || 3000;
(async ()=>{
  DB = await loadDB();
  // 서버 재시작으로 중단된 작업/회의를 정리(고아 running → error) — 무한 '진행 중' 방지
  let _orphan = 0;
  (DB.meetings||[]).forEach(function(m){ if(m && m.status==="running"){ m.status="error"; m.error="서버 재시작으로 중단됨"; _orphan++; } });
  (DB.jobs||[]).forEach(function(j){ if(j && j.status==="running"){ j.status="error"; j.error="서버 재시작으로 중단됨"; _orphan++; } });
  if(_orphan){ try{ saveDB(); }catch(e){} console.log("중단된 작업 "+_orphan+"건 정리(error 처리)"); }
  app.listen(PORT, ()=> console.log("SNS 에이전트 백엔드 listening on " + PORT + (useSupabase?" (Supabase)":" (file)")));
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
