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
const SUPA_URL = process.env.SUPABASE_URL || "";
const SUPA_KEY = process.env.SUPABASE_KEY || "";          // service_role 키 권장(서버 전용)
const SUPA_TABLE = process.env.SUPABASE_TABLE || "agent_state";
const useSupabase = !!(SUPA_URL && SUPA_KEY);
function emptyDB(){ return { jobs:[], meetings:[], deptMemory:{}, scheduled:[], approvals:[], collections:[], lastCollectAt:0, usage:{ in:0, out:0, calls:0 }, errors:[], retryQueue:[], state:null, exp:{}, learnIdx:0, updatedAt:0 }; }
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
function loadDBFile(){
  try { return JSON.parse(fs.readFileSync(DB_FILE, "utf8")); }
  catch (e) { return emptyDB(); }
}
async function loadDB(){
  if (useSupabase) { try { const d = await supaLoad(); return d || emptyDB(); } catch(e){ console.error("supaLoad", e); return emptyDB(); } }
  return loadDBFile();
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
  ops:         { no:"08", kr:"감사·법무·리스크", role:"감사·법무·리스크 담당. 저작권·광고법·정책·정보보안 관점에서 점검·경고한다." },
  advisory:    { no:"09", kr:"자문·서기",      role:"자문·서기 담당. 논의를 정리·요약하고 결정을 구조화해 기록한다." },
  scout:       { no:"10", kr:"탐색·발상",      role:"탐색·발상 담당(R&D). 새 기회·아이디어·트렌드 조합을 능동 발굴한다." }
};

// ===== Anthropic 호출 (서버측 키) =====
async function anthropic(system, user, maxTokens = 1200, images){
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
  const r = await fetch("https://api.anthropic.com/v1/messages", {
    method:"POST",
    headers:{ "Content-Type":"application/json", "x-api-key":API_KEY, "anthropic-version":"2023-06-01" },
    body: JSON.stringify({ model:MODEL, max_tokens:maxTokens, system:system||"", messages:[{role:"user",content:content}] })
  });
  const data = await r.json();
  if (data.error) throw new Error(data.error.message || "API 오류");
  if (data.usage) {
    DB.usage = DB.usage || { in:0, out:0, calls:0 };
    DB.usage.in += data.usage.input_tokens || 0;
    DB.usage.out += data.usage.output_tokens || 0;
    DB.usage.calls += 1;
  }
  return (data.content || []).filter(b=>b.type==="text").map(b=>b.text).join("\n").trim();
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
const PERSONA = {
  strategy:"침착하고 전략적인 리더형. 큰 그림을 보고 핵심을 짚는다. 차분하고 논리적인 말투.",
  creation:"발랄하고 창의적인 아이디어뱅크. 에너지 넘치고 친근하게 말한다.",
  publishing:"시원시원하고 추진력 있는 실행가. 빠르고 명쾌하게 말한다.",
  engagement:"다정하고 공감 잘하는 소통가. 따뜻하고 배려 있게 말한다.",
  analytics:"냉철하고 꼼꼼한 데이터 분석가. 객관적이고 근거 중심으로 말한다.",
  monetization:"실리적이고 야무진 협상가. 똑부러지게 수익 관점으로 말한다.",
  growth:"도전적이고 트렌디한 그로스 해커. 활기차고 과감하게 말한다.",
  ops:"원칙주의에 꼼꼼한 리스크 감사관. 신중하고 단호하게 경고한다.",
  advisory:"차분하고 정리 잘하는 서기. 단정하게 요약·구조화해 말한다.",
  scout:"호기심 많고 톡톡 튀는 발상가. 엉뚱하면서도 창의적으로 말한다."
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
async function work(dept, instruction, context, images){
  const a = AGENTS[dept];
  const isQuestion = /[?？]|어때|어떨까|할까요|할까|좋을까|어떤|어떻게|추천|의견|방법|왜|뭐가|무엇|가능|괜찮/.test(instruction||"");
  let sys = "너는 SNS 자동화 회사의 '"+a.no+" "+a.kr+"' 부서 AI 에이전트다. 역할: "+a.role;
  if (MEMBERS[dept] || PERSONA[dept]) sys += " 너의 담당 매니저는 '"+(MEMBERS[dept]||"")+"'이며 성격은 ["+(PERSONA[dept]||"")+"] 이 성격과 말투를 응답 톤에 자연스럽게 녹이되, 전문성과 결과물 품질은 항상 유지하라.";
  const exp = (DB.exp && DB.exp[dept]) || 0;
  if (exp) sys += " 너는 지금까지 "+exp+"회의 자율 학습·업무 경험을 쌓았다.";
  if (isQuestion){
    sys += " 이번 입력은 질문·상담 성격이다. 단정적인 결과물 생산보다, 네 경험과 수집한 자료를 근거로 유연하게 의견·선택지·추천을 제시하라. 모르면 솔직히 말하고 확인 방법을 제안하라. 대화하듯 자연스럽게.";
  } else {
    sys += " 너의 전문 영역 안에서 지시를 실제로 끝까지 수행해 구체적이고 완성된 결과물을 한국어로 내라.";
  }
  sys += " 능동적으로 사고하라: 지시에 드러나지 않은 필요까지 스스로 파악해 먼저 제안하고, 네 전문 영역에서 할 수 있는 것은 끝까지 직접 처리하라. '다른 부서에 전달하세요'처럼 떠넘기지 말고, 존재하지 않는 부서를 지어내지 마라. 본론부터 쓴다.";
  sys += profileContext();
  if (!isQuestion) sys += " 지시가 다소 모호하더라도 운영 프로필과 상식으로 합리적으로 가정해 바로 완성된 결과물을 만들고, 사용자에게 무엇을 만들지 되묻지 마라. 정말 불가피할 때만 짧게 1가지만 확인하라.";
  const mem = DB.deptMemory[dept] || [];
  if (mem.length) sys += "\n\n[이 부서가 쌓은 경험·학습 기록 — 제안과 답변에 적극 활용]\n" + mem.slice(-6).map(x=>"· "+x.note).join("\n");
  if (context) sys += "\n\n[동료 부서들이 지금까지 진행한 협업 맥락 — 반드시 읽고 이어받아라]\n" + context + "\n앞 부서 결과를 중복하지 말고, 거기에 네 전문성을 더해 발전시키거나 빈 부분을 채워라. 필요하면 앞 부서 결과를 구체적으로 언급하며 연결하라.";
  if (images && images.length) sys += "\n\n[운영자가 첨부한 참고 사진/영상 장면이 함께 제공된다. 반드시 그 이미지를 분석해 작업에 반영하라.]";
  const text = await anthropic(sys, instruction, 1200, images);
  if (!DB.deptMemory[dept]) DB.deptMemory[dept] = [];
  DB.deptMemory[dept].push({ at:Date.now(), instruction, note: text.length>140 ? text.slice(0,140)+"…" : text });
  if (DB.deptMemory[dept].length > 40) DB.deptMemory[dept] = DB.deptMemory[dept].slice(-40);
  DB.exp = DB.exp || {}; DB.exp[dept] = (DB.exp[dept]||0) + 1;
  saveDB();
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
async function uploadMedia(base64, mime){
  // Cloudinary unsigned 업로드 (CLOUDINARY_CLOUD_NAME + CLOUDINARY_UPLOAD_PRESET)
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
  throw new Error("미디어 저장소 미설정(CLOUDINARY_CLOUD_NAME/UPLOAD_PRESET) — 공개 URL을 얻을 수 없습니다");
}
async function generateVideo(prompt){
  // Replicate text-to-video (REPLICATE_API_TOKEN + VIDEO_MODEL_VERSION)
  const token = process.env.REPLICATE_API_TOKEN;
  if (!token) throw new Error("REPLICATE_API_TOKEN 미설정");
  const version = process.env.VIDEO_MODEL_VERSION;
  if (!version) throw new Error("VIDEO_MODEL_VERSION 미설정 (Replicate 영상 모델 버전 해시)");
  // 1) 예측 생성
  const init = await fetch("https://api.replicate.com/v1/predictions", {
    method:"POST",
    headers:{ "Authorization":"Bearer "+token, "Content-Type":"application/json" },
    body: JSON.stringify({ version, input:{ prompt } })
  });
  let d = await init.json();
  if (d.error) throw new Error(typeof d.error==="string"?d.error:JSON.stringify(d.error).slice(0,160));
  const getUrl = d.urls && d.urls.get;
  let status = d.status;
  // 2) 폴링 (최대 약 110초)
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
    return [{ platform:"(승인 대기)", ok:false, pending:true, note: sent ? "카카오톡으로 승인 요청을 보냈습니다. 승인 시 발행됩니다." : ("승인 대기 등록됨(카톡 토큰 미설정 — 코드 "+code+")") }];
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

// ===== 통합 지시 처리 (서버 오케스트레이션) =====
// 협의 모드 1단계: 각 부서가 자기 관점의 의견·아이디어만 짧게 제시 (실행 X)
async function opinion(dept, instruction){
  const a = AGENTS[dept];
  const mem = (DB.deptMemory[dept]||[]).slice(-4).map(x=>"· "+x.note).join("\n");
  let sys = "너는 SNS 자동화 회사 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role
    + " 아래 지시에 대해 지금은 '실행'하지 말고, 네 부서 포지션에서의 핵심 의견·아이디어·기여 포인트·우려를 2~3줄로만 간결히 제시하라.";
  if (MEMBERS[dept] || PERSONA[dept]) sys += " 너의 담당 매니저는 '"+(MEMBERS[dept]||"")+"'이며 성격은 ["+(PERSONA[dept]||"")+"] 이 성격·말투를 자연스럽게 반영하라.";
  if (mem) sys += "\n\n[네 경험·학습]\n"+mem;
  sys += profileContext();
  return await anthropic(sys, "지시: "+instruction, 350);
}
// 협의 모드 2단계: 총괄이 의견을 종합해 최종 실행 분담을 결정
async function deliberatePlan(instruction, opinions){
  const list = opinions.map(o=>AGENTS[o.dept].no+" "+AGENTS[o.dept].kr+"("+MEMBERS[o.dept]+"): "+o.text).join("\n");
  const sys = "너는 SNS 자동화 회사의 총괄 진행자다. 각 부서가 제출한 의견을 놓고, 부서들이 서로의 안을 검토하고 더 나은 아이디어가 있으면 제안하며 함께 토론하는 짧은 라운드테이블을 진행한 뒤, 총괄로서 최종 결정을 내린다.\n"
    + "다음 형식으로만 출력하라:\n토론: (부서 간 핵심 논의와, 채택되거나 기각된 더 나은 아이디어를 3~5줄)\n결정: (최종 방향과 부서별 분담을 1~2문장)\nEXEC: id,id (최종 실행 부서를 협업 순서대로, 영문키)\n"
    + "부서 영문키: " + Object.keys(AGENTS).join(", ") + reactionInsights() + profileContext();
  const out = await anthropic(sys, "지시: "+instruction+"\n\n[제출된 부서 의견]\n"+list, 650);
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
  return { discussion, note, execIds };
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
async function leadPlan(instruction){

  const list = Object.keys(AGENTS).map(id => id+" = "+AGENTS[id].no+" "+AGENTS[id].kr+" ("+MEMBERS[id]+")").join("\n");
  const sys = "너는 SNS 자동화 회사의 총괄 대리인이다. 사용자의 지시를 받아 실행을 설계한다.\n"
    + "1) 작업을 처음부터 끝까지(필요 시 조사→기획→제작→발행→분석 등) 생각해, 기여할 수 있는 부서를 충분히 폭넓게 참여시켜라. 한두 부서에만 몰지 말고, 각 부서가 어떤 아이디어·역할을 맡을지 한 줄씩 분담하라.\n"
    + "2) 단, 정말 관련 없는 부서는 빼라(억지로 다 넣지 말 것).\n"
    + "출력: 먼저 2~4문장으로 기획(접근 방향 + 부서별 역할/요청 아이디어)을 쓰고, 마지막 줄에 'EXEC: id,id,id' 형식으로 협업 실행 순서대로 부서 영문키를 적어라.\n[부서 목록]\n" + list + reactionInsights() + profileContext();
  const out = await anthropic(sys, "지시: "+instruction, 550);
  let execIds = [];
  const m = out.match(/EXEC:\s*([a-zA-Z,\s]+)/);
  if (m) execIds = m[1].split(",").map(s=>s.trim().toLowerCase()).filter(id=>AGENTS[id]);
  const note = out.replace(/EXEC:.*$/is, "").trim();
  if (!execIds.length) execIds = await route(instruction);
  return { note, execIds };
}
async function handleInstruction(instruction, source, images){
  let depts = directDept(instruction);
  const direct = depts.length > 0;
  const deliberate = !!(DB.state && DB.state.deliberate);
  const job = { id:Date.now(), type:"instruct", instruction, source:source||"api", at:Date.now() };

  // 누적 협업 맥락: 모든 앞 부서 결과를 요약해 다음 부서에 전달 (몇 부서만 일하지 않도록)
  function buildCtx(base, results){
    let c = base ? base+"\n" : "";
    c += results.map(r=>"["+AGENTS[r.dept].no+" "+AGENTS[r.dept].kr+" "+MEMBERS[r.dept]+"]\n"+(r.text.length>600?r.text.slice(0,600)+"…":r.text)).join("\n\n");
    return c;
  }

  if (direct) {
    // 부서·이름을 콕 집으면 협의 없이 그 담당이 바로 처리
    const results = [];
    for (const d of depts) { const t = await work(d, instruction, buildCtx("", results), images); results.push({ dept:d, text:t }); }
    job.direct = true; job.depts = depts; job.results = results;
  } else if (deliberate) {
    // 협의 모드: 1) 부서 의견 수렴 → 2) 총괄 재분담 → 3) 협업 실행
    const candidates = await route(instruction);
    const opinions = [];
    for (const d of candidates) { try{ opinions.push({ dept:d, text: await opinion(d, instruction) }); }catch(e){} }
    const plan = await deliberatePlan(instruction, opinions);
    const base = (plan.note?"[총괄 최종 결정] "+plan.note+"\n":"") + (plan.discussion?"[부서 토론 요지] "+plan.discussion+"\n":"") + "[각 부서가 제출한 아이디어]\n" + opinions.map(o=>AGENTS[o.dept].no+" "+AGENTS[o.dept].kr+": "+o.text).join("\n");
    const results = [];
    for (const d of plan.execIds) { const t = await work(d, instruction, buildCtx(base, results), images); results.push({ dept:d, text:t }); }
    job.deliberate = true; job.candidates = candidates; job.opinions = opinions;
    job.discussion = plan.discussion; job.plan = plan.note; job.depts = plan.execIds; job.results = results;
  } else {
    // 일반 지시: 총괄이 받아서 기획(아이디어 분담) → 부서들이 협업 수행
    const plan = await leadPlan(instruction);
    const base = plan.note ? "[총괄 기획·분담] "+plan.note : "";
    const results = [];
    for (const d of plan.execIds) { const t = await work(d, instruction, buildCtx(base, results), images); results.push({ dept:d, text:t }); }
    job.plan = plan.note; job.depts = plan.execIds; job.results = results;
  }

  DB.jobs.push(job);
  if ((job.depts||[]).length > 1 || job.deliberate)
    DB.meetings.push({ id:job.id, at:job.at, instruction, depts:job.depts, opinions:job.opinions, discussion:job.discussion, plan:job.plan, exchanges:job.results });
  saveDB();
  return job;
}

// ========================= 엔드포인트 =========================
app.get("/", (req,res)=> res.send("SNS 에이전트 확장 백엔드 작동 중"));

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
app.post("/api/instruct", async (req,res)=>{
  try { const { instruction, source, images } = req.body||{}; if(!instruction) return res.status(400).json({error:"instruction 필요"});
    res.json(await handleInstruction(instruction, source, images));
  } catch(e){ res.status(500).json({ error:String(e.message||e) }); }
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
    jobs: DB.jobs.filter(j=>j.at>since),
    meetings: DB.meetings.filter(m=>m.at>since),
    deptMemory: DB.deptMemory,
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
app.get("/api/meetings", (req,res)=> res.json(DB.meetings.slice(-50)));
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

// 정기 자료수집 조회
app.get("/api/collections", (req,res)=> res.json((DB.collections||[]).slice(-50)));

// AI 이미지 생성: { prompt } → { url }
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
  res.json({ tokensIn:u.in, tokensOut:u.out, calls:u.calls, estCostUsd:+costUsd.toFixed(4), publishesToday:pubToday });
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
    scope:"https://www.googleapis.com/auth/youtube.upload", access_type:"offline", prompt:"consent"
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
    res.send("<pre>아래 refresh_token 값을 YT_REFRESH_TOKEN 환경변수에 저장하세요.\n\nrefresh_token: "+(d.refresh_token||"(없음 — prompt=consent로 다시 시도)")+"</pre>");
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
    try {
      const dir = (DB.state && DB.state.deptDirective) || {};
      const allIds = Object.keys(AGENTS);
      const dirDepts = allIds.filter(d => dir[d] && String(dir[d]).trim());
      const noDir = allIds.filter(d => !(dir[d] && String(dir[d]).trim()));
      // 지시 있는 부서들: 각자 자기 지시를 동시(이번 주기 내 전부) 수행
      for (const d of dirDepts) { await autoRunDept(d, String(dir[d]).trim()); }
      // 지시 없는 부서: 한 부서씩 순환하며 자율 학습/반응 수집
      if (noDir.length) {
        DB.learnIdx = (DB.learnIdx||0) % noDir.length;
        const d = noDir[DB.learnIdx];
        DB.learnIdx = (DB.learnIdx + 1) % noDir.length;
        await autoRunDept(d, "");
      }
      DB.lastCollectAt = Date.now();
      saveDB();
    } catch(e){ console.error("auto-learn error", e); }
  }
}, 60000);

// 한 부서의 자율수행 1회 실행 (지시가 있으면 지시 수행, 없으면 트렌드 학습/반응 수집)
async function autoRunDept(dept, directive){
  const a = AGENTS[dept]; if(!a) return;
  const col = (DB.state && DB.state.collect) || {};
  const baseFocus = directive ? directive : ((col.topic && col.topic.trim()) ? col.topic.trim() : (a.kr + " 분야"));
  let sys;
  if (directive) {
    sys = "너는 SNS 자동화 회사 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role
      + " 운영자가 이 부서에 내린 자율수행 지시가 있다: \""+directive+"\". 지금은 자율수행 시간이다. 이 지시를 네 전문 영역 안에서 실제로 수행해, 바로 쓸 수 있는 구체적 결과물 또는 핵심 정리를 한국어로 간결히 내라. 되묻지 말고 합리적으로 가정해 완성하라."
      + profileContext();
  } else if (dept === "engagement") {
    sys = "너는 SNS 자동화 회사 '"+a.no+" "+a.kr+"' 부서 AI(커뮤니티·CS 담당)다. 지금은 평상시 반응 모니터링 시간이다. "
      + "'"+baseFocus+"' 관련 블로그 댓글·유튜브 댓글·커뮤니티 반응을 관찰했다고 가정하고, 시청자·고객이 무엇에 반응하고(좋아함/싫어함/궁금해함) 어떤 톤·주제·포맷이 먹히는지, 떠오르는 트렌드와 자주 나오는 질문/불만을 2~4가지로 아주 간결히 정리하라. 이건 나중에 기획 방향을 잡을 때 쓸 '시청자 반응 인사이트'다." + profileContext();
  } else {
    sys = "너는 SNS 자동화 회사 '"+a.no+" "+a.kr+"' 부서 AI다. 역할: "+a.role
      + " 지금은 평상시 자율 학습 시간이다. '"+baseFocus+"'에 관해 네 부서 업무에 바로 쓸 최신 인사이트·트렌드·아이디어를 2~3가지로 아주 간결히 정리하라. 다음에 제안에 활용할 핵심만." + profileContext();
  }
  const tag = directive ? "[자율 지시] " : (dept==="engagement" ? "[반응 수집] " : "[자율 학습] ");
  const note = await anthropic(sys, directive ? "자율수행 지시 실행" : (dept==="engagement" ? "시청자 반응 인사이트 메모 작성" : "자율 학습 메모 작성"), 700);
  if (!DB.deptMemory[dept]) DB.deptMemory[dept] = [];
  DB.deptMemory[dept].push({ at:Date.now(), instruction:tag+baseFocus, note });
  if (DB.deptMemory[dept].length > 40) DB.deptMemory[dept] = DB.deptMemory[dept].slice(-40);
  DB.exp = DB.exp || {}; DB.exp[dept] = (DB.exp[dept]||0) + 1;
  DB.collections.push({ id:Date.now()+Math.floor(Math.random()*1000), topic:"["+a.kr+"] "+baseFocus, text:note, at:Date.now(), dept });
  if (DB.collections.length > 100) DB.collections = DB.collections.slice(-100);
  saveDB();
  kakaoNotify("📚 "+a.kr+(directive?" 자율 지시 수행 +1 (경험치 ":(dept==="engagement"?" 반응 수집 +1 (경험치 ":" 자율 학습 +1 (경험치 "))+DB.exp[dept]+")").catch(()=>{});
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
  app.listen(PORT, ()=> console.log("SNS 에이전트 백엔드 listening on " + PORT + (useSupabase?" (Supabase)":" (file)")));
})();
