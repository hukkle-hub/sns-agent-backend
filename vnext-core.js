// Hukkle Hub vNext Core v0.1
// Drop-in ESM module. Keeps existing app architecture intact.

export const VNEXT_VERSION = "0.1.0";

export function ensureVNextDB(DB){
  if (!DB || typeof DB !== "object") throw new Error("DB object required");
  DB.vnext = DB.vnext || {};
  const v = DB.vnext;
  v.projects = Array.isArray(v.projects) ? v.projects : [];
  v.activeProjectId = v.activeProjectId || "";
  v.radarItems = Array.isArray(v.radarItems) ? v.radarItems : [];
  v.ideas = Array.isArray(v.ideas) ? v.ideas : [];
  v.experiments = Array.isArray(v.experiments) ? v.experiments : [];
  v.contents = Array.isArray(v.contents) ? v.contents : [];
  v.results = Array.isArray(v.results) ? v.results : [];
  v.projectKnowledge = v.projectKnowledge && typeof v.projectKnowledge === "object" ? v.projectKnowledge : {};
  v.usageEvents = Array.isArray(v.usageEvents) ? v.usageEvents : [];
  v.todayQueue = Array.isArray(v.todayQueue) ? v.todayQueue : [];
  return v;
}

export function normalizeProject(p = {}){
  const now = Date.now();
  return {
    id: String(p.id || ("prj_"+now+"_"+Math.random().toString(36).slice(2,7))),
    name: String(p.name || p.title || "새 프로젝트").trim(),
    type: String(p.type || "general"),
    goal: String(p.goal || p.brief || "").trim(),
    stage: ["discovery","growth","monetization","scale"].includes(p.stage) ? p.stage : "discovery",
    audience: String(p.audience || "").trim(),
    channels: Array.isArray(p.channels) ? p.channels : [],
    primaryKpi: String(p.primaryKpi || "progress"),
    secondaryKpis: Array.isArray(p.secondaryKpis) ? p.secondaryKpis : [],
    aiBudgetMonthly: Math.max(0, Number(p.aiBudgetMonthly || 0)),
    paidAutoExecution: p.paidAutoExecution === true,
    revenueMonthly: Math.max(0, Number(p.revenueMonthly || 0)),
    status: ["active","paused","archived"].includes(p.status) ? p.status : "active",
    workflowHints: Array.isArray(p.workflowHints) ? p.workflowHints : [],
    createdAt: Number(p.createdAt || now),
    updatedAt: now
  };
}

export function getProject(DB, projectId){
  const v = ensureVNextDB(DB);
  const id = projectId || v.activeProjectId;
  return v.projects.find(p => String(p.id) === String(id)) || null;
}

export function projectContext(DB, projectId){
  const p = getProject(DB, projectId);
  if (!p) return "";
  const k = (ensureVNextDB(DB).projectKnowledge[p.id] || []).slice(-8);
  const lines = [
    "프로젝트: "+p.name,
    "유형: "+p.type,
    "목표: "+(p.goal||"미설정"),
    "단계: "+p.stage,
    "핵심 KPI: "+p.primaryKpi,
    "보조 KPI: "+(p.secondaryKpis.join(", ")||"없음"),
    "대상: "+(p.audience||"미설정"),
    "채널: "+(p.channels.join(", ")||"미설정"),
    "AI 월 예산: "+p.aiBudgetMonthly,
    "유료 자동실행: "+(p.paidAutoExecution?"허용":"금지")
  ];
  if (k.length) lines.push("최근 프로젝트 학습:\n"+k.map(x=>"- ["+(x.confidence||1)+"/5] "+x.text).join("\n"));
  return "\n\n[현재 프로젝트 컨텍스트]\n"+lines.join("\n");
}

export function classifyCost({costClass="FREE"} = {}){
  const c = String(costClass||"FREE").toUpperCase();
  return ["FREE","LOW","HIGH"].includes(c) ? c : "HIGH";
}

export function costGate(DB, projectId, opts = {}){
  const p = getProject(DB, projectId);
  const costClass = classifyCost(opts);
  const estimatedCost = Math.max(0, Number(opts.estimatedCost || 0));
  if (costClass === "FREE" || estimatedCost <= 0) return {allowed:true, reason:"free"};
  if (!p) return {allowed:false, reason:"project_required"};
  const v = ensureVNextDB(DB);
  const month = new Date().toISOString().slice(0,7);
  const used = v.usageEvents.filter(e=>e.projectId===p.id && String(e.atMonth||"")===month).reduce((s,e)=>s+Number(e.actualCost||e.estimatedCost||0),0);
  const remaining = Math.max(0, p.aiBudgetMonthly-used);
  // Core principle: zero revenue + zero budget => no paid auto execution.
  if (p.revenueMonthly <= 0 && p.aiBudgetMonthly <= 0) return {allowed:false, reason:"zero_revenue_zero_budget", remaining};
  if (costClass === "HIGH") return {allowed:false, reason:"manual_approval_required", remaining};
  if (!p.paidAutoExecution) return {allowed:false, reason:"paid_auto_execution_off", remaining};
  if (estimatedCost > remaining) return {allowed:false, reason:"budget_exceeded", remaining};
  return {allowed:true, reason:"within_budget", remaining};
}

export function recordUsage(DB, event = {}){
  const v=ensureVNextDB(DB);
  const now=Date.now();
  const rec={
    id:"use_"+now+"_"+Math.random().toString(36).slice(2,6),
    projectId:String(event.projectId||""),
    taskType:String(event.taskType||"unknown"),
    model:String(event.model||""),
    costClass:classifyCost(event),
    tokensIn:Number(event.tokensIn||0),
    tokensOut:Number(event.tokensOut||0),
    estimatedCost:Number(event.estimatedCost||0),
    actualCost:Number(event.actualCost||0),
    at:now,
    atMonth:new Date(now).toISOString().slice(0,7)
  };
  v.usageEvents.push(rec);
  if(v.usageEvents.length>3000) v.usageEvents=v.usageEvents.slice(-3000);
  return rec;
}

function scoreIdea(s={}){
  const keys=["attention","value","makeability","potential"];
  const out={}; let total=0;
  keys.forEach(k=>{ out[k]=Math.max(1,Math.min(5,Number(s[k]||1))); total+=out[k]; });
  return {...out,total};
}

function duplicateKey(item={}){
  return String(item.url||item.product||item.topic||item.title||"").trim().toLowerCase().replace(/\s+/g," ").slice(0,300);
}

export function installVNextRoutes({app,getDB,saveDB}){
  if(!app || !getDB || !saveDB) throw new Error("app/getDB/saveDB required");
  const db=()=>getDB();
  const persist=()=>{ const r=saveDB(); return r; };

  app.get("/api/vnext/health",(req,res)=>{
    const DB=db(); const v=ensureVNextDB(DB);
    res.json({ok:true,version:VNEXT_VERSION,projects:v.projects.length,activeProjectId:v.activeProjectId});
  });

  app.get("/api/vnext/projects",(req,res)=>{
    const v=ensureVNextDB(db()); res.json({projects:v.projects,activeProjectId:v.activeProjectId});
  });

  app.post("/api/vnext/projects",async(req,res)=>{
    const DB=db(); const v=ensureVNextDB(DB); const p=normalizeProject(req.body||{});
    v.projects.push(p); if(!v.activeProjectId) v.activeProjectId=p.id; await persist(); res.json({ok:true,project:p});
  });

  app.patch("/api/vnext/projects/:id",async(req,res)=>{
    const DB=db(); const v=ensureVNextDB(DB); const i=v.projects.findIndex(p=>String(p.id)===String(req.params.id));
    if(i<0) return res.status(404).json({error:"project_not_found"});
    const merged=normalizeProject({...v.projects[i],...(req.body||{}),id:v.projects[i].id,createdAt:v.projects[i].createdAt});
    v.projects[i]=merged; await persist(); res.json({ok:true,project:merged});
  });

  app.post("/api/vnext/projects/:id/activate",async(req,res)=>{
    const DB=db(); const v=ensureVNextDB(DB); if(!v.projects.some(p=>String(p.id)===String(req.params.id))) return res.status(404).json({error:"project_not_found"});
    v.activeProjectId=String(req.params.id); await persist(); res.json({ok:true,activeProjectId:v.activeProjectId});
  });

  app.get("/api/vnext/context",(req,res)=> res.json({context:projectContext(db(),req.query.projectId)}));

  app.get("/api/vnext/radar",(req,res)=>{
    const v=ensureVNextDB(db()); const pid=String(req.query.projectId||v.activeProjectId||"");
    res.json(v.radarItems.filter(x=>!pid||x.projectId===pid).slice(-200));
  });

  app.post("/api/vnext/radar",async(req,res)=>{
    const DB=db(); const v=ensureVNextDB(DB); const projectId=String(req.body.projectId||v.activeProjectId||"");
    if(!projectId) return res.status(400).json({error:"projectId_required"});
    const key=duplicateKey(req.body);
    const dup=v.radarItems.find(x=>x.projectId===projectId && x.duplicateKey===key && key);
    if(dup) return res.json({ok:true,duplicate:true,item:dup});
    const item={id:"rad_"+Date.now(),projectId,title:String(req.body.title||req.body.topic||"새 후보"),url:String(req.body.url||""),source:String(req.body.source||"manual"),reason:String(req.body.reason||""),duplicateKey:key,status:"new",createdAt:Date.now()};
    v.radarItems.push(item); await persist(); res.json({ok:true,item});
  });

  app.post("/api/vnext/pick",async(req,res)=>{
    const DB=db(); const v=ensureVNextDB(DB); const rad=v.radarItems.find(x=>x.id===req.body.radarId);
    if(!rad) return res.status(404).json({error:"radar_not_found"});
    const score=scoreIdea(req.body.score||{});
    const idea={id:"idea_"+Date.now(),projectId:rad.projectId,radarId:rad.id,title:rad.title,pain:String(req.body.pain||""),audience:String(req.body.audience||""),promise:String(req.body.promise||""),format:String(req.body.format||""),hooks:Array.isArray(req.body.hooks)?req.body.hooks:[],score,status:req.body.status||"approved",createdAt:Date.now()};
    v.ideas.push(idea); rad.status="picked"; await persist(); res.json({ok:true,idea});
  });

  app.post("/api/vnext/experiments",async(req,res)=>{
    const DB=db(); const v=ensureVNextDB(DB); const idea=v.ideas.find(x=>x.id===req.body.ideaId);
    if(!idea) return res.status(404).json({error:"idea_not_found"});
    const e={id:"exp_"+Date.now(),projectId:idea.projectId,ideaId:idea.id,hypothesis:String(req.body.hypothesis||""),variable:String(req.body.variable||""),variants:Array.isArray(req.body.variants)?req.body.variants:[],successMetrics:Array.isArray(req.body.successMetrics)?req.body.successMetrics:[],status:"planned",createdAt:Date.now()};
    v.experiments.push(e); await persist(); res.json({ok:true,experiment:e});
  });

  app.post("/api/vnext/results",async(req,res)=>{
    const DB=db(); const v=ensureVNextDB(DB); const r={id:"res_"+Date.now(),projectId:String(req.body.projectId||v.activeProjectId||""),contentId:String(req.body.contentId||""),experimentId:String(req.body.experimentId||""),window:String(req.body.window||"manual"),metrics:req.body.metrics&&typeof req.body.metrics==="object"?req.body.metrics:{},createdAt:Date.now()};
    v.results.push(r); await persist(); res.json({ok:true,result:r});
  });

  app.post("/api/vnext/learn",async(req,res)=>{
    const DB=db(); const v=ensureVNextDB(DB); const projectId=String(req.body.projectId||v.activeProjectId||"");
    const verdict=String(req.body.verdict||"").toUpperCase(); if(!["KEEP","TEST","KILL"].includes(verdict)) return res.status(400).json({error:"verdict_must_be_KEEP_TEST_KILL"});
    const k={id:"know_"+Date.now(),projectId,type:String(req.body.type||"observation"),confidence:Math.max(1,Math.min(5,Number(req.body.confidence||1))),text:String(req.body.text||""),verdict,nextAction:String(req.body.nextAction||""),evidence:Array.isArray(req.body.evidence)?req.body.evidence:[],createdAt:Date.now()};
    v.projectKnowledge[projectId]=v.projectKnowledge[projectId]||[]; v.projectKnowledge[projectId].push(k); if(v.projectKnowledge[projectId].length>300) v.projectKnowledge[projectId]=v.projectKnowledge[projectId].slice(-300);
    await persist(); res.json({ok:true,knowledge:k});
  });

  app.get("/api/vnext/today",(req,res)=>{
    const DB=db(); const v=ensureVNextDB(DB); const projectId=String(req.query.projectId||v.activeProjectId||""); const p=getProject(DB,projectId);
    if(!p) return res.status(404).json({error:"project_not_found"});
    const picks=v.ideas.filter(x=>x.projectId===projectId&&x.status==="approved").slice(-3).reverse();
    const unreviewed=v.results.filter(x=>x.projectId===projectId).slice(-3).reverse();
    const actions=[];
    if(picks[0]) actions.push({kind:"MAKE",title:picks[0].title,refId:picks[0].id});
    if(unreviewed[0]) actions.push({kind:"LEARN",title:"최근 성과 검토",refId:unreviewed[0].id});
    if(actions.length<3) actions.push({kind:"RADAR",title:"신규 후보 탐색",refId:""});
    res.json({project:p,actions:actions.slice(0,3),cost:usageSummary(DB,projectId)});
  });

  app.get("/api/vnext/usage",(req,res)=>{
    res.json(usageSummary(db(),String(req.query.projectId||"")));
  });

  app.post("/api/vnext/cost/check",(req,res)=> res.json(costGate(db(),req.body.projectId,req.body)));
}

export function usageSummary(DB, projectId){
  const v=ensureVNextDB(DB); const pid=String(projectId||v.activeProjectId||""); const month=new Date().toISOString().slice(0,7);
  const events=v.usageEvents.filter(e=>(!pid||e.projectId===pid)&&e.atMonth===month);
  const estimated=events.reduce((s,e)=>s+Number(e.estimatedCost||0),0); const actual=events.reduce((s,e)=>s+Number(e.actualCost||0),0);
  const p=getProject(DB,pid);
  return {projectId:pid,month,calls:events.length,estimatedCost:+estimated.toFixed(6),actualCost:+actual.toFixed(6),budget:p?p.aiBudgetMonthly:0,remaining:p?Math.max(0,p.aiBudgetMonthly-actual):0};
}
