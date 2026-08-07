import assert from "node:assert/strict";
import { costGate, ensureVNextDB, installVNextRoutes, normalizeProject } from "./vnext-core.js";

const DB={};
const v=ensureVNextDB(DB);
const project=normalizeProject({id:"p1",name:"Lab",aiBudgetMonthly:0,paidAutoExecution:true,revenueMonthly:0});
v.projects.push(project); v.activeProjectId=project.id;
assert.equal(costGate(DB,"p1",{costClass:"LOW",estimatedCost:0.01}).allowed,false);
assert.equal(costGate(DB,"p1",{costClass:"FREE",estimatedCost:0}).allowed,true);

const routes=new Map(), app={
  get:(path,handler)=>routes.set("GET "+path,handler),
  post:(path,handler)=>routes.set("POST "+path,handler),
  patch:(path,handler)=>routes.set("PATCH "+path,handler)
};
let saves=0;
installVNextRoutes({app,getDB:()=>DB,saveDB:async()=>{saves++;}});
async function call(method,path,{body={},query={},params={}}={}){
  const handler=routes.get(method+" "+path); assert.ok(handler,method+" "+path);
  const out={status:200,data:null};
  const res={status(code){out.status=code;return this;},json(data){out.data=data;return this;}};
  await handler({body,query,params},res); return out;
}
assert.equal((await call("GET","/api/vnext/health")).data.ok,true);
let r=await call("POST","/api/vnext/radar",{body:{projectId:"p1",title:"전동채칼",url:"https://example.com/item"}});
assert.equal(r.data.ok,true);
r=await call("POST","/api/vnext/radar",{body:{projectId:"p1",title:"전동채칼",url:"https://example.com/item"}});
assert.equal(r.data.duplicate,true);
r=await call("GET","/api/vnext/today",{query:{projectId:"p1"}});
assert.equal(r.data.project.id,"p1");
assert.equal(r.data.cost.budget,0);
assert.equal(saves,1);
console.log("vNext integration test: PASS");
