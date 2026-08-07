// === Hukkle Hub vNext integration snippet ===
// 1) server.js import section에 추가:
import { ensureVNextDB, installVNextRoutes, projectContext, costGate, recordUsage } from "./vnext-core.js";

// 2) DB 로딩이 끝난 뒤(그리고 app route 등록 전에) 1회:
ensureVNextDB(DB);
installVNextRoutes({
  app,
  getDB: () => DB,
  saveDB: () => saveDB()
});

// 3) 기존 profileContext() 바로 다음에 아래 helper 추가 권장:
function fullContext(projectId){
  return profileContext() + projectContext(DB, projectId);
}

// 4) work()를 다음 형태로 확장 권장:
// async function work(dept, instruction, context, images, teamLog, projectId){
//   ...
//   sys += fullContext(projectId);
// }
// 기존 호출부는 projectId가 없어도 동작하므로 마지막 인자로 추가하면 하위 호환 가능.

// 5) 유료 AI 호출 직전 예시:
// const gate = costGate(DB, projectId, { costClass:"LOW", estimatedCost:0.003 });
// if (!gate.allowed) throw new Error("COST_BLOCKED:"+gate.reason);
// 호출 완료 뒤 recordUsage(DB, {projectId,taskType:"strategy",model:MODEL,costClass:"LOW",actualCost:0.0027});
