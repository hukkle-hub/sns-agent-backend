// ===== Service Worker — 항상 최신 유지(캐시 갇힘 근본 차단) =====
const CACHE = "agent-platform-v268";
const ASSETS = ["./manifest.json","./icon-192.png","./icon-512.png"]; // HTML/sw는 캐시 목록에서 제외(항상 새로 받음)

self.addEventListener("install", e => {
  // 새 SW를 즉시 대기 없이 활성화
  self.skipWaiting();
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(ASSETS)).catch(()=>{}));
});

self.addEventListener("activate", e => {
  e.waitUntil((async () => {
    // 옛 캐시 전부 삭제
    const ks = await caches.keys();
    await Promise.all(ks.filter(k => k !== CACHE).map(k => caches.delete(k)));
    // 모든 열린 탭을 이 SW가 즉시 제어
    await self.clients.claim();
    // 열려있는 모든 페이지에 '새 버전 활성화됨' 알림 → 자동 새로고침 유도
    const clientsArr = await self.clients.matchAll({ type: "window" });
    for (const c of clientsArr) { try { c.postMessage({ type: "SW_UPDATED", cache: CACHE }); } catch(_){} }
  })());
});

// HTML 문서 요청은 '네트워크 우선', 절대 오래된 캐시로 대체하지 않음(오프라인만 예외)
function isDoc(req){
  return req.mode === "navigate" || (req.destination === "document") ||
    (req.headers.get("accept")||"").includes("text/html");
}

self.addEventListener("fetch", e => {
  const req = e.request;
  if (req.method !== "GET") return;
  const url = new URL(req.url);

  // sw.js 자체와 HTML은 항상 네트워크에서 새로(캐시 무효화). 캐시 갇힘의 근본 차단.
  if (isDoc(req) || url.pathname.endsWith("/sw.js") || url.pathname.endsWith("index.html")) {
    e.respondWith(
      fetch(req, { cache: "no-store" })
        .then(r => {
          // 성공 시 백업용으로만 저장(오프라인 대비), 하지만 우선은 항상 네트워크
          const copy = r.clone();
          caches.open(CACHE).then(c => c.put(req, copy)).catch(()=>{});
          return r;
        })
        .catch(() => caches.match(req).then(r => r || caches.match("./index.html")))
    );
    return;
  }

  // 그 외 정적 자원(아이콘 등)은 네트워크 우선 + 캐시 백업
  e.respondWith(
    fetch(req).then(r => {
      const copy = r.clone();
      caches.open(CACHE).then(c => c.put(req, copy)).catch(()=>{});
      return r;
    }).catch(() => caches.match(req))
  );
});

self.addEventListener("push", e => {
  let d = {}; try { d = e.data.json(); } catch(_){}
  e.waitUntil(self.registration.showNotification(d.title || "SNS 에이전트", {
    body: d.body || "", icon: "./icon-192.png", badge: "./icon-192.png"
  }));
});
self.addEventListener("notificationclick", e => {
  e.notification.close();
  e.waitUntil(clients.matchAll({ type: "window" }).then(ws => {
    for (const w of ws) { if ("focus" in w) return w.focus(); }
    return clients.openWindow("./index.html");
  }));
});
