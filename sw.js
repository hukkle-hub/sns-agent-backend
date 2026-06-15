const CACHE="agent-platform-v59";
const ASSETS=["./index.html","./manifest.json","./icon-192.png","./icon-512.png"];
self.addEventListener("install",e=>{e.waitUntil(caches.open(CACHE).then(c=>c.addAll(ASSETS)));self.skipWaiting()});
self.addEventListener("activate",e=>{e.waitUntil(caches.keys().then(ks=>Promise.all(ks.filter(k=>k!==CACHE).map(k=>caches.delete(k)))));self.clients.claim()});
// 네트워크 우선: 항상 최신 버전을 받고, 오프라인일 때만 캐시 사용
self.addEventListener("fetch",e=>{
  e.respondWith(
    fetch(e.request).then(r=>{
      const copy=r.clone();
      caches.open(CACHE).then(c=>c.put(e.request,copy)).catch(()=>{});
      return r;
    }).catch(()=>caches.match(e.request).then(r=>r||caches.match("./index.html")))
  );
});
self.addEventListener("push",e=>{let d={};try{d=e.data.json();}catch(_){}e.waitUntil(self.registration.showNotification(d.title||"SNS 에이전트",{body:d.body||"",icon:"./icon-192.png",badge:"./icon-192.png"}));});
self.addEventListener("notificationclick",e=>{e.notification.close();e.waitUntil(clients.matchAll({type:"window"}).then(ws=>{for(const w of ws){if("focus" in w)return w.focus();}return clients.openWindow("./index.html");}));});
