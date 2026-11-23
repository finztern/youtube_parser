"""Built-in web dashboard: samples Stats every 2 s, serves a live page (/) and JSON (/api). No extra deps."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque

log = logging.getLogger("dashboard")

KEYS = ("polls", "received", "fresh", "repeat", "known", "new", "sent", "req", "blocked", "http_err", "err", "bytes",
        "send_failed", "send_queue_drop", "queue_drop", "url_unverified", "url_check_fail", "node_quarantine")
FILTERED = ("repeat", "known", "parse_fail", "no_comments", "token_stale")


class Sampler:
    def __init__(self, stats, pool, hot, pipeline, sender, every: float = 2.0, keep: int = 450) -> None:
        self.s, self.pool, self.hot, self.pipe, self.sender, self.every = stats, pool, hot, pipeline, sender, every
        self.points: deque[dict] = deque(maxlen=keep)
        self._snap = {k: 0 for k in KEYS}
        self._t = self._t0 = time.monotonic()

    def sample(self) -> dict:
        now = time.monotonic()
        dt = max(now - self._t, 1e-6)
        cur = {k: self.s.c[k] for k in KEYS}
        d = {k: cur[k] - self._snap[k] for k in KEYS}
        pct = lambda a, b: round(100.0 * a / b, 1) if b else 0.0  # noqa: E731
        reqs = d["req"] + d["blocked"] + d["http_err"] + d["err"]
        p = {
            "t": int(time.time()),
            "new": round(d["new"] / dt, 2), "sent": round(d["sent"] / dt, 2), "polls": round(d["polls"] / dt, 2),
            "received": round(d["received"] / dt, 1), "fresh": round(d["fresh"] / dt, 2),
            "blocked": pct(d["blocked"] + d["err"], reqs), "known": pct(d["known"], d["known"] + d["new"]),
            "rep": pct(d["repeat"], d["repeat"] + d["fresh"]), "mbs": round(d["bytes"] / 1e6 / dt, 3),
            "eblocked": d["blocked"], "ehttp": d["http_err"], "eerr": d["err"],
            "esend": d["send_failed"], "euc": d["url_check_fail"],
            "checkq": self.pipe.q.qsize(), "sendq": self.sender.q.qsize(),
        }
        self._snap, self._t = cur, now
        self.points.append(p)
        return p

    def state(self) -> dict:
        c = self.s.c
        filtered = {k: c[k] for k in list(FILTERED) + [k for k in c if k.startswith("skip_")] if c[k]}
        gauges = {"check_q": self.pipe.q.qsize(), "send_q": self.sender.q.qsize(),
                  "nodes": f"{self.pool.live_count()}/{len(self.pool.proxies)}",
                  "hot": len(self.hot.hot), "videos": len(self.hot.videos),
                  "hot_int": round(self.hot.pacer.current), "seen": len(self.pipe.seen)}
        return {"uptime": int(time.monotonic() - self._t0), "points": list(self.points),
                "totals": {k: c[k] for k in KEYS}, "filtered": filtered, "gauges": gauges}

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self.every)
            self.sample()


async def serve(sampler: Sampler, host: str, port: int) -> None:
    async def handle(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        try:
            line = (await asyncio.wait_for(r.readline(), 5)).decode("latin1").split()
            while (await asyncio.wait_for(r.readline(), 5)) not in (b"\r\n", b"\n", b""):
                pass
            path = line[1].split("?")[0] if len(line) > 1 else "/"
            if path == "/api":
                body, ctype, code = json.dumps(sampler.state()).encode(), "application/json", "200 OK"
            elif path == "/":
                body, ctype, code = PAGE.encode(), "text/html; charset=utf-8", "200 OK"
            else:
                body, ctype, code = b"not found", "text/plain", "404 Not Found"
            w.write(f"HTTP/1.1 {code}\r\nContent-Type: {ctype}\r\nContent-Length: {len(body)}\r\n"
                    f"Cache-Control: no-store\r\nConnection: close\r\n\r\n".encode() + body)
            await w.drain()
        except Exception:  # noqa: BLE001
            pass
        finally:
            w.close()

    try:
        srv = await asyncio.start_server(handle, host, port)
    except OSError as e:
        log.warning("dashboard not started: %s", e)
        return
    log.info("dashboard: http://%s:%d", host, port)
    async with srv:
        await srv.serve_forever()


PAGE = r"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>youtube_parser</title><style>
:root{--bg:#0e1117;--card:#161b22;--bd:#262d38;--tx:#e6edf3;--mu:#8b949e;--g:#3fb950;--b:#58a6ff;--o:#f0883e;--r:#f85149;--p:#bc8cff;--y:#d29922}
@media(prefers-color-scheme:light){:root{--bg:#f6f8fa;--card:#fff;--bd:#d0d7de;--tx:#1f2328;--mu:#656d76}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--tx);font:14px system-ui,sans-serif;padding:16px;max-width:1400px;margin:auto}
header{display:flex;justify-content:space-between;align-items:center;margin-bottom:14px}h1{font-size:18px;margin:0}
#st{color:var(--mu);font-size:12px}#st i{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--g);margin-right:6px}
.grid{display:grid;gap:10px;grid-template-columns:repeat(8,1fr);margin-bottom:10px}
.card{background:var(--card);border:1px solid var(--bd);border-radius:10px;padding:10px 12px;min-width:0}
.card b{display:block;font-size:22px;font-variant-numeric:tabular-nums;margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.card span{color:var(--mu);font-size:12px;white-space:nowrap}
.charts{display:grid;gap:10px;grid-template-columns:repeat(3,1fr);margin-bottom:10px}
.two{display:grid;gap:10px;grid-template-columns:repeat(auto-fit,minmax(420px,1fr))}
@media(max-width:1000px){.grid{grid-template-columns:repeat(4,1fr)}.charts{grid-template-columns:repeat(2,1fr)}}
@media(max-width:600px){.grid{grid-template-columns:repeat(2,1fr)}.charts,.two{grid-template-columns:1fr}}
.ch h3{margin:0 0 6px;font-size:13px;font-weight:600}.lg{font-size:11px;color:var(--mu)}.lg u{text-decoration:none;margin-right:10px}
canvas{width:100%;height:150px;display:block}
table{width:100%;border-collapse:collapse;font-size:13px}td{padding:3px 0;border-bottom:1px solid var(--bd)}td:last-child{text-align:right;font-variant-numeric:tabular-nums}
.bad{color:var(--r)}.ok{color:var(--g)}.warn{color:var(--y)}
</style></head><body>
<header><h1>youtube_parser</h1><div id="st"><i></i><span id="up">connecting…</span></div></header>
<div class="grid" id="cards"></div>
<div class="charts" id="charts"></div>
<div class="two"><div class="card"><h3 style="margin:0 0 6px;font-size:13px">Filtered (total)</h3><table id="flt"></table></div>
<div class="card"><h3 style="margin:0 0 6px;font-size:13px">Totals</h3><table id="tot"></table></div></div>
<script>
const C={g:'#3fb950',b:'#58a6ff',o:'#f0883e',r:'#f85149',p:'#bc8cff',y:'#d29922'};
const CH=[
 {t:'new/s vs sent/s',s:[['new','new/s',C.g],['sent','sent/s',C.b]]},
 {t:'polls/s · received/s · fresh/s',s:[['polls','polls/s',C.o],['received','received/s',C.p],['fresh','fresh/s',C.g]]},
 {t:'blocked % · url-check known % · repeats %',s:[['blocked','blocked%',C.r],['known','known%',C.b],['rep','rep%',C.p]],max:100},
 {t:'errors per 2 s',s:[['eblocked','blocked',C.r],['ehttp','http',C.o],['eerr','err',C.y],['esend','send',C.p],['euc','url-check',C.b]]},
 {t:'backlog: check_q · send_q (items)',s:[['checkq','check_q',C.o],['sendq','send_q',C.g]]},
 {t:'traffic MB/s',s:[['mbs','MB/s',C.g]]}];
const $=id=>document.getElementById(id);
$('charts').innerHTML=CH.map((c,i)=>`<div class="card ch"><h3>${c.t}</h3><canvas id="c${i}"></canvas><div class="lg">${c.s.map(x=>`<u style="color:${x[2]}">● ${x[1]}</u>`).join('')}</div></div>`).join('');
function draw(cv,pts,c){const dpr=devicePixelRatio||1,w=cv.clientWidth,h=cv.clientHeight;cv.width=w*dpr;cv.height=h*dpr;
 const x=cv.getContext('2d');x.scale(dpr,dpr);x.clearRect(0,0,w,h);const st=getComputedStyle(document.body);
 let mx=c.max||Math.max(1e-9,...pts.flatMap(p=>c.s.map(s=>p[s[0]]||0)));mx*=1.1;const L=34,B=14;
 x.font='10px system-ui';x.fillStyle=st.getPropertyValue('--mu');x.strokeStyle=st.getPropertyValue('--bd');
 for(let i=0;i<=3;i++){const y=4+(h-B-4)*i/3;x.beginPath();x.moveTo(L,y);x.lineTo(w,y);x.stroke();x.fillText((mx*(1-i/3)).toFixed(mx<10?1:0),0,y+3)}
 if(pts.length<2)return;const n=pts.length;
 c.s.forEach(s=>{x.beginPath();x.strokeStyle=s[2];x.lineWidth=1.6;pts.forEach((p,i)=>{const X=L+(w-L)*i/(n-1),Y=4+(h-B-4)*(1-(p[s[0]]||0)/mx);i?x.lineTo(X,Y):x.moveTo(X,Y)});x.stroke()});
 const t0=new Date(pts[0].t*1000),t1=new Date(pts[n-1].t*1000);x.fillStyle=st.getPropertyValue('--mu');
 x.fillText(t0.toLocaleTimeString(),L,h-2);x.textAlign='right';x.fillText(t1.toLocaleTimeString(),w,h-2);x.textAlign='left'}
const avg=(a,k)=>a.length?a.reduce((s,p)=>s+(p[k]||0),0)/a.length:0;
const fmt=n=>n>=1e6?(n/1e6).toFixed(2)+'M':n>=1e3?(n/1e3).toFixed(1)+'k':String(n);
const dur=s=>`${Math.floor(s/3600)}h ${Math.floor(s%3600/60)}m ${s%60}s`;
async function tick(){try{const d=await (await fetch('/api')).json();const P=d.points,L=P.slice(-15),G=d.gauges;
 $('up').textContent='live · uptime '+dur(d.uptime);
 const g=k=>G[k]??'—',newS=avg(L,'new'),blk=avg(L,'blocked'),e=(k)=>L.reduce((s,p)=>s+(p[k]||0),0);
 const eb=e('esend')+e('euc');
 const cards=[
 ['new/s',newS.toFixed(1),newS>=10?'ok':'warn'],['sent/s',avg(L,'sent').toFixed(1),''],['polls/s',avg(L,'polls').toFixed(1),''],['received/s',avg(L,'received').toFixed(0),''],
 ['fresh/s',avg(L,'fresh').toFixed(1),''],['blocked %',blk.toFixed(0),blk>40?'bad':blk>20?'warn':'ok'],['known % (url-check)',avg(L,'known').toFixed(0),''],['repeats %',avg(L,'rep').toFixed(0),''],
 ['send / url-check err (30 s)',e('esend')+' / '+e('euc'),eb?'bad':'ok'],['check_q',g('check_q'),''],['send_q',g('send_q'),''],['nodes live/total',g('nodes'),''],
 ['hot / videos',g('hot')+' / '+g('videos'),''],['hot interval, s',g('hot_int'),''],['seen cache',g('seen'),''],['MB / min',(avg(L,'mbs')*60).toFixed(1),'']];
 $('cards').innerHTML=cards.map(c=>`<div class="card"><span>${c[0]}</span><b class="${c[2]}">${c[1]}</b></div>`).join('');
 CH.forEach((c,i)=>draw($('c'+i),P,c));
 const f=Object.entries(d.filtered).sort((a,b)=>b[1]-a[1]);
 $('flt').innerHTML=f.length?f.map(([k,v])=>`<tr><td>${k}</td><td>${fmt(v)}</td></tr>`).join(''):'<tr><td>—</td><td></td></tr>';
 $('tot').innerHTML=Object.entries(d.totals).map(([k,v])=>`<tr><td>${k}</td><td>${fmt(v)}</td></tr>`).join('');
 }catch(e){$('up').textContent='disconnected'}}
tick();setInterval(tick,2000);addEventListener('resize',tick);
</script></body></html>"""
