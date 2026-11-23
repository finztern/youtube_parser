"""Built-in web dashboard: samples Stats every 2 s, serves a live page (/), JSON (/api), Prometheus (/metrics). No extra deps."""
from __future__ import annotations

import asyncio
import json
import logging
import re
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

    def metrics(self) -> str:
        """Prometheus text exposition: every counter as ytp_<key>_total plus gauges."""
        c, out = self.s.c, []
        for k in sorted(set(KEYS) | set(FILTERED) | set(c)):
            n = "ytp_" + re.sub(r"\W", "_", k) + "_total"
            out += [f"# TYPE {n} counter", f"{n} {c[k]}"]
        g = {"check_q": self.pipe.q.qsize(), "send_q": self.sender.q.qsize(), "nodes_live": self.pool.live_count(),
             "nodes_total": len(self.pool.proxies), "hot": len(self.hot.hot), "videos": len(self.hot.videos),
             "hot_interval_seconds": round(self.hot.pacer.current, 1), "seen": len(self.pipe.seen),
             "uptime_seconds": int(time.monotonic() - self._t0)}
        for k, v in g.items():
            out += [f"# TYPE ytp_{k} gauge", f"ytp_{k} {v}"]
        return "\n".join(out) + "\n"

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
            elif path == "/metrics":
                body, ctype, code = sampler.metrics().encode(), "text/plain; version=0.0.4", "200 OK"
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
header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;gap:10px;flex-wrap:wrap}h1{font-size:18px;margin:0}
#st{color:var(--mu);font-size:12px}#st i{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--g);margin-right:6px}
.win button{background:var(--card);color:var(--mu);border:1px solid var(--bd);padding:3px 10px;border-radius:6px;cursor:pointer;font:inherit;font-size:12px;margin-left:4px}
.win button.on{color:var(--tx);border-color:var(--b)}
h2{font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:var(--mu);margin:16px 0 6px;font-weight:600}
.lamps{display:flex;gap:8px;flex-wrap:wrap}.lamp{background:var(--card);border:1px solid var(--bd);border-radius:20px;padding:4px 12px;font-size:12px;display:flex;align-items:center;gap:6px}
.lamp i{width:9px;height:9px;border-radius:50%;background:var(--g)}.lamp.warn i{background:var(--y)}.lamp.bad i{background:var(--r)}.lamp span{color:var(--mu)}
.cards{display:grid;gap:10px;grid-template-columns:repeat(auto-fill,minmax(150px,1fr))}
.card{background:var(--card);border:1px solid var(--bd);border-radius:10px;padding:10px 12px;min-width:0}
.card b{display:block;font-size:22px;font-variant-numeric:tabular-nums;margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.card span{color:var(--mu);font-size:12px;white-space:nowrap}
.sp{width:100%;height:26px;display:block;margin-top:4px}
.charts{display:grid;gap:10px;grid-template-columns:repeat(3,1fr)}
.two{display:grid;gap:10px;grid-template-columns:repeat(auto-fit,minmax(420px,1fr))}
@media(max-width:1000px){.charts{grid-template-columns:repeat(2,1fr)}}
@media(max-width:600px){.charts,.two{grid-template-columns:1fr}}
.ch h3,.card h3{margin:0 0 6px;font-size:13px;font-weight:600}.lg{font-size:11px;color:var(--mu)}.lg u{text-decoration:none;margin-right:10px}
canvas.big{width:100%;height:150px;display:block}
.bar{display:grid;grid-template-columns:150px 1fr 90px;gap:8px;align-items:center;font-size:13px;padding:3px 0}
.bar .t{height:14px;background:var(--bd);border-radius:4px;overflow:hidden}.bar .t div{height:100%;border-radius:4px}
.bar .n{text-align:right;font-variant-numeric:tabular-nums}.bar .k{color:var(--tx);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.loss{font-size:11px;color:var(--mu);padding:0 0 2px 158px}
</style></head><body>
<header><h1>youtube_parser</h1><div><span id="st"><i></i><span id="up">connecting…</span></span>
<span class="win" id="win"><button data-w="30">1m</button><button data-w="150" class="on">5m</button><button data-w="450">15m</button></span></div></header>
<div class="lamps" id="lamps"></div>
<h2>Flow</h2><div class="cards" id="c_flow"></div>
<h2>Quality</h2><div class="cards" id="c_q"></div>
<h2>Queues &amp; nodes</h2><div class="cards" id="c_n"></div>
<h2>Pipeline funnel (since start)</h2><div class="card" id="funnel"></div>
<h2>Charts</h2><div class="charts" id="charts"></div>
<h2>Filtered &amp; totals</h2>
<div class="two"><div class="card"><h3>Filtered / skip reasons</h3><div id="flt"></div></div>
<div class="card"><h3>Totals</h3><div id="tot"></div></div></div>
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
let D=null,win=150,hov=null;
$('charts').innerHTML=CH.map((c,i)=>`<div class="card ch"><h3>${c.t}</h3><canvas class="big" id="c${i}"></canvas><div class="lg">${c.s.map(x=>`<u style="color:${x[2]}">● ${x[1]}</u>`).join('')}</div></div>`).join('');
const css=k=>getComputedStyle(document.body).getPropertyValue(k);
const pf=v=>v>=100?v.toFixed(0):v>=10?v.toFixed(1):v.toFixed(2);
function draw(cv,P,c){const dpr=devicePixelRatio||1,w=cv.clientWidth,h=cv.clientHeight;cv.width=w*dpr;cv.height=h*dpr;
 const x=cv.getContext('2d');x.scale(dpr,dpr);x.clearRect(0,0,w,h);
 let mx=c.max||Math.max(1e-9,...P.flatMap(p=>c.s.map(s=>p[s[0]]||0)));mx*=1.1;const L=34,B=14,n=P.length;
 x.font='10px system-ui';x.fillStyle=css('--mu');x.strokeStyle=css('--bd');
 for(let i=0;i<=3;i++){const y=4+(h-B-4)*i/3;x.beginPath();x.moveTo(L,y);x.lineTo(w,y);x.stroke();x.fillText((mx*(1-i/3)).toFixed(mx<10?1:0),0,y+3)}
 if(n<2)return;
 const X=i=>L+(w-L)*i/(n-1),Y=v=>4+(h-B-4)*(1-(v||0)/mx);
 c.s.forEach(s=>{x.beginPath();P.forEach((p,i)=>i?x.lineTo(X(i),Y(p[s[0]])):x.moveTo(X(i),Y(p[s[0]])));
  x.strokeStyle=s[2];x.lineWidth=1.6;x.stroke();
  x.lineTo(X(n-1),h-B);x.lineTo(X(0),h-B);x.closePath();x.globalAlpha=.1;x.fillStyle=s[2];x.fill();x.globalAlpha=1});
 x.fillStyle=css('--mu');x.textAlign='left';x.fillText(new Date(P[0].t*1000).toLocaleTimeString(),L,h-2);
 x.textAlign='right';x.fillText(new Date(P[n-1].t*1000).toLocaleTimeString(),w,h-2);x.textAlign='left';
 if(hov!==null){const i=Math.round(hov*(n-1)),p=P[i],px=X(i);
  x.strokeStyle=css('--mu');x.setLineDash([3,3]);x.beginPath();x.moveTo(px,4);x.lineTo(px,h-B);x.stroke();x.setLineDash([]);
  const rows=[new Date(p.t*1000).toLocaleTimeString(),...c.s.map(s=>s[1]+' '+pf(p[s[0]]||0))],bw=Math.max(...rows.map(r=>x.measureText(r).width))+14,bh=rows.length*13+6;
  const bx=px>w/2?px-bw-8:px+8;x.fillStyle=css('--card');x.strokeStyle=css('--bd');x.fillRect(bx,6,bw,bh);x.strokeRect(bx,6,bw,bh);
  rows.forEach((r,j)=>{x.fillStyle=j?c.s[j-1][2]:css('--mu');x.fillText(r,bx+7,19+j*13)});
  c.s.forEach(s=>{x.beginPath();x.arc(px,Y(p[s[0]]),3,0,7);x.fillStyle=s[2];x.fill()})}}
function spark(cv,P,k,col){const dpr=devicePixelRatio||1,w=cv.clientWidth,h=cv.clientHeight;cv.width=w*dpr;cv.height=h*dpr;
 const x=cv.getContext('2d');x.scale(dpr,dpr);const n=P.length;if(n<2)return;const mx=Math.max(1e-9,...P.map(p=>p[k]||0));
 const X=i=>w*i/(n-1),Y=v=>h-2-(h-4)*(v||0)/mx;x.beginPath();P.forEach((p,i)=>i?x.lineTo(X(i),Y(p[k])):x.moveTo(X(i),Y(p[k])));
 x.strokeStyle=col;x.lineWidth=1.3;x.stroke();x.lineTo(w,h);x.lineTo(0,h);x.closePath();x.globalAlpha=.15;x.fillStyle=col;x.fill()}
const avg=(a,k)=>a.length?a.reduce((s,p)=>s+(p[k]||0),0)/a.length:0;
const sum=(a,k)=>a.reduce((s,p)=>s+(p[k]||0),0);
const fmt=n=>n>=1e6?(n/1e6).toFixed(2)+'M':n>=1e3?(n/1e3).toFixed(1)+'k':String(n);
const dur=s=>`${Math.floor(s/3600)}h ${Math.floor(s%3600/60)}m ${s%60}s`;
const colOf={ok:C.g,warn:C.y,bad:C.r,'':C.b};
function cardsHtml(a){return a.map(c=>`<div class="card"><span>${c[0]}</span><b class="${c[2]}" style="${c[2]?`color:${colOf[c[2]]}`:''}">${c[1]}</b>${c[3]?`<canvas class="sp" data-k="${c[3]}" data-c="${colOf[c[2]]}"></canvas>`:''}</div>`).join('')}
function funnel(T,F){
 const skip=Object.entries(F).filter(([k])=>k.startsWith('skip_')).reduce((s,[,v])=>s+v,0),rcv=T.received||0;
 const st=[['received',rcv,C.p,''],['passed filters',rcv-skip,C.b,`−${fmt(skip)} skipped (skip_*)`],
  ['fresh (not seen)',T.fresh||0,C.b,`−${fmt(T.repeat||0)} repeats`],
  ['NEW (url-check)',(T.new||0),C.g,`−${fmt(T.known||0)} known · ${fmt(T.url_unverified||0)} unverified · ${fmt(T.queue_drop||0)} queue drop`],
  ['sent',T.sent||0,C.g,`${fmt(T.send_failed||0)} send_failed · ${fmt(T.send_queue_drop||0)} send_q drop`]];
 $('funnel').innerHTML=st.map(([k,v,c,l])=>`<div class="bar"><span class="k">${k}</span><div class="t"><div style="width:${rcv?Math.max(.5,100*v/rcv):0}%;background:${c}"></div></div><span class="n">${fmt(v)} · ${rcv?(100*v/rcv).toFixed(1):0}%</span></div>${l?`<div class="loss">${l}</div>`:''}`).join('')}
function bars(el,obj,col){const e=Object.entries(obj).sort((a,b)=>b[1]-a[1]),mx=e.length?e[0][1]:1;
 $(el).innerHTML=e.length?e.map(([k,v])=>`<div class="bar" style="grid-template-columns:130px 1fr 70px"><span class="k">${k}</span><div class="t"><div style="width:${100*v/mx}%;background:${col}"></div></div><span class="n">${fmt(v)}</span></div>`).join(''):'<div class="lg">—</div>'}
function lamp(n,cls,txt){return `<div class="lamp ${cls}"><i></i>${n} <span>${txt}</span></div>`}
function render(){if(!D)return;const P=D.points.slice(-win),L=D.points.slice(-15),G=D.gauges,T=D.totals;
 $('up').textContent='live · uptime '+dur(D.uptime);
 const [lv,tt]=String(G.nodes).split('/').map(Number),nr=tt?lv/tt:0,blk=avg(L,'blocked'),newS=avg(L,'new');
 const q0=avg(P.slice(0,15),'checkq'),q1=avg(L,'checkq'),qg=q1>2*q0+100,es=sum(P,'esend'),eu=sum(L,'euc');
 $('lamps').innerHTML=lamp('nodes',nr<.2?'bad':nr<.5?'warn':'',G.nodes)+lamp('blocked',blk>40?'bad':blk>20?'warn':'',blk.toFixed(0)+'%')+
  lamp('check_q',qg?'warn':'',qg?'growing '+q1.toFixed(0):'stable '+q1.toFixed(0))+lamp('send_failed',es?'bad':'',es?es+' in window':'0')+
  lamp('url-check',eu?'warn':'',eu?eu+' errors / 30 s':'ok')+lamp('new/s',newS<=0?'bad':'',newS.toFixed(1));
 const fl=[['new/s',newS.toFixed(1),newS>=10?'ok':'warn','new'],['sent/s',avg(L,'sent').toFixed(1),'','sent'],['polls/s',avg(L,'polls').toFixed(1),'','polls'],
  ['received/s',avg(L,'received').toFixed(0),'','received'],['fresh/s',avg(L,'fresh').toFixed(1),'','fresh'],['MB / min',(avg(L,'mbs')*60).toFixed(1),'','mbs']];
 const q=[['blocked %',blk.toFixed(0),blk>40?'bad':blk>20?'warn':'ok','blocked'],['known % (url-check)',avg(L,'known').toFixed(0),'','known'],['repeats %',avg(L,'rep').toFixed(0),'','rep'],
  ['send / url-check err (30 s)',sum(L,'esend')+' / '+eu,(sum(L,'esend')+eu)?'bad':'ok','']];
 const n=[['check_q',G.check_q??'—','','checkq'],['send_q',G.send_q??'—','','sendq'],['nodes live/total',G.nodes??'—',nr<.2?'bad':nr<.5?'warn':'ok',''],
  ['hot / videos',G.hot+' / '+G.videos,'',''],['hot interval, s',G.hot_int,'',''],['seen cache',G.seen,'','']];
 $('c_flow').innerHTML=cardsHtml(fl);$('c_q').innerHTML=cardsHtml(q);$('c_n').innerHTML=cardsHtml(n);
 document.querySelectorAll('canvas.sp').forEach(cv=>spark(cv,P,cv.dataset.k,cv.dataset.c));
 funnel(T,D.filtered);CH.forEach((c,i)=>draw($('c'+i),P,c));
 const f=D.filtered,sk=Object.fromEntries(Object.entries(f));bars('flt',sk,C.o);bars('tot',T,C.b)}
async function tick(){try{D=await (await fetch('/api')).json();render()}catch(e){$('up').textContent='disconnected'}}
document.querySelectorAll('#win button').forEach(b=>b.onclick=()=>{win=+b.dataset.w;document.querySelectorAll('#win button').forEach(x=>x.classList.toggle('on',x===b));render()});
CH.forEach((c,i)=>{const cv=$('c'+i);cv.onmousemove=e=>{const r=cv.getBoundingClientRect();hov=Math.min(1,Math.max(0,(e.clientX-r.left-34)/(r.width-34)));render()};cv.onmouseleave=()=>{hov=null;render()}});
tick();setInterval(tick,2000);addEventListener('resize',render);
</script></body></html>"""
