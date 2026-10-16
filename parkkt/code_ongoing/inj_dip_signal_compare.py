import sys; sys.path.insert(0,__import__('os').path.dirname(__file__))
import inj_dip_signal_base as A
from inj_dip_signal_base import load, segs, auc
import numpy as np, pandas as pd
A.MIN_TICK=1; PRE=2; POST=2
col=sys.argv[1] if len(sys.argv)>1 else 'Scale_Out___PT_P1'
def sigdefs(df):
    S1=df.Shot_Injection_HD1_P1.values!=0; S2=df.Shot_Injection_On_HD1.values!=0
    F={k:df[f'HD1_FOAMING_SHOT{k}'].values!=0 for k in range(1,5)}
    d={'S1 HD1_P1':[S1],'S2 On_HD1':[S2],'S3 FOAMING1~4':[F[k] for k in F]}
    d['S4 On_HD1∧SHOTk']=[S2&F[k] for k in F]
    for k in F: d[f'  S3 SHOT{k}']=[F[k]]
    for k in F: d[f'  S4 On∧SHOT{k}']=[S2&F[k]]
    return d,S2
res={}
for name in ['fri','today']:
    df=load(name); v=df[col].values.astype(float); bu=df.bu.values; cyc=df.cyc.values; tick=df.tick.values
    sd,S2=sigdefs(df); n=len(v); cs=np.r_[0,np.cumsum(S2)]
    # 틱 단위 하락: 직전 2틱 평균 대비
    base=np.full(n,np.nan); base[2:]=(v[:-2]+v[1:-1])/2
    d_t=(base-v)/base*100
    ok=np.zeros(n,bool)
    for t in range(2,n): ok[t]=bu[t-2:t+1].all() and cyc[t-2]==cyc[t] and tick[t-2]>=A.MIN_TICK
    quiet=np.array([ok[t] and cs[min(t+POST+1,n)]-cs[max(t-PRE-2,0)]==0 for t in range(n)])  # [t-4,t+2]에 S2 없음
    # 이벤트 null (응답길이 L)
    null={}
    for L in range(1,13):
        out=[]
        for s in range(PRE,n-L-POST):
            if cs[s+L+POST]-cs[s-PRE]>0: continue
            r=A.event_drop(v,bu,cyc,tick,s,s+L-1)
            if r and r[2]==L+POST: out.append(r[1])
        null[L+POST]=np.array(out)
    ev={}
    for nm,masks in sd.items():
        rows=[]; resp=np.zeros(n,bool)
        for m in masks:
            for s,e in segs(m):
                r=A.event_drop(v,bu,cyc,tick,s,e)
                if r: rows.append(r)
                resp[s:min(e+POST,n-1)+1]=True
        ev[nm]=(np.array(rows),resp&ok)
    res[name]=dict(d_t=d_t,ok=ok,quiet=quiet,null=null,ev=ev)
# null 합치기 (두 날)
qd=np.concatenate([r['d_t'][r['quiet']] for r in res.values()])
print(f'##### {col}')
print(f'[틱단위 null] 앞뒤로 S2 없는 안정 틱 n={len(qd)} (fri {res["fri"]["quiet"].sum()}, today {res["today"]["quiet"].sum()})')
for p in [50,75,90,95,99]: print(f'   P{p}: {np.percentile(qd,p):.2f}%')
thr=np.percentile(qd,95); thr99=np.percentile(qd,99)
allok_d={k:r['d_t'][r['ok']] for k,r in res.items()}
print(f'   (참고) 전체 안정틱 P95: fri {np.percentile(allok_d["fri"],95):.2f}%  today {np.percentile(allok_d["today"],95):.2f}%')
nullL={L:np.concatenate([r['null'].get(L,np.array([])) for r in res.values()]) for L in range(3,15)}
print('[이벤트 null] 앞2틱 평균 대비 (응답L틱 최저) 하락%, 창 전체에 S2 없음:')
for L,x in nullL.items():
    if len(x)>=20: print(f'   응답L={L:2d} n={len(x):4d} P50={np.median(x):.2f} P90={np.percentile(x,90):.2f} P95={np.percentile(x,95):.2f} P99={np.percentile(x,99):.2f}')
usable=[L for L,x in nullL.items() if len(x)>=50]; Lmax=max(usable)
def thrL(L): return np.percentile(nullL[min(max(L,min(usable)),Lmax)],95)
for name,r in res.items():
    print(f'\n=== {name}  (유의 하락 틱 기준 d_t>={thr:.2f}%  / 강한 하락 >={thr99:.2f}%)')
    sig_tick=r['ok']&(r['d_t']>=thr); big=r['ok']&(r['d_t']>=thr99)
    print(f'   안정틱 {r["ok"].sum()}개 중 유의하락 {sig_tick.sum()} ({sig_tick.sum()/r["ok"].sum()*100:.1f}%), 강한하락 {big.sum()}')
    print(f"   {'신호':18s} {'평가이벤트':>8s} {'하락중앙':>7s} {'P75':>6s} {'P90':>6s} {'>null95':>8s} {'AUC':>5s} | {'시간점유':>6s} {'포착률':>6s} {'lift':>5s} {'강한포착':>7s}")
    for nm,(a,resp) in r['ev'].items():
        if len(a)==0: continue
        dd=a[:,1]; L=a[:,2].astype(int)
        hit=np.mean([x>=thrL(l) for x,l in zip(dd,L)])*100
        Ls=[l for l in np.unique(L)]
        au=sum(auc(dd[L==l],nullL[min(max(l,min(usable)),Lmax)])*(L==l).sum() for l in Ls)/len(L)
        cov=resp.sum()/r['ok'].sum(); rec=(resp&sig_tick).sum()/sig_tick.sum(); recb=(resp&big).sum()/max(big.sum(),1)
        print(f"   {nm:18s} {len(a):8d} {np.median(dd):7.2f} {np.percentile(dd,75):6.2f} {np.percentile(dd,90):6.2f} {hit:7.1f}% {au:5.2f} | {cov*100:5.1f}% {rec*100:5.1f}% {rec/cov:5.2f} {recb*100:6.1f}%")
import pickle; pickle.dump({'qd':qd,'nullL':nullL,'res':{k:{'ev':{n:a for n,(a,_) in r['ev'].items()}} for k,r in res.items()}},open(rf'parkkt/cache/inj_dip_res_{col}.pkl','wb'))
