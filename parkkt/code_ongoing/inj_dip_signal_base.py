import pandas as pd, numpy as np
PRE=2; POST=2; MIN_TICK=2
def load(name):
    df=pd.read_pickle(f'parkkt/cache/inj_dip_p1_{name}.pkl').drop(columns='source',errors='ignore').ffill().fillna(0).reset_index(drop=True)
    bu=(df.Pump_BuildUp_P1!=0).values
    cyc=np.cumsum(np.r_[True, bu[1:]!=bu[:-1]])
    df['cyc']=cyc; df['bu']=bu
    df['tick']=df.groupby('cyc').cumcount()
    return df
def segs(mask):
    m=np.asarray(mask,bool); d=np.diff(np.r_[0,m.astype(int),0])
    return list(zip(np.where(d==1)[0], np.where(d==-1)[0]-1))
def event_drop(v,bu,cyc,tick,s,e):
    """baseline = 앞 PRE틱 평균, 응답 = [s, e+POST] 중 BU=1 이고 같은 사이클인 틱의 최저값"""
    b0=s-PRE
    if b0<0 or not bu[b0:s].all() or cyc[b0]!=cyc[s] or tick[b0]<MIN_TICK: return None
    r=np.arange(s, min(e+POST, len(v)-1)+1); r=r[bu[r]&(cyc[r]==cyc[s])]
    base=v[b0:s].mean(); mn=v[r].min()
    return base, (base-mn)/base*100, len(r)
def run(df, col):
    v=df[col].values.astype(float); bu=df.bu.values; cyc=df.cyc.values; tick=df.tick.values
    S1=df.Shot_Injection_HD1_P1.values!=0; S2=df.Shot_Injection_On_HD1.values!=0
    F={k:df[f'HD1_FOAMING_SHOT{k}'].values!=0 for k in range(1,5)}
    sigs={'S1 Shot_Injection_HD1_P1':S1,'S2 Shot_Injection_On_HD1':S2}
    for k in F: sigs[f'S3 FOAMING_SHOT{k}']=F[k]
    for k in F: sigs[f'S4 On_HD1 & SHOT{k}']=S2&F[k]
    # S3 전체(번호별 각각 1이벤트) 는 아래서 합쳐서 계산
    ev={}
    for n,m in sigs.items():
        rows=[event_drop(v,bu,cyc,tick,s,e) for s,e in segs(m)]
        ev[n]=(len(rows), np.array([r for r in rows if r is not None]))
    ev['S3 FOAMING 1~4 (번호별 이벤트)']=(sum(ev[f'S3 FOAMING_SHOT{k}'][0] for k in F), np.vstack([ev[f'S3 FOAMING_SHOT{k}'][1] for k in F if len(ev[f'S3 FOAMING_SHOT{k}'][1])]))
    ev['S4 On_HD1 & SHOT1~4']=(sum(ev[f'S4 On_HD1 & SHOT{k}'][0] for k in F), np.vstack([ev[f'S4 On_HD1 & SHOT{k}'][1] for k in F if len(ev[f'S4 On_HD1 & SHOT{k}'][1])]))
    # null: 같은 길이 L 의 가짜 이벤트를 S2(실제 사출) 가 창 [s-PRE, s+L-1+POST] 어디에도 없는 위치에
    s2_near=np.convolve(S2.astype(int), np.ones(1), 'same')>0
    cs=np.r_[0,np.cumsum(S2)]
    null={}
    for L in range(1,20):
        out=[]
        for s in range(PRE, len(v)-L-POST):
            e=s+L-1
            if cs[e+POST+1]-cs[s-PRE]>0: continue
            if not bu[s-PRE:e+POST+1].all() or cyc[s-PRE]!=cyc[e+POST]: continue
            r=event_drop(v,bu,cyc,tick,s,e)
            if r: out.append(r[1])
        null[L]=np.array(out)
    return ev,null
def auc(a,b):
    if len(a)==0 or len(b)==0: return np.nan
    a=np.asarray(a); b=np.sort(b)
    return (np.searchsorted(b,a,'left').sum()+0.5*(np.searchsorted(b,a,'right')-np.searchsorted(b,a,'left')).sum())/(len(a)*len(b))
if __name__=='__main__':
    import sys
    col=sys.argv[1] if len(sys.argv)>1 else 'Scale_Out___PT_P1'
    for name in ['fri','today','both']:
        if name=='both': continue
        df=load(name); ev,null=run(df,col)
        print(f'\n######## {name}  {col}')
        print('null(사출 없는 위치) 크기·분위 by 응답길이 L:')
        for L in [1,2,3,4,5,6,8,10,12]:
            n=null[L]; 
            if len(n): print(f'  L={L:2d} n={len(n):5d}  median={np.median(n):5.2f}%  P90={np.percentile(n,90):5.2f}  P95={np.percentile(n,95):5.2f}  P99={np.percentile(n,99):5.2f}')
        print(f"{'신호':32s} {'이벤트':>6s} {'평가':>5s} {'drop중앙%':>8s} {'P75':>6s} {'>P95비율':>8s} {'AUC':>5s}")
        for n,(tot,a) in ev.items():
            if len(a)==0: print(f'{n:32s} {tot:6d} {0:5d}'); continue
            d=a[:,1]; L=a[:,2].astype(int)
            thr=np.array([np.percentile(null[min(l,19)],95) if len(null[min(l,19)])>30 else np.nan for l in L])
            nulls=np.concatenate([null[l] for l in np.unique(L) if len(null[l])])
            # AUC: length-matched - weighted by event lengths
            aucs=[(auc(d[L==l],null[l]),(L==l).sum()) for l in np.unique(L) if len(null[l])>30]
            A=np.nansum([x*w for x,w in aucs])/max(sum(w for x,w in aucs if not np.isnan(x)),1)
            print(f'{n:32s} {tot:6d} {len(a):5d} {np.median(d):8.2f} {np.percentile(d,75):6.2f} {np.nanmean(d>thr)*100:7.1f}% {A:5.2f}   L med={np.median(L):.0f}')
