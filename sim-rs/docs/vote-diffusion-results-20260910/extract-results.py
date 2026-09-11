#!/usr/bin/env python3
"""Build a current report from completed logs only; never treat partial logs as results."""
import csv, datetime, hashlib, json, math, re, sys
from pathlib import Path

root=Path(sys.argv[1]).resolve() if len(sys.argv)>1 else Path(__file__).resolve().parent
for manifest in ['input-sha256.json','log-sha256.json']:
    checksums=json.loads((Path(__file__).resolve().parent/manifest).read_text())
    for name,digest in checksums.items():
        if hashlib.sha256((root/name).read_bytes()).hexdigest()!=digest:
            raise ValueError('Checksum mismatch: '+name)
with (root/'runs.csv').open() as f: runs=list(csv.DictReader(f))
ansi=re.compile(r'\x1b\[[0-9;]*m')
results=[]
errors=[]

def atomic(name,text):
    tmp=root/(name+'.tmp');tmp.write_text(text);tmp.replace(root/name)

def extract(text,pattern,required=True):
    matches=list(re.finditer(pattern,text,re.MULTILINE))
    if not matches:
        if required: raise ValueError('Missing metric: '+pattern)
        return None
    return matches[-1]

def quorum(text,label):
    line=extract(text,r'^  Quorum at '+label+r'[^\n]+').group(0)
    m=extract(line,r': (\d+) of (\d+) EB\(s\) reached one, (\d+) of them by the ([\d.]+)s deadline and (\d+) by the ([\d.]+)s inclusion deadline\.')
    q=dict(zip(['reached','total','by_vote_deadline','vote_deadline_s','by_inclusion','inclusion_s'],[int(m[1]),int(m[2]),int(m[3]),float(m[4]),int(m[5]),float(m[6])]))
    m=extract(line,r'Average ([\d.]+)s from t0 \(median ([\d.]+), p95 ([\d.]+), max ([\d.]+)\)',False)
    q.update(mean_s=float(m[1]) if m else None,median_s=float(m[2]) if m else None,p95_s=float(m[3]) if m else None,max_s=float(m[4]) if m else None)
    return q

topologies={}
for size in ['750','1500']:
    nodes=json.loads((root/f'topology-{size}.yaml').read_text())['nodes']
    stakes=sorted((int(n.get('stake',0) or 0) for n in nodes.values()),reverse=True)
    topologies[size]={'nodes':len(nodes),'pools':sum(s>0 for s in stakes),'total_stake':sum(stakes),'seated_stake':sum(stakes[:900]),'links':sum(len(n.get('producers',{})) for n in nodes.values())}

for row in runs:
    if row['status']!='passed': continue
    try:
        text=ansi.sub('',(root/(row['run']+'.txt')).read_text())
        if 'Final protocol stats:' not in text or 'Final network stats:' not in text:
            raise ValueError('No final summaries despite successful exit')
        final=text[text.rindex('Final protocol stats:'):]
        r=dict(row)
        r['nodes']=int(r['nodes']);r['seed']=int(r['seed']);r['elapsed_s']=float(r['elapsed_s'])
        r['ebs_generated']=int(extract(final,r'(\d+) EB\(s\) were generated;')[1])
        r['votes_generated']=int(extract(final,r'(\d+) total votes were generated\.')[1])
        r['l1_endorsements']=int(extract(final,r'(\d+) L1 block\(s\) had a Leios endorsement\.')[1])
        m=extract(final,r'Total generated voting weight: (\d+); quorum requires (\d+)',False)
        topo=topologies[row['nodes']]
        r['voting_weight_generated']=int(m[1]) if m else r['votes_generated']
        r['quorum_threshold']=int(m[2]) if m else math.ceil(int(row['nodes'])*.75)
        r['eligible_voters']=min(900,topo['pools']) if row['committee']=='top-stake-seats' else int(row['nodes'])
        r['eligible_stake_fraction']=topo['seated_stake']/topo['total_stake'] if row['committee']=='top-stake-seats' else 1.0
        r['quorum_first']=quorum(final,'the first node anywhere')
        r['quorum_median']=quorum(final,'the stake-weighted median node')
        r['quorum_p95']=quorum(final,'the 95th-percentile node by stake')
        m=extract(final,r'(\d+) Vote body message\(s\) were sent\. (\d+) of them were received .*? (\d+) of those .*?; (\d+) accepted; (\d+) pending; (\d+) verification\(s\) completed')
        for field,value in zip(['bodies_sent','bodies_received','redundant_arrivals','accepted','pending','verifications'],m.groups()): r[field]=int(value)
        assert r['bodies_received']==r['redundant_arrivals']+r['accepted']+r['pending']
        r['verifications_per_accepted']=r['verifications']/r['accepted'] if r['accepted'] else None
        m=extract(final,r'Vote mini-protocol traffic sent: (\d+) message\(s\), ([\d.]+) MB')
        r['wire_messages']=int(m[1]);r['wire_mb_rounded']=float(m[2])
        m=extract(final,r'(\d+) out of (\d+) vote bundle\(s\) reached 95% of nodes .*?; (\d+) never did\.',False)
        r['bundle_coverage95']={'reached':int(m[1]),'total':int(m[2]),'missed':int(m[3])} if m else None
        r['no_vote_reasons']={m[1]:int(m[2]) for m in re.finditer(r'^    (\w+): (\d+)$',final,re.MULTILINE)}
        m=extract(text,r'^\s*(\d+)\s+maximum resident set size\s*$',False)
        r['peak_rss_bytes']=int(m[1]) if m else None
        results.append(r)
    except Exception as e:
        errors.append({'run':row['run'],'error':str(e)})

index={(r['nodes'],r['committee'],r['seed'],r['transport'],r['fanout']):r for r in results}
pairs=[]
for r in results:
    if r['transport']=='announce-then-request': continue
    baseline=index.get((r['nodes'],r['committee'],r['seed'],'announce-then-request','all'))
    if not baseline: continue
    a=r['quorum_p95'];b=baseline['quorum_p95']
    pairs.append({'run':r['run'],'baseline':baseline['run'],'wire_ratio':r['wire_mb_rounded']/baseline['wire_mb_rounded'] if baseline['wire_mb_rounded'] else None,'q95_mean_delta_s':a['mean_s']-b['mean_s'] if a['mean_s'] is not None and b['mean_s'] is not None else None,'q95_reached_delta':a['reached']-b['reached'],'q95_by_7s_delta':a['by_vote_deadline']-b['by_vote_deadline'],'l1_endorsements_delta':r['l1_endorsements']-baseline['l1_endorsements'],'votes_generated_delta':r['votes_generated']-baseline['votes_generated'],'caveat':'Timing means are conditional on reaching quorum and may cover different EBs; equal counts do not prove equal EB identities.'})

payload={'updated_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds'),'revision':(root/'revision.txt').read_text().strip(),'binary_sha256':(root/'binary.sha256').read_text().strip(),'completed':len(results),'planned':len(runs),'topologies':topologies,'parse_errors':errors,'results':results,'paired_comparisons':pairs}
atomic('results.json',json.dumps(payload,indent=2)+'\n')
lines=['# Corrected Linear Leios vote study','',f"Updated {payload['updated_utc']}. **{len(results)}/{len(runs)} runs completed and parsed.**",'',f"Simulator `{payload['revision']}`; upstream config `{(root/'upstream-revision.txt').read_text().strip()}`.",'','All runs: 400 slots, 6 ms TX interarrival (~167 tx/s), 3s voting gate, 7s voting deadline, 14s earliest inclusion, echo disabled. The copied executable and saved inputs are hash-checked before execution.','', '**Interpretation:** this is simulator evidence. Pending/failed runs are excluded. Q95 times are means of per-EB times when nodes holding 95% of network stake each have a quorum, conditional on attainment. Fixed end-of-run truncation can leave the newest EBs unfinished. Equal endorsement counts do not establish identical EB identities. Vote-credit and header approximations limit transfer to Haskell.','', '| Nodes | Stake pools | Eligible in fixed-size arm | Stake coverage | Links |','|---:|---:|---:|---:|---:|']
for n,t in topologies.items(): lines.append(f"| {n} | {t['pools']} | {min(900,t['pools'])} | {t['seated_stake']/t['total_stake']:.1%} | {t['links']} |")
lines+=['','## Completed runs','','`stake` = top-stake-seats; `all-nodes` = everyone. Traffic is decimal GB, rounded in the simulator log. Q95 means nodes collectively holding 95% of network stake each have a quorum. This receiving-node percentage is separate from the 75% voting threshold for a certificate.','', '| Nodes | Committee | Seed | Transport | Fanout | EBs | L1 endorsements | Q95 reached | Q95 by 7s | Q95 mean s | Wire GB | Verify/accepted | Pending | Runtime min |','|---:|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
for r in results:
    q=r['quorum_p95'];mean='—' if q['mean_s'] is None else f"{q['mean_s']:.3f}";amp='—' if r['verifications_per_accepted'] is None else f"{r['verifications_per_accepted']:.2f}"
    lines.append(f"| {r['nodes']} | {'stake' if r['committee']=='top-stake-seats' else 'all-nodes'} | {r['seed']} | {r['transport']} | {r['fanout']} | {r['ebs_generated']} | {r['l1_endorsements']} | {q['reached']}/{q['total']} | {q['by_vote_deadline']}/{q['total']} | {mean} | {r['wire_mb_rounded']/1000:.3f} | {amp} | {r['pending']} | {r['elapsed_s']/60:.1f} |")
if errors:
    lines+=['','## Parsing issues','']+[f"- {e['run']}: {e['error']}" for e in errors]
lines+=['','Full numeric data and paired comparisons: [results.json](results.json). Inputs, overlays and summary logs are retained beside this report.','']
atomic('RESULTS.md','\n'.join(lines))
print(f'Parsed {len(results)}/{len(runs)} completed runs; {len(errors)} parsing issues.',flush=True)
