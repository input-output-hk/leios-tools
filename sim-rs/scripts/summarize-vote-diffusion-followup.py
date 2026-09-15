#!/usr/bin/env python3
"""Reconcile a completed focused matrix and report fanout and control-size effects."""
import argparse
import csv
import hashlib
import importlib.util
import json
from pathlib import Path
import statistics
import subprocess
import sys

sys.dont_write_bytecode = True
SCRIPTS = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('traffic', SCRIPTS / 'summarize-vote-traffic.py')
traffic = importlib.util.module_from_spec(spec)
spec.loader.exec_module(traffic)


def ratio(numerator, denominator):
    return numerator / denominator if denominator else None


def fmt(value, digits=3):
    return 'n/a' if value is None else f'{value:.{digits}f}'


def verify(root, manifest):
    checksums = json.loads((root / manifest).read_text())
    for name, expected in checksums.items():
        with (root / name).open('rb') as stream:
            if hashlib.file_digest(stream, 'sha256').hexdigest() != expected:
                raise ValueError(f'Checksum mismatch: {name}')
    return checksums


def key(row):
    return (row['transport'], row['fanout'], row['protects_producers'],
            int(row['announcement_bytes']), int(row['request_bytes']))


def summarize(root, output):
    # Reuse the protocol extractor, which verifies frozen inputs and final logs.
    subprocess.run([sys.executable, str(SCRIPTS.parent / 'docs' /
                    'vote-diffusion-results-20260910' / 'extract-results.py'), str(root)], check=True)
    extracted = json.loads((root / 'results.json').read_text())
    with (root / 'runs.csv').open() as stream:
        runs = list(csv.DictReader(stream))
    expected = {('push', str(k), protect, 8, 8) for k in [22, 16, 8]
                for protect in ['false', 'true']}
    expected.add(('push', 'all', 'false', 8, 8))
    expected.update(('announce-then-request', 'all', 'false', size, size) for size in [8, 40, 64])
    seeds = sorted({int(r['seed']) for r in runs})
    durations = {int(r['slots']) for r in runs}
    if len(durations) != 1 or next(iter(durations)) <= 0:
        raise ValueError('All focused runs must have one positive duration')
    for seed in seeds:
        selected = [r for r in runs if int(r['seed']) == seed]
        if len(selected) != 10 or {key(r) for r in selected} != expected:
            raise ValueError(f'Seed {seed}: expected the ten focused configurations')
    if any(r['nodes'] != '1500' or r['committee'] != 'top-stake-seats'
           or r['status'] != 'passed' or r['exit_code'] != '0' for r in runs):
        raise ValueError('Expected completed 1500-node stake-weighted runs')
    captures = verify(root, 'vote-traffic-sha256.json')
    topology = json.loads((root / 'topology-1500.yaml').read_text())['nodes']
    results = {r['run']: r for r in extracted['results']}
    output.mkdir(parents=True, exist_ok=True)
    combined = []
    for run in runs:
        name = run['run']
        filename = name + '.vote-traffic.json'
        if filename not in captures:
            raise ValueError(f'Capture missing from checksum manifest: {name}')
        report = traffic.load(root / filename)
        if report['format_version'] != 2 or report['seed'] != int(run['seed']):
            raise ValueError(f'Unexpected capture version or seed: {name}')
        duration = int(run['slots'])
        nodes = traffic.summarize(report, duration)
        if len(nodes) != len(topology) or {r['name'] for r in nodes} != set(topology):
            raise ValueError(f'Capture nodes differ from topology: {name}')
        if any(r['stake'] != (topology[r['name']].get('stake') or 0) for r in nodes):
            raise ValueError(f'Capture stake differs from topology: {name}')
        traffic.reconcile(nodes, (root / (name + '.txt')).read_text(), duration)
        for kind, field in [('announcements', 'announcement_bytes'), ('requests', 'request_bytes')]:
            for direction in ['sent', 'received']:
                if any(r[f'{direction}_{kind}_bytes'] !=
                       r[f'{direction}_{kind}_messages'] * int(run[field]) for r in nodes):
                    raise ValueError(f'Control bytes differ from recorded settings: {name}')
        target = output / name
        target.mkdir(exist_ok=True)
        with (target / 'nodes.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(nodes[0]), lineterminator='\n')
            writer.writeheader()
            writer.writerows(nodes)
        (target / 'summary.md').write_text(traffic.markdown(nodes, duration))
        totals = {direction: {kind: {unit: sum(r[f'{direction}_{kind}_{unit}'] for r in nodes)
                                    for unit in ['messages', 'bytes']}
                              for kind in traffic.KINDS}
                  for direction in ['sent', 'received']}
        for direction in totals:
            totals[direction]['total_bytes'] = sum(totals[direction][k]['bytes'] for k in traffic.KINDS)
        roles = {}
        for role in ['BP', 'relay']:
            subset = [r for r in nodes if r['role'] == role]
            roles[role] = {'nodes': len(subset)}
            for direction in ['sent', 'received']:
                values = [r[f'{direction}_bytes'] for r in subset]
                peaks = [r[f'{direction}_peak_1s_mbit_s'] for r in subset]
                roles[role][direction] = dict(total_bytes=sum(values), median_bytes=statistics.median(values),
                                              p95_bytes=traffic.percentile(values, .95), max_bytes=max(values),
                                              median_peak_1s_mbit_s=statistics.median(peaks),
                                              p95_peak_1s_mbit_s=traffic.percentile(peaks, .95),
                                              max_peak_1s_mbit_s=max(peaks))
        combined.append(dict(protocol=results[name], wire=totals, roles=roles))
    comparisons = []
    for seed in seeds:
        group = {key(r['protocol']): r for r in combined if r['protocol']['seed'] == seed}
        push = group[('push', 'all', 'false', 8, 8)]
        if any(push['wire']['sent'][kind]['messages'] for kind in ['announcements', 'requests']):
            raise ValueError('Unrestricted push baseline unexpectedly has control messages')
        for size in [8, 40, 64]:
            pull = group[('announce-then-request', 'all', 'false', size, size)]
            a = push['protocol']['quorum_p95']['mean_s']
            b = pull['protocol']['quorum_p95']['mean_s']
            comparisons.append(dict(seed=seed, push=push['protocol']['run'], pull=pull['protocol']['run'],
                                    control_bytes=size,
                                    push_pull_wire_ratio=ratio(push['wire']['sent']['total_bytes'],
                                                               pull['wire']['sent']['total_bytes']),
                                    push_q95_mean_advantage_s=b-a if a is not None and b is not None else None))
    payload = dict(revision=extracted['revision'], binary_sha256=extracted['binary_sha256'],
                   seeds=seeds, slots=next(iter(durations)), results=combined, control_comparisons=comparisons)
    original = json.loads((SCRIPTS.parent / 'docs/vote-diffusion-results-20260910/results.json').read_text())
    legacy = {(r['seed'], r['transport'], r['fanout']): r for r in original['results']
              if r['nodes'] == 1500 and r['committee'] == 'top-stake-seats'}
    # Compare protocol outcomes, not wall-clock duration or new instrumentation.
    fields = ['ebs_generated', 'l1_endorsements', 'votes_generated', 'voting_weight_generated',
              'quorum_first', 'quorum_median', 'quorum_p95', 'bodies_sent', 'bodies_received',
              'redundant_arrivals', 'accepted', 'pending', 'verifications', 'wire_messages',
              'wire_mb_rounded', 'bundle_coverage95']
    compatibility = []
    for item in combined:
        r = item['protocol']
        prior = legacy.get((r['seed'], r['transport'], r['fanout']))
        if not prior or r['protects_producers'] != 'false' or int(r['announcement_bytes']) != 8 \
                or int(r['request_bytes']) != 8 or r['slots'] != prior['slots']:
            continue
        changes = {field: dict(archived=prior[field], current=r[field])
                   for field in fields if prior[field] != r[field]}
        compatibility.append(dict(run=r['run'], archived_run=prior['run'], differences=changes))
    payload['archived_default_comparisons'] = compatibility
    (output / 'followup.json').write_text(json.dumps(payload, indent=2) + '\n')
    (output / 'SUMMARY.md').write_text(markdown(payload))
    print(f'Reconciled {len(combined)} runs and wrote focused tables to {output}.')


def markdown(payload):
    pools = payload['results'][0]['roles']['BP']['nodes']
    lines = ['# Focused vote diffusion comparison', '',
             f"1500 nodes, {pools} stake pools, top-stake-seats, {payload['slots']} slots; seeds: "
             + ', '.join(map(str, payload['seeds'])) + '.', '',
             'Traffic is exact decimal GB from per-node captures and includes vote bodies, announcements and requests. '
             'It excludes TCP/IP. The 40/40 and 64/64 byte cases are sensitivity assumptions, not verified encodings. '
             'Q95 means quorum availability at nodes holding 95% of stake; each node needs 75% of total voting stake. '
             'Timing means are conditional on attainment and may cover different EBs. Newest EBs may be unfinished at the fixed cutoff.', '',
             '## Availability and traffic', '',
             'Protected BP links take places within the total fanout cap. Protecting one BP under cap 22 leaves 21 places for other consumers.', '',
             '| Seed | Transport | Cap | Protect BP | Announce/request B | EBs | Q50 | Q95 | Q95 by 7s | Q95 mean s | Endorsements | Bundles at 95% of nodes | Wire GB |',
             '|---:|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|']
    for item in payload['results']:
        r = item['protocol']; q = r['quorum_p95']; coverage = r['bundle_coverage95']
        covered = f"{coverage['reached']}/{coverage['total']}" if coverage else 'n/a'
        lines.append(f"| {r['seed']} | {r['transport']} | {r['fanout']} | {r['protects_producers']} | "
                     f"{r['announcement_bytes']}/{r['request_bytes']} | {r['ebs_generated']} | "
                     f"{r['quorum_median']['reached']}/{r['ebs_generated']} | {q['reached']}/{q['total']} | "
                     f"{q['by_vote_deadline']}/{q['total']} | {fmt(q['mean_s'])} | {r['l1_endorsements']} | "
                     f"{covered} | {item['wire']['sent']['total_bytes']/1e9:.6f} |")
    lines += ['', '## Control-size sensitivity', '',
              'The same unrestricted push case is the baseline for all three pull cases because it sends no control messages. '
              'These ratios use executed simulations, not a repricing of fixed message counts.', '',
              '| Seed | Pull announce/request B | Push / pull bytes | Push Q95 mean advantage s |',
              '|---:|---|---:|---:|']
    for row in payload['control_comparisons']:
        lines.append(f"| {row['seed']} | {row['control_bytes']}/{row['control_bytes']} | "
                     f"{fmt(row['push_pull_wire_ratio'])} | {fmt(row['push_q95_mean_advantage_s'])} |")
    lines += ['', '## Message counts and verification', '',
              'Obsolete checks are a subset of total checks, measured when completion occurs after local EB pruning. '
              'They are not subtracted from the reported cost. Missing metrics are unmeasured, not zero.', '',
              '| Run | Sent bodies | Announcements | Requests | Bodies GB | Announcements GB | Requests GB | Checks / accepted | Obsolete checks |',
              '|---|---:|---:|---:|---:|---:|---:|---:|---:|']
    for item in payload['results']:
        r = item['protocol']; w = item['wire']['sent']; obsolete = r.get('obsolete_work', {}).get('verifications')
        cells = [str(w[k]['messages']) for k in traffic.KINDS] + [f"{w[k]['bytes']/1e9:.6f}" for k in traffic.KINDS]
        lines.append('| ' + r['run'] + ' | ' + ' | '.join(cells) + ' | ' +
                     fmt(r['verifications_per_accepted']) + ' | ' + (str(obsolete) if obsolete is not None else 'n/a') + ' |')
    lines += ['', '## Per-node distribution', '',
              'Peaks are node totals across outgoing links in fixed one-second windows. These are queued sends, '
              'not instantaneous link throughput. The 10 Mbit/s limit applies to each link, not each node.', '',
              '| Run | Role | Total sent GB | Median sent MB/node | p95 | Max | Median received MB/node | Median node peak sent Mbit/s | Max |',
              '|---|---|---:|---:|---:|---:|---:|---:|---:|']
    for item in payload['results']:
        for role, data in item['roles'].items():
            sent = data['sent']; received = data['received']; name = item['protocol']['run']
            lines.append(f"| [{name}]({name}/summary.md) | {role} | {sent['total_bytes']/1e9:.6f} | "
                         f"{sent['median_bytes']/1e6:.3f} | {sent['p95_bytes']/1e6:.3f} | {sent['max_bytes']/1e6:.3f} | "
                         f"{received['median_bytes']/1e6:.3f} | {sent['median_peak_1s_mbit_s']:.3f} | {sent['max_peak_1s_mbit_s']:.3f} |")
    lines += ['', 'Each linked report includes received-byte and peak-rate percentiles, with a per-node CSV. '
              'The machine-readable [followup.json](followup.json) includes exact totals, all protocol metrics and obsolete-work subsets.', '',
              '## Archived default comparison', '',
              'Matching unprotected 8/8 cases are compared with the original 400-slot runs. '
              'Checks include quorum counts/times, endorsements, vote generation, body delivery, '
              'verification and wire totals. Wall-clock runtime and newly added instrumentation are excluded.', '',
              '| Run | Compared protocol metrics |', '|---|---|']
    for row in payload['archived_default_comparisons']:
        result = 'All equal' if not row['differences'] else 'Different: ' + ', '.join(row['differences'])
        lines.append(f"| {row['run']} | {result} |")
    if not payload['archived_default_comparisons']:
        lines.append('| n/a | No matching archived duration and configuration |')
    lines.append('')
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    summarize(args.directory.resolve(), args.output.resolve())


if __name__ == '__main__':
    main()
