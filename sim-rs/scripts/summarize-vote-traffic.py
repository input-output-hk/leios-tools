#!/usr/bin/env python3
"""Export per-node vote bytes and fixed-window rates from --vote-traffic JSON."""
import argparse
import csv
import gzip
import json
import math
from pathlib import Path
import re
import statistics

KINDS = ('bodies', 'announcements', 'requests')


def load(path):
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt') as stream:
        return json.load(stream)


def summarize(report, duration):
    if report['format_version'] not in (1, 2) or report['window_seconds'] != 1:
        raise ValueError('Expected version 1 or 2 with one-second buckets')
    if not math.isfinite(duration) or duration <= 0 or duration != report.get('requested_slots'):
        raise ValueError('Duration must equal the requested slot count of the completed run')
    observed = report['observed_until_s']
    if not math.isfinite(observed) or not 0 <= observed <= duration:
        raise ValueError('Invalid observed simulation interval')
    rows = []
    ids = set()
    for node in report['nodes']:
        if node['id'] in ids:
            raise ValueError('Duplicate node ID')
        ids.add(node['id'])
        row = dict(id=node['id'], name=node['name'], role='BP' if node['stake'] else 'relay', stake=node['stake'])
        for direction, index in [('sent', 0), ('received', 1)]:
            total = 0
            for kind in KINDS:
                counts = node[direction][kind]
                row[f'{direction}_{kind}_messages'] = counts['messages']
                row[f'{direction}_{kind}_bytes'] = counts['bytes']
                total += counts['bytes']
            if len(node['seconds']) > math.ceil(duration):
                raise ValueError('Traffic bucket falls outside the completed run')
            buckets = [pair[index] for pair in node['seconds']]
            if sum(buckets) != total:
                raise ValueError(f"Node {node['id']}: {direction} buckets do not reconcile")
            peak = max(buckets, default=0)
            row[f'{direction}_bytes'] = total
            row[f'{direction}_mean_mbit_s'] = total * 8 / 1e6 / duration
            row[f'{direction}_peak_1s_mbit_s'] = peak * 8 / 1e6
            row[f'{direction}_peak_window_start_s'] = buckets.index(peak) if peak else ''
        rows.append(row)
    if not rows:
        raise ValueError('No nodes in report')
    return rows


def reconcile(rows, log, duration, legacy=False):
    log = re.sub(r'\x1b\[[0-9;]*m', '', log)
    protocol = log.rfind('Final protocol stats:')
    network = log.rfind('Final network stats:')
    if protocol < 0 or network < protocol:
        raise ValueError('Missing final protocol/network summaries')
    completions = re.findall(r'Simulation completed: (\d+) slots\.', log[network:])
    if completions and int(completions[-1]) != duration:
        raise ValueError('Completion duration differs from the requested duration')
    if not completions:
        if not legacy:
            raise ValueError('Missing completion marker for the requested duration')
        slots = re.findall(r'Slot (\d+) has begun\.', log[:protocol])
        if not slots or int(slots[-1]) + 1 != duration:
            raise ValueError('Legacy log did not reach the requested final slot')
    log = log[network:]
    lines = re.findall(r'Vote mini-protocol traffic sent: .*', log)
    if not lines:
        if any(r['sent_bytes'] for r in rows):
            raise ValueError('Missing global vote-traffic summary')
        # The monitor omits the wire breakdown when no votes were sent.
        lines = ['Vote mini-protocol traffic sent: 0 message(s), 0.00 MB = 0 bodies (0.00 MB) + 0 announcement(s) (0.00 MB) + 0 request(s) (0.00 MB).']
    match = re.fullmatch(r'Vote mini-protocol traffic sent: (\d+) message\(s\), ([\d.]+) MB = (\d+) bod(?:y|ies) \(([\d.]+) MB\) \+ (\d+) announcement\(s\) \(([\d.]+) MB\) \+ (\d+) request\(s\) \(([\d.]+) MB\)\.', lines[-1])
    if not match:
        raise ValueError('Unrecognized global vote-traffic summary')
    groups = match.groups()
    for kind, count, mb in zip(KINDS, groups[2::2], groups[3::2]):
        if sum(r[f'sent_{kind}_messages'] for r in rows) != int(count):
            raise ValueError(f'{kind} send count differs from global summary')
        if abs(sum(r[f'sent_{kind}_bytes'] for r in rows) / 1e6 - float(mb)) > .0051:
            raise ValueError(f'{kind} send bytes differ from global summary')
    bodies = re.findall(r'(\d+) Vote body message\(s\) were sent\. (\d+) of them were received', log)
    if not bodies or sum(r['received_bodies_messages'] for r in rows) != int(bodies[-1][1]):
        raise ValueError('Body receive count differs from global summary')


def percentile(values, p):
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * p) - 1)]


def markdown(rows, duration):
    lines = ['# Per-node vote traffic', '', f'Duration: {duration:g} simulated seconds. Decimal GB/MB and Mbit/s. BP means a stake-holding node in a topology that separates BPs from relays.', '',
             'Traffic includes duplicate bodies plus vote announcements and requests. Sends are recorded when queued; receives when delivered. No TCP/IP framing or other protocols are included. Peaks are the busiest fixed one-second buckets aligned to simulation time zero, not instantaneous or sliding-window link throughput. A per-node send peak sums all its outgoing links.', '',
             '| Role | Nodes | Sent GB | Received GB | Median sent MB/node | p95 sent MB/node | Max sent MB/node |',
             '|---|---:|---:|---:|---:|---:|---:|']
    for role in ['BP', 'relay', 'all']:
        subset = [r for r in rows if role == 'all' or r['role'] == role]
        if not subset:
            continue
        sent = [r['sent_bytes'] / 1e6 for r in subset]
        lines.append(f"| {role} | {len(subset)} | {sum(sent)/1000:.6f} | {sum(r['received_bytes'] for r in subset)/1e9:.6f} | {statistics.median(sent):.3f} | {percentile(sent,.95):.3f} | {max(sent):.3f} |")
    lines += ['', 'Percentiles below are across nodes, using each node’s own busiest one-second window. Nodes can peak at different times.', '',
              '| Role | Median node peak sent Mbit/s | p95 | Max | Median node peak received Mbit/s | p95 | Max |',
              '|---|---:|---:|---:|---:|---:|---:|']
    for role in ['BP', 'relay', 'all']:
        subset = [r for r in rows if role == 'all' or r['role'] == role]
        if not subset:
            continue
        values = []
        for direction in ['sent', 'received']:
            peaks = [r[f'{direction}_peak_1s_mbit_s'] for r in subset]
            values.extend([statistics.median(peaks), percentile(peaks,.95), max(peaks)])
        lines.append('| ' + role + ' | ' + ' | '.join(f'{v:.3f}' for v in values) + ' |')
    lines += ['', 'The [per-node CSV](nodes.csv) includes separate body/control counts, sent/received totals, mean rates, and each peak’s window start. Raw JSON retains every node’s one-second buckets.', '']
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    parser.add_argument('--duration', type=float, required=True, help='Actual simulated duration of the completed run, in seconds')
    parser.add_argument('--log', type=Path, required=True, help='Reconcile counts with the simulator summary')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--legacy-runs', type=Path,
                        help='For version 1 archives only: runs.csv proving this run exited successfully')
    args = parser.parse_args()
    report = load(args.report)
    legacy = report['format_version'] == 1
    if legacy:
        if not args.legacy_runs:
            raise ValueError('Version 1 requires archived runs.csv via --legacy-runs')
        with args.legacy_runs.open() as stream:
            runs = list(csv.DictReader(stream))
        candidates = [r for r in runs if int(r['seed']) == report['seed']
                      and int(r['nodes']) == len(report['nodes'])
                      and int(r['slots']) == args.duration]
        if len(candidates) != 1 or candidates[0]['status'] != 'passed' or candidates[0]['exit_code'] != '0':
            raise ValueError('Legacy run must have one matching successful completion record')
    rows = summarize(report, args.duration)
    reconcile(rows, args.log.read_text(), args.duration, legacy=legacy)
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / 'nodes.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    (args.output / 'summary.md').write_text(markdown(rows, args.duration))
    print(f'Wrote {len(rows)} node rows; counts reconcile with the global summary.')


if __name__ == '__main__':
    main()
