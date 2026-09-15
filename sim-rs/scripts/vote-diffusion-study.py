#!/usr/bin/env python3
"""Freeze, run, and record the matched vote-diffusion matrix (standard library only)."""
import argparse
import csv
import datetime
import hashlib
import io
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parents[1]
FIELDS = ['run', 'seed', 'nodes', 'committee', 'transport', 'fanout', 'slots',
          'status', 'started_utc', 'finished_utc', 'elapsed_s', 'exit_code', 'protects_producers']


def atomic(path, text):
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(text)
    temporary.replace(path)


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def manifest(root, names):
    return {name: digest(root / name) for name in sorted(names)}


def verify(root, checksums):
    for name, expected in checksums.items():
        if digest(root / name) != expected:
            raise ValueError(f'Checksum mismatch: {name}')


def save_runs(root, rows):
    output = io.StringIO(newline='')
    writer = csv.DictWriter(output, fieldnames=FIELDS, lineterminator='\n')
    writer.writeheader()
    writer.writerows(rows)
    atomic(root / 'runs.csv', output.getvalue())


def write_json(path, value):
    atomic(path, json.dumps(value, indent=2) + '\n')


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')


def positive(value):
    if not value.isascii() or not value.isdigit() or int(value) == 0:
        raise ValueError(f'Expected a positive integer: {value!r}')
    return int(value)


def choices(variable, default, allowed):
    values = os.environ.get(variable, default).split()
    if not values or len(values) != len(set(values)) or any(v not in allowed for v in values):
        raise ValueError(f'Invalid {variable}: {values!r}')
    return values


def flag(variable, default, true, false):
    values = choices(variable, default, {true, false})
    if len(values) != 1:
        raise ValueError(f'{variable} must be {true} or {false}')
    return values[0] == true


def build_binary(root, revision):
    manifest_path = str(HERE / 'Cargo.toml')
    target = Path(os.environ.get('CARGO_TARGET_DIR', HERE / 'target')).resolve()
    built = target / 'release/sim-cli'
    env = dict(os.environ)
    env.pop('VERGEN_GIT_SHA', None)
    for attempt in range(2):
        if attempt:
            # Older build scripts miss common refs in linked worktrees.
            subprocess.run(['cargo', 'clean', '--release', '-p', 'sim-cli',
                            '--manifest-path', manifest_path], check=True, env=env)
        subprocess.run(['cargo', 'build', '--release', '--locked', '--manifest-path',
                        manifest_path, '--bin', 'sim-cli'], check=True, env=env)
        version = subprocess.check_output([str(built), '--version'], text=True)
        embedded = re.search(r'-([0-9a-f]{7,40})\s*$', version)
        if embedded and revision.strip().startswith(embedded[1]):
            binary = root / 'sim-cli'
            shutil.copy2(built, binary)
            atomic(root / 'binary.txt', version)
            return binary
    raise ValueError(f'Executable revision differs from source {revision.strip()}: {version.strip()}')


def study_topology(size):
    source = 'topology-v2-cip.yaml' if size == '750' else 'topology-v2-1500.yaml'
    topology = json.loads((HERE.parent / 'data/simulation/pseudo-mainnet' / source).read_text())
    nodes = topology['nodes']
    for name, node in nodes.items():
        if size == '1500':
            node['cpu-core-count'] = 4
            for peer in node.get('producers', {}).values():
                peer['bandwidth-bytes-per-second'] = 1250000
        # These two study fixtures separate BPs and their two upstream relays.
        # Make that assumption explicit in the saved topology, not in routing.
        if node.get('stake', 0):
            peers = node.get('producers', {})
            if len(peers) != 2 or any(nodes[p].get('stake', 0) for p in peers):
                raise ValueError(f'{name}: expected a BP with two non-staking upstream relays')
            for peer in peers.values():
                peer['always-forward-votes'] = True
    return topology


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('seeds', nargs='*', default=['0'])
    args = parser.parse_args()
    sizes = choices('VOTE_STUDY_SIZES', '750 1500', {'750', '1500'})
    committees = choices('VOTE_STUDY_COMMITTEES', 'everyone top-stake-seats', {'everyone', 'top-stake-seats'})
    fanouts = os.environ.get('VOTE_STUDY_FANOUTS', 'all 22 16 8').split()
    if not fanouts or len(fanouts) != len(set(fanouts)):
        raise ValueError('Fanouts must be nonempty and distinct')
    for value in fanouts:
        if value != 'all':
            positive(value)
    slots = positive(os.environ.get('VOTE_STUDY_SLOTS', '400'))
    transports = choices('VOTE_STUDY_TRANSPORTS', 'announce-then-request push push-late-dedupe',
                         {'announce-then-request', 'push', 'push-late-dedupe'})
    capture = flag('VOTE_STUDY_NODE_TRAFFIC', '0', '1', '0')
    protects = flag('VOTE_STUDY_FANOUT_PROTECTS_PRODUCERS', 'false', 'true', 'false')
    seeds = args.seeds or ['0']
    if any(not s.isascii() or not s.isdigit() for s in seeds):
        raise ValueError('Seeds must be nonnegative integers')
    seeds = [str(int(s)) for s in seeds]
    if len(seeds) != len(set(seeds)):
        raise ValueError('Seeds must be distinct')
    dry_run = flag('VOTE_STUDY_DRY_RUN', '0', '1', '0')
    cfg = args.config.resolve(strict=True)
    root = args.output.resolve()
    root.mkdir()  # Never overwrite a previous study.
    for source, target in [(cfg, 'study-config.yaml'),
                           (HERE / 'parameters/config.default.yaml', 'defaults.yaml'),
                           (HERE / 'parameters/study-linear-tx-load.yaml', 'workload.yaml'),
                           (HERE / 'parameters/turbo.yaml', 'engine.yaml')]:
        shutil.copyfile(source, root / target)
    revision = subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True)
    atomic(root / 'revision.txt', revision)
    patch = subprocess.check_output(['git', '-C', str(HERE), 'diff', 'HEAD', '--',
                                     '.', '../shared-rs', '../data/simulation'], text=True)
    atomic(root / 'source.patch', patch)
    upstream = os.environ.get('VOTE_STUDY_CONFIG_REVISION')
    if not upstream:
        detected = subprocess.run(['git', '-C', str(cfg.parent), 'rev-parse', 'HEAD'],
                                  capture_output=True, text=True)
        upstream = detected.stdout.strip() if detected.returncode == 0 else 'unknown (input bytes preserved)'
    atomic(root / 'upstream-revision.txt', upstream + '\n')
    shutil.copyfile(Path(__file__), root / 'runner.py')
    for size in sizes:
        (root / f'topology-{size}.yaml').write_text(json.dumps(study_topology(size)))
    rows = []
    arms = [('announce-then-request', 'all')] if 'announce-then-request' in transports else []
    arms += [(transport, fanout) for fanout in fanouts
             for transport in ['push', 'push-late-dedupe'] if transport in transports]
    for seed in seeds:
        for size in sizes:
            for committee in committees:
                for transport, fanout in arms:
                    protection = str(protects and transport != 'announce-then-request' and fanout != 'all').lower()
                    name = f'{size}-{committee}-{transport}-f{fanout}-bp{protection}-s{seed}'
                    cap = 'null' if fanout == 'all' else str(int(fanout))
                    (root / (name + '.yaml')).write_text(
                        f'committee-selection-algorithm: "{committee}"\ncommittee-seat-count: 900\n'
                        f'quorum-weight-fraction: 0.75\nseed: {seed}\nvote-transport: "{transport}"\n'
                        f'vote-push-fanout: {cap}\nvote-push-fanout-protects-producers: {protection}\n'
                        f'vote-transport-echo-to-source: false\n')
                    rows.append(dict(zip(FIELDS, [name, seed, size, committee, transport, fanout, slots,
                                                'planned', '', '', '', '', protection])))
    save_runs(root, rows)
    inputs = manifest(root, [p.name for p in root.glob('*.yaml')] + ['source.patch'])
    write_json(root / 'input-sha256.json', inputs)
    logs = {}
    traffic_reports = {}
    write_json(root / 'log-sha256.json', logs)
    if dry_run:
        print(f'Planned {len(rows)} runs in {root}', flush=True)
        return 0
    binary = build_binary(root, revision)
    binary_hash = digest(binary)
    atomic(root / 'binary.sha256', binary_hash + '\n')
    for index, row in enumerate(rows, 1):
        verify(root, inputs)
        if digest(binary) != binary_hash:
            raise ValueError('Frozen executable changed')
        row.update(status='running', started_utc=utc())
        save_runs(root, rows)
        started = time.monotonic()
        traffic_name = row['run'] + '.vote-traffic.json'
        traffic_args = ['--vote-traffic', str(root / traffic_name)] if capture else []
        with (root / (row['run'] + '.txt')).open('w') as log:
            completed = subprocess.run([str(binary), str(root / f"topology-{row['nodes']}.yaml"),
                                        '-s', str(row['slots']),
                                        '-p', str(root / 'study-config.yaml'),
                                        '-p', str(root / 'workload.yaml'), '-p', str(root / 'engine.yaml'),
                                        '-p', str(root / (row['run'] + '.yaml'))] + traffic_args, stdout=log, stderr=subprocess.STDOUT)
        # Ctrl-C deliberately returns success for ordinary interactive runs.
        # A study result still needs the entire configured simulation interval.
        completion = f"Simulation completed: {row['slots']} slots."
        log_text = re.sub(r'\x1b\[[0-9;]*m', '', (root / (row['run'] + '.txt')).read_text())
        passed = completed.returncode == 0 and any(line.endswith(completion) for line in log_text.splitlines())
        if completed.returncode == 0 and not passed:
            print(f"{row['run']}: missing completion marker; recording a failed study run", flush=True)
        row.update(status='passed' if passed else 'failed',
                   finished_utc=utc(), elapsed_s=f'{time.monotonic() - started:.3f}', exit_code=completed.returncode)
        if capture and passed:
            traffic_reports[traffic_name] = digest(root / traffic_name)
            write_json(root / 'vote-traffic-sha256.json', traffic_reports)
        logs[row['run'] + '.txt'] = digest(root / (row['run'] + '.txt'))
        write_json(root / 'log-sha256.json', logs)
        save_runs(root, rows)
        print(f"[{index}/{len(rows)}] {row['run']}: {row['status']} ({row['elapsed_s']}s)", flush=True)
    verify(root, inputs)
    verify(root, logs)
    verify(root, traffic_reports)
    if digest(binary) != binary_hash:
        raise ValueError('Frozen executable changed')
    return int(any(row['status'] != 'passed' for row in rows))


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
