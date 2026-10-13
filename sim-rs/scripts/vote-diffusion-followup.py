#!/usr/bin/env python3
"""Freeze and run ten matched fanout/control-size sensitivity cases per seed."""
import argparse
import csv
import importlib.util
import os
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('study', Path(__file__).with_name('vote-diffusion-study.py'))
study = importlib.util.module_from_spec(spec)
spec.loader.exec_module(study)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('seeds', nargs='*', default=['0'])
    args = parser.parse_args()
    root = args.output.resolve()
    root.mkdir()
    plans = root / 'plans'
    plans.mkdir()
    original_env = dict(os.environ)
    dry_run = study.flag('VOTE_STUDY_DRY_RUN', '0', '1', '0')
    capture = study.flag('VOTE_STUDY_NODE_TRAFFIC', '1', '1', '0')
    # Keep the total copy budget fixed when comparing the two forwarding rules.
    cases = [('push', 'all 22 16 8', 'false', 8),
             ('push', '22 16 8', 'true', 8)]
    cases += [('announce-then-request', 'all', 'false', size) for size in [8, 40, 64]]
    rows = []
    try:
        for index, (transport, fanouts, protects, control_bytes) in enumerate(cases):
            os.environ.clear()
            os.environ.update({k: v for k, v in original_env.items() if not k.startswith('VOTE_STUDY_')
                               or k in ('VOTE_STUDY_SLOTS', 'VOTE_STUDY_CONFIG_REVISION')})
            os.environ.update(VOTE_STUDY_DRY_RUN='1', VOTE_STUDY_SIZES='1500',
                              VOTE_STUDY_COMMITTEES='top-stake-seats', VOTE_STUDY_TRANSPORTS=transport,
                              VOTE_STUDY_FANOUTS=fanouts, VOTE_STUDY_FANOUT_PROTECTS_PRODUCERS=protects,
                              VOTE_STUDY_ANNOUNCEMENT_BYTES=str(control_bytes),
                              VOTE_STUDY_REQUEST_BYTES=str(control_bytes))
            part = plans / str(index)
            study.main([str(args.config), str(part), *(args.seeds or ['0'])])
            with (part / 'runs.csv').open() as stream:
                rows.extend(csv.DictReader(stream))
            # All common inputs must be identical; keep only one frozen copy.
            for path in part.iterdir():
                if path.name in ('runs.csv', 'input-sha256.json', 'log-sha256.json'):
                    continue
                target = root / path.name
                if target.exists():
                    if study.digest(path) != study.digest(target):
                        raise ValueError(f'Common input changed during planning: {path.name}')
                else:
                    shutil.copy2(path, target)
    finally:
        os.environ.clear()
        os.environ.update(original_env)
    if len({row['run'] for row in rows}) != len(rows):
        raise ValueError('Duplicate focused run identity')
    # Keep each protected/unprotected pair adjacent for early inspection.
    def order(row):
        if row['fanout'] != 'all':
            return (int(row['seed']), 0, -int(row['fanout']), row['protects_producers'])
        if row['transport'] == 'announce-then-request':
            return (int(row['seed']), 1, int(row['announcement_bytes']), '')
        return (int(row['seed']), 2, 0, '')
    rows.sort(key=order)
    shutil.copy2(__file__, root / 'followup-runner.py')
    shutil.rmtree(plans)
    study.save_runs(root, rows)
    inputs = study.manifest(root, [p.name for p in root.glob('*.yaml')]
                            + ['source.patch', 'runner.py', 'followup-runner.py'])
    study.write_json(root / 'input-sha256.json', inputs)
    study.write_json(root / 'log-sha256.json', {})
    if dry_run:
        print(f'Planned {len(rows)} focused runs in {root}')
        return 0
    return study.execute(root, rows, inputs, capture, (root / 'revision.txt').read_text())


if __name__ == '__main__':
    sys.exit(main())
