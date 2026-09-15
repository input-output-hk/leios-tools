#!/usr/bin/env python3
"""Exercise real capture, failure and retry paths against a built sim-cli."""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time
import unittest


class CaptureTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        nodes = {f'n{i}': {'location': [0, 0], 'stake': 1000, 'cpu-core-count': 4, 'tx-generation-weight': 1,
                          'producers': {f'n{j}': {'latency-ms': 1} for j in range(8) if i != j}}
                 for i in range(8)}
        (self.root / 'topology.json').write_text(json.dumps({'nodes': nodes}))
        (self.root / 'config.yaml').write_text('''leios-variant: linear-with-tx-references
simulate-transactions: true
engine: sequential
shard-count: 1
seed: 0
committee-selection-algorithm: top-stake-seats
committee-seat-count: 900
quorum-weight-fraction: 0.75
vote-transport: push
rb-generation-probability: 0.5
rb-body-max-size-bytes: 3000
eb-referenced-txs-max-size-bytes: 12000
tx-start-time: 0
tx-stop-time: 64
tx-generation-distribution: {distribution: constant, value: 50}
tx-size-bytes-distribution: {distribution: constant, value: 1500}
''')

    def command(self, *extra, slots=64, output=None):
        return [str(BINARY), 'topology.json'] + ([output] if output else []) + [
            '-p', 'config.yaml', '-s', str(slots), *extra]

    def run_cli(self, *extra, slots=64, output=None, timeout=15):
        return subprocess.run(self.command(*extra, slots=slots, output=output), cwd=self.root,
                              capture_output=True, text=True, timeout=timeout,
                              env=dict(os.environ, RUST_LOG='info'))

    def test_real_capture_reconciles_and_preserves_simulation_totals(self):
        for transport in ['push', 'announce-then-request']:
            (self.root / 'transport.yaml').write_text(f'vote-transport: {transport}\n')
            before = self.run_cli('-p', 'transport.yaml')
            after = self.run_cli('-p', 'transport.yaml', '--vote-traffic', 'traffic.json')
            self.assertEqual(before.returncode, 0, before.stderr)
            self.assertEqual(after.returncode, 0, after.stderr)
            pattern = r'Vote mini-protocol traffic sent: [^\n]+'
            self.assertEqual(re.findall(pattern, before.stdout)[-1], re.findall(pattern, after.stdout)[-1])
            report = json.loads((self.root / 'traffic.json').read_text())
            self.assertGreater(sum(n['sent']['bodies']['messages'] for n in report['nodes']), 0)
            log = self.root / 'run.log'
            log.write_text(after.stdout)
            result = subprocess.run([sys.executable, str(Path(__file__).with_name('summarize-vote-traffic.py')),
                                     str(self.root / 'traffic.json'), '--duration', '64', '--log', str(log),
                                     '--output', str(self.root / 'summary')], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            (self.root / 'traffic.json').unlink()

    def test_actor_capture_completes(self):
        (self.root / 'engine.yaml').write_text('engine: actor\n')
        result = self.run_cli('-p', 'engine.yaml', '--vote-traffic', 'traffic.json')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Simulation completed: 64 slots.', result.stdout)
        self.assertTrue((self.root / 'traffic.json').is_file())

    def test_existing_file_and_path_aliases_fail_before_simulation(self):
        for engine in ['sequential', 'actor']:
            (self.root / 'engine.yaml').write_text(f'engine: {engine}\n')
            (self.root / 'traffic.json').write_text('keep me')
            result = self.run_cli('-p', 'engine.yaml', '--vote-traffic', 'traffic.json', slots=1000000, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn('Slot 0', result.stdout)
            self.assertEqual((self.root / 'traffic.json').read_text(), 'keep me')
            (self.root / 'traffic.json').unlink()
        for event_path in ['traffic.json', './traffic.json', 'sub/../traffic.json']:
            result = self.run_cli('--vote-traffic', 'traffic.json', output=event_path)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('different files', result.stderr)
            self.assertFalse((self.root / 'traffic.json').exists())

    def test_symlink_alias_is_rejected(self):
        (self.root / 'events.json').symlink_to('traffic.json')
        result = self.run_cli('--vote-traffic', 'traffic.json', output='events.json')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('different files', result.stderr)
        self.assertFalse((self.root / 'traffic.json').exists())

    def test_monitor_failure_cancels_worker_and_retry_succeeds(self):
        (self.root / 'directory').mkdir()
        result = self.run_cli('--vote-traffic', 'traffic.json', output='directory', slots=1000000, timeout=5)
        self.assertNotEqual(result.returncode, 0)
        self.assertLess(len(result.stdout), 200000)
        self.assertFalse((self.root / 'traffic.json').exists())
        self.assertEqual(list(self.root.glob('.vote-traffic-*.tmp')), [])
        retry = self.run_cli('--vote-traffic', 'traffic.json')
        self.assertEqual(retry.returncode, 0, retry.stderr)

    def test_interrupted_capture_is_not_published(self):
        with (self.root / 'interrupted.log').open('w') as log:
            process = subprocess.Popen(self.command('--vote-traffic', 'traffic.json', slots=1000000),
                                       cwd=self.root, stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 5
                while not list(self.root.glob('.vote-traffic-*.tmp')) and time.monotonic() < deadline:
                    time.sleep(.01)
                process.send_signal(signal.SIGINT)
                self.assertNotEqual(process.wait(timeout=5), 0)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
        self.assertFalse((self.root / 'traffic.json').exists())
        self.assertEqual(list(self.root.glob('.vote-traffic-*.tmp')), [])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    args = parser.parse_args()
    BINARY = args.binary.resolve(strict=True)
    unittest.main(argv=[sys.argv[0]])
