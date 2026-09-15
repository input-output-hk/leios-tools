#!/usr/bin/env python3
"""Regression checks for the runner/extractor interface; uses the archived logs."""
import csv
import importlib.util
from unittest.mock import patch
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

HERE = Path(__file__).resolve().parents[1]
REPORT = HERE / 'docs/vote-diffusion-results-20260910'
EXTRACT = REPORT / 'extract-results.py'
RUNNER = HERE / 'scripts/vote-diffusion-study.sh'
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('runner', RUNNER.with_suffix('.py'))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class StudyInterfaceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.archive_temp = tempfile.TemporaryDirectory()
        cls.archive = Path(cls.archive_temp.name)
        for name in ['inputs.tar.gz', 'summary-logs.tar.gz']:
            with tarfile.open(REPORT / name) as archive:
                archive.extractall(cls.archive, filter='data')
        for name in ['runs.csv', 'revision.txt', 'upstream-revision.txt', 'binary.sha256']:
            shutil.copyfile(REPORT / name, cls.archive / name)
        with (cls.archive / 'runs.csv').open() as stream:
            cls.rows = list(csv.DictReader(stream))

    @classmethod
    def tearDownClass(cls):
        cls.archive_temp.cleanup()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.addCleanup(self.temporary.cleanup)

    def extract(self, root=None, *options):
        return subprocess.run([sys.executable, str(EXTRACT), str(root or self.root), *options],
                              capture_output=True, text=True)

    def manifest(self, name, filenames):
        data = {f: hashlib.sha256((self.root / f).read_bytes()).hexdigest() for f in filenames}
        (self.root / name).write_text(json.dumps(data))

    def subset(self):
        row = dict(next(r for r in self.rows if r['nodes'] == '750'))
        for name in ['topology-750.yaml', row['run'] + '.txt', row['run'] + '.yaml',
                     'revision.txt', 'upstream-revision.txt', 'binary.sha256']:
            shutil.copyfile(self.archive / name, self.root / name)
        # A fresh run's logs and inputs have their own manifests; the frozen
        # 108-run manifests must not be applied to this one-size matrix.
        self.manifest('input-sha256.json', ['topology-750.yaml', row['run'] + '.yaml'])
        self.manifest('log-sha256.json', [row['run'] + '.txt'])
        self.write_rows([row])
        return row

    def write_rows(self, rows):
        with (self.root / 'runs.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    def test_published_archive_reproduces_all_numeric_results(self):
        result = self.extract(self.archive, '--archive')
        self.assertEqual(result.returncode, 0, result.stderr)
        actual = json.loads((self.archive / 'results.json').read_text())
        expected = json.loads((REPORT / 'results.json').read_text())
        self.assertEqual(actual['results'], expected['results'])
        self.assertEqual(actual['paired_comparisons'], expected['paired_comparisons'])
        self.assertEqual(actual['completed'], 108)

    def test_subset_uses_local_manifests_and_only_its_topology(self):
        row = self.subset()
        result = self.extract()
        self.assertEqual(result.returncode, 0, result.stderr)
        actual = json.loads((self.root / 'results.json').read_text())
        self.assertEqual(actual['completed'], 1)
        self.assertEqual(set(actual['topologies']), {'750'})
        self.assertEqual(actual['results'][0]['elapsed_s'], float(row['elapsed_s']))

    def test_missing_elapsed_time_is_a_nonzero_parse_failure(self):
        row = self.subset()
        del row['elapsed_s']
        self.write_rows([row])
        result = self.extract()
        self.assertNotEqual(result.returncode, 0)
        actual = json.loads((self.root / 'results.json').read_text())
        self.assertEqual(actual['completed'], 0)
        self.assertIn('elapsed_s', actual['parse_errors'][0]['error'])

    def test_changed_log_fails_checksum_verification(self):
        row = self.subset()
        with (self.root / (row['run'] + '.txt')).open('a') as stream:
            stream.write('\nchanged\n')
        result = self.extract()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Checksum mismatch', result.stderr)

    def test_passed_log_must_be_in_manifest(self):
        self.subset()
        (self.root / 'log-sha256.json').write_text('{}')
        result = self.extract()
        self.assertNotEqual(result.returncode, 0)
        actual = json.loads((self.root / 'results.json').read_text())
        self.assertIn('missing from checksum manifest', actual['parse_errors'][0]['error'])

    def test_unsuccessful_run_is_not_a_successful_extraction(self):
        row = self.subset()
        row['status'] = 'failed'
        self.write_rows([row])
        self.assertNotEqual(self.extract().returncode, 0)

    def add_obsolete_metrics(self, row, first=4):
        with (self.root / (row['run'] + '.txt')).open('a') as stream:
            stream.write(f"\n  Obsolete vote work (subsets of totals): 9 arrivals (846 bytes; 8 after prior processing); 14 completed verifications ({first} first, 10 repeat; 3 cache reinsertions).\n")
            stream.write("  Obsolete vote traffic sent (subsets of totals): 8 bodies (752 bytes); 7 announcements (56 bytes).\n")
        self.manifest('log-sha256.json', [row['run'] + '.txt'])

    def test_obsolete_metrics_are_extracted_as_subsets(self):
        row = self.subset()
        self.add_obsolete_metrics(row)
        result = self.extract()
        self.assertEqual(result.returncode, 0, result.stderr)
        actual = json.loads((self.root / 'results.json').read_text())['results'][0]
        self.assertEqual(actual['obsolete_work']['cache_reinsertions'], 3)
        self.assertEqual(actual['obsolete_work']['first_verifications'], 4)
        self.assertEqual(actual['obsolete_work']['repeat_verifications'], 10)
        self.assertGreater(actual['verifications'], 14)

    def test_obsolete_checks_must_reconcile(self):
        row = self.subset()
        self.add_obsolete_metrics(row, first=5)
        result = self.extract()
        self.assertNotEqual(result.returncode, 0)
        actual = json.loads((self.root / 'results.json').read_text())
        self.assertIn('do not reconcile', actual['parse_errors'][0]['error'])

    def test_runner_executes_capture_and_records_its_checksum(self):
        output = self.root / 'one-run'
        fake_binary = self.root / 'sim-cli'
        fake_binary.write_text("#!/usr/bin/env python3\nimport sys\nfrom pathlib import Path\np = Path(sys.argv[sys.argv.index('--vote-traffic') + 1])\np.write_text('{\"captured\":true}\\n')\nprint('Simulation completed: ' + sys.argv[sys.argv.index('-s') + 1] + ' slots.')\n")
        fake_binary.chmod(0o755)
        env = dict(os.environ, VOTE_STUDY_DRY_RUN='0', VOTE_STUDY_SIZES='1500',
                   VOTE_STUDY_COMMITTEES='top-stake-seats', VOTE_STUDY_FANOUTS='all',
                   VOTE_STUDY_TRANSPORTS='push', VOTE_STUDY_NODE_TRAFFIC='1')
        with patch.dict(os.environ, env, clear=True), patch.object(runner, 'build_binary', return_value=fake_binary), \
                patch.object(sys, 'argv', [str(RUNNER), str(self.archive / 'study-config.yaml'), str(output), '0']):
            self.assertEqual(runner.main(), 0)
        with (output / 'runs.csv').open() as stream:
            rows = list(csv.DictReader(stream))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['status'], 'passed')
        self.assertEqual(rows[0]['run'], '1500-top-stake-seats-push-fall-bpfalse-a8-r8-s0')
        name = rows[0]['run'] + '.vote-traffic.json'
        self.assertEqual(json.loads((output / name).read_text()), {'captured': True})
        checksums = json.loads((output / 'vote-traffic-sha256.json').read_text())
        self.assertEqual(checksums[name], runner.digest(output / name))

    def test_runner_rejects_a_successful_exit_without_completion(self):
        output = self.root / 'interrupted-run'
        fake_binary = self.root / 'sim-cli'
        fake_binary.write_text("#!/usr/bin/env python3\nprint('Final protocol stats:')\nprint('Final network stats:')\n")
        fake_binary.chmod(0o755)
        env = dict(os.environ, VOTE_STUDY_DRY_RUN='0', VOTE_STUDY_SIZES='750',
                   VOTE_STUDY_COMMITTEES='top-stake-seats', VOTE_STUDY_FANOUTS='all',
                   VOTE_STUDY_TRANSPORTS='push', VOTE_STUDY_NODE_TRAFFIC='0')
        with patch.dict(os.environ, env, clear=True), patch.object(runner, 'build_binary', return_value=fake_binary), \
                patch.object(sys, 'argv', [str(RUNNER), str(self.archive / 'study-config.yaml'), str(output), '0']):
            self.assertEqual(runner.main(), 1)
        with (output / 'runs.csv').open() as stream:
            row, = csv.DictReader(stream)
        self.assertEqual(row['exit_code'], '0')
        self.assertEqual(row['status'], 'failed')

    def test_stale_binary_rebuilt_and_rejected_if_still_stale(self):
        revision = 'abcdef0123456789'
        target = self.root / 'target'
        (target / 'release').mkdir(parents=True)
        (target / 'release/sim-cli').write_text('executable fixture')
        with patch.dict(os.environ, CARGO_TARGET_DIR=str(target)), patch.object(runner.subprocess, 'run') as run:
            with patch.object(runner.subprocess, 'check_output', side_effect=['sim-cli 2.0.1-1234567\n', 'sim-cli 2.0.1-abcdef0\n']):
                runner.build_binary(self.root, revision)
            self.assertEqual([c.args[0][1] for c in run.call_args_list], ['build', 'clean', 'build'])
            with patch.object(runner.subprocess, 'check_output', return_value='sim-cli 2.0.1-1234567\n'):
                with self.assertRaisesRegex(ValueError, 'Executable revision differs'):
                    runner.build_binary(self.root, revision)

    def test_protection_is_named_and_recorded_in_both_rules(self):
        names = []
        for protection in ['false', 'true']:
            output = self.root / protection
            env = dict(os.environ, VOTE_STUDY_DRY_RUN='1', VOTE_STUDY_SIZES='750',
                       VOTE_STUDY_COMMITTEES='top-stake-seats', VOTE_STUDY_FANOUTS='8',
                       VOTE_STUDY_TRANSPORTS='push', VOTE_STUDY_FANOUT_PROTECTS_PRODUCERS=protection)
            result = subprocess.run([str(RUNNER), str(self.archive / 'study-config.yaml'), str(output)],
                                    env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            with (output / 'runs.csv').open() as stream:
                row, = csv.DictReader(stream)
            names.append(row['run'])
            self.assertEqual(row['protects_producers'], protection)
            self.assertIn(f'vote-push-fanout-protects-producers: {protection}', (output / (row['run'] + '.yaml')).read_text())
            nodes = json.loads((output / 'topology-750.yaml').read_text())['nodes']
            self.assertEqual(sum(link.get('always-forward-votes', False) for n in nodes.values()
                                 for link in n['producers'].values()), 432)
        self.assertNotEqual(*names)

    def test_control_sizes_are_recorded_in_run_identity_and_inputs(self):
        names = []
        for announce, request in [(8, 8), (40, 64)]:
            output = self.root / f'{announce}-{request}'
            env = dict(os.environ, VOTE_STUDY_DRY_RUN='1', VOTE_STUDY_SIZES='750',
                       VOTE_STUDY_COMMITTEES='top-stake-seats', VOTE_STUDY_TRANSPORTS='announce-then-request',
                       VOTE_STUDY_ANNOUNCEMENT_BYTES=str(announce), VOTE_STUDY_REQUEST_BYTES=str(request))
            result = subprocess.run([str(RUNNER), str(self.archive / 'study-config.yaml'), str(output)],
                                    env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            with (output / 'runs.csv').open() as stream:
                row, = csv.DictReader(stream)
            names.append(row['run'])
            self.assertEqual(row['announcement_bytes'], str(announce))
            self.assertEqual(row['request_bytes'], str(request))
            overlay = (output / (row['run'] + '.yaml')).read_text()
            self.assertIn(f'vote-announcement-size-bytes: {announce}', overlay)
            self.assertIn(f'vote-request-size-bytes: {request}', overlay)
        self.assertNotEqual(*names)

    def test_extractor_keeps_different_control_sizes(self):
        row = self.subset()
        rows = []
        for size in ['8', '40', '64']:
            r = dict(row, run=row['run'] + '-a' + size, announcement_bytes=size, request_bytes=size)
            shutil.copyfile(self.root / (row['run'] + '.txt'), self.root / (r['run'] + '.txt'))
            rows.append(r)
        self.write_rows(rows)
        self.manifest('log-sha256.json', [r['run'] + '.txt' for r in rows])
        result = self.extract()
        self.assertEqual(result.returncode, 0, result.stderr)
        results = json.loads((self.root / 'results.json').read_text())['results']
        self.assertEqual({r['announcement_bytes'] for r in results}, {'8', '40', '64'})

    def test_extractor_keeps_both_protection_rules(self):
        row = self.subset()
        rows = []
        for protection in ['false', 'true']:
            r = dict(row, run=row['run'] + '-bp' + protection, protects_producers=protection)
            shutil.copyfile(self.root / (row['run'] + '.txt'), self.root / (r['run'] + '.txt'))
            rows.append(r)
        self.write_rows(rows)
        self.manifest('log-sha256.json', [r['run'] + '.txt' for r in rows])
        result = self.extract()
        self.assertEqual(result.returncode, 0, result.stderr)
        results = json.loads((self.root / 'results.json').read_text())['results']
        self.assertEqual({r['protects_producers'] for r in results}, {'false', 'true'})

    def test_focused_plan_has_ten_distinct_matched_cases(self):
        output = self.root / 'focused'
        env = dict(os.environ, VOTE_STUDY_DRY_RUN='1')
        result = subprocess.run([sys.executable, str(HERE / 'scripts/vote-diffusion-followup.py'),
                                 str(self.archive / 'study-config.yaml'), str(output), '0'],
                                env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        with (output / 'runs.csv').open() as stream:
            rows = list(csv.DictReader(stream))
        self.assertEqual(len(rows), 10)
        self.assertEqual(len({r['run'] for r in rows}), 10)
        self.assertTrue(all((r['nodes'], r['committee'], r['seed']) == ('1500', 'top-stake-seats', '0') for r in rows))
        for cap in ['22', '16', '8']:
            self.assertEqual({r['protects_producers'] for r in rows if r['fanout'] == cap}, {'true', 'false'})
        self.assertEqual({(r['announcement_bytes'], r['request_bytes']) for r in rows if r['transport'] == 'announce-then-request'}, {('8','8'), ('40','40'), ('64','64')})
        self.assertTrue(all(r['status'] == 'planned' for r in rows))
        runner.verify(output, json.loads((output / 'input-sha256.json').read_text()))

    def test_runner_plans_complete_matrix_with_extractor_schema(self):
        output = self.root / 'plan'
        env = {k: v for k, v in os.environ.items() if not k.startswith('VOTE_STUDY_')}
        env['VOTE_STUDY_DRY_RUN'] = '1'
        result = subprocess.run([str(RUNNER), str(self.archive / 'study-config.yaml'),
                                 str(output), '0', '1', '2'], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        with (output / 'runs.csv').open() as stream:
            reader = csv.DictReader(stream)
            self.assertEqual(reader.fieldnames, list(self.rows[0]) + ['protects_producers', 'announcement_bytes', 'request_bytes'])
            rows = list(reader)
        self.assertEqual(len(rows), 108)
        self.assertEqual(len({r['run'] for r in rows}), 108)
        self.assertTrue(all(r['status'] == 'planned' for r in rows))
        for name in ['input-sha256.json', 'log-sha256.json', 'revision.txt', 'upstream-revision.txt']:
            self.assertTrue((output / name).is_file(), name)
        checksums = json.loads((output / 'input-sha256.json').read_text())
        for name, expected in checksums.items():
            self.assertEqual(hashlib.sha256((output / name).read_bytes()).hexdigest(), expected)
        for row in rows:
            overlay = (output / (row['run'] + '.yaml')).read_text()
            self.assertIn(f'vote-transport: "{row["transport"]}"', overlay)
            self.assertIn(f'vote-push-fanout: {"null" if row["fanout"] == "all" else row["fanout"]}', overlay)
            self.assertIn('vote-push-fanout-protects-producers: false', overlay)


if __name__ == '__main__':
    unittest.main()
