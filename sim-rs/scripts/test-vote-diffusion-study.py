#!/usr/bin/env python3
"""Regression checks for the runner/extractor interface; uses the archived logs."""
import csv
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

    def test_runner_plans_complete_matrix_with_extractor_schema(self):
        output = self.root / 'plan'
        env = {k: v for k, v in os.environ.items() if not k.startswith('VOTE_STUDY_')}
        env['VOTE_STUDY_DRY_RUN'] = '1'
        result = subprocess.run([str(RUNNER), str(self.archive / 'study-config.yaml'),
                                 str(output), '0', '1', '2'], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        with (output / 'runs.csv').open() as stream:
            reader = csv.DictReader(stream)
            self.assertEqual(reader.fieldnames, list(self.rows[0]))
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
            self.assertIn('vote-push-fanout-protects-producers: true', overlay)


if __name__ == '__main__':
    unittest.main()
