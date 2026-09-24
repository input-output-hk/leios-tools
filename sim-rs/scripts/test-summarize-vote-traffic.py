#!/usr/bin/env python3
import importlib.util
from pathlib import Path
import unittest
import sys
sys.dont_write_bytecode = True

spec = importlib.util.spec_from_file_location('traffic', Path(__file__).with_name('summarize-vote-traffic.py'))
traffic = importlib.util.module_from_spec(spec)
spec.loader.exec_module(traffic)


class TrafficTests(unittest.TestCase):
    def report(self):
        def totals(body, announce, request):
            return {kind: {'messages': count, 'bytes': count*size} for kind, count, size in
                    [('bodies', body, 94), ('announcements', announce, 8), ('requests', request, 8)]}
        return {'format_version': 1, 'window_seconds': 1, 'observed_until_s': 2, 'requested_slots': 4,
                'nodes': [{'id': 0, 'name': 'bp', 'stake': 1000, 'sent': totals(2, 1, 0),
                           'received': totals(0, 0, 1), 'seconds': [[102, 0], [94, 8]]},
                          {'id': 1, 'name': 'relay', 'stake': 0, 'sent': totals(0, 0, 1),
                           'received': totals(2, 1, 0), 'seconds': [[0, 0], [8, 196]]}]}

    def test_units_peaks_and_global_reconciliation(self):
        rows = traffic.summarize(self.report(), 4)
        self.assertEqual(rows[0]['sent_bytes'], 196)
        self.assertEqual(rows[0]['sent_mean_mbit_s'], 196*8/1e6/4)
        self.assertEqual(rows[0]['sent_peak_1s_mbit_s'], 102*8/1e6)
        self.assertEqual(rows[1]['received_peak_window_start_s'], 1)
        log = 'Final protocol stats:\nFinal network stats:\n2 Vote body message(s) were sent. 2 of them were received\nVote mini-protocol traffic sent: 4 message(s), 0.00 MB = 2 bodies (0.00 MB) + 1 announcement(s) (0.00 MB) + 1 request(s) (0.00 MB).\nSimulation completed: 4 slots.'
        traffic.reconcile(rows, log, 4)
        with self.assertRaises(ValueError):
            traffic.reconcile(rows, log.replace('2 bodies', '3 bodies'), 4)

    def test_inflated_or_nonfinite_duration_is_rejected(self):
        for duration in [3, 5, float('inf'), float('nan')]:
            with self.subTest(duration=duration), self.assertRaises(ValueError):
                traffic.summarize(self.report(), duration)

    def test_periodic_or_interrupted_summary_is_rejected(self):
        rows = traffic.summarize(self.report(), 4)
        for log in ['Vote mini-protocol traffic sent: 0 message(s)',
                    'Final protocol stats:\nFinal network stats:',
                    'Final protocol stats:\nFinal network stats:\nSimulation completed: 3 slots.']:
            with self.subTest(log=log), self.assertRaises(ValueError):
                traffic.reconcile(rows, log, 4)

    def test_missing_bucket_bytes_fail_instead_of_understating_peak(self):
        report = self.report()
        report['nodes'][1]['seconds'] = []
        with self.assertRaises(ValueError):
            traffic.summarize(report, 4)


if __name__ == '__main__':
    unittest.main()
