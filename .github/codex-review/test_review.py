import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import review

HEAD = 'a' * 40
BASE = 'b' * 40
LAST = 'c' * 40
FILES = [{'filename': 'src/main.rs', 'patch': '@@ -8,2 +8,2 @@\n unchanged\n-old\n+new'}]
RESULT = {'summary': 'One concrete defect found.', 'findings': [{
    'severity': 'medium', 'path': 'src/main.rs', 'line': 9, 'side': 'RIGHT',
    'title': 'Handle a closed channel', 'body': 'When the channel closes, this loop spins indefinitely. Break on closure to avoid consuming a CPU core.',
}]}


class ValidationTests(unittest.TestCase):
    def test_additions_and_deletions(self):
        self.assertEqual(review.changed_lines(FILES[0]['patch']), {('LEFT', 9), ('RIGHT', 9)})
        self.assertEqual(review.validate(RESULT, FILES)[0]['line'], 9)
        deleted = copy.deepcopy(RESULT)
        deleted['findings'][0]['side'] = 'LEFT'
        self.assertEqual(review.validate(deleted, FILES)[0]['side'], 'LEFT')

    def test_invalid_findings_fail_closed(self):
        for key, value in [('severity', 'low'), ('severity', 'P2'), ('path', '../secret'),
                           ('line', 8), ('line', True), ('side', 'UNKNOWN'),
                           ('body', 'P1: repair this'), ('body', 'word ' * 91),
                           ('title', '<!-- codex-review -->')]:
            with self.subTest(key=key, value=value):
                invalid = copy.deepcopy(RESULT)
                invalid['findings'][0][key] = value
                with self.assertRaises(ValueError):
                    review.validate(invalid, FILES)

    def test_empty_findings_and_missing_patch(self):
        self.assertEqual(review.validate({'summary': 'No qualifying findings.', 'findings': []}, []), [])
        with self.assertRaises(ValueError):
            review.validate(RESULT, [{'filename': 'src/main.rs'}])

    def test_only_bot_markers_for_matching_base_count(self):
        def row(user, head, base):
            return {'user': {'login': user}, 'body': f'<!-- codex-review head={head} base={base} -->'}
        rows = [row(review.BOT, LAST, BASE), row('outsider', HEAD, BASE), row(review.BOT, HEAD, LAST)]
        self.assertEqual(review.previous_head(rows, BASE), LAST)


class OrchestrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.env = patch.dict(os.environ, {
            'GITHUB_REPOSITORY': 'owner/repo', 'GITHUB_ACTOR': 'maintainer',
            'GITHUB_TRIGGERING_ACTOR': 'maintainer', 'GITHUB_EVENT_NAME': 'issue_comment',
            'GITHUB_EVENT_PATH': f'{self.temp.name}/event.json',
            'GITHUB_OUTPUT': f'{self.temp.name}/output', 'HAS_KEY': 'true',
            'GITHUB_RUN_ID': '42', 'GITHUB_SERVER_URL': 'https://github.com',
            'PR': '12', 'HEAD': HEAD, 'BASE': BASE, 'REQUESTED': 'false',
            'REVIEW_RESULT': 'success', 'PREPARE_STATE': 'review', 'REVIEW_MODE': 'full',
            'REVIEW_JSON': json.dumps(RESULT),
        })
        self.env.start()
        self.addCleanup(self.env.stop)
        self.pr = {'state': 'open', 'draft': False, 'title': 'Fix', 'body': '',
                   'head': {'sha': HEAD, 'repo': {'full_name': 'owner/repo'}}, 'base': {'sha': BASE}}
        self.event = {'sender': {'type': 'User'}, 'issue': {'number': 12, 'pull_request': {}},
                      'comment': {'id': 23, 'body': '@codex review'}}
        self.event['issue']['pull_request'] = {'url': 'https://api.github.com/repos/owner/repo/pulls/12'}
        self.posts = []
        self.reviews = []
        self.permission = 'write'
        self.mock_api = patch.object(review, 'api', side_effect=self.api)
        self.mock_api.start()
        self.addCleanup(self.mock_api.stop)

    def api(self, endpoint, payload=None, pages=False):
        if payload is not None:
            self.posts.append((endpoint, payload))
            return {}
        if '/permission' in endpoint:
            return {'permission': self.permission}
        if '/reviews?' in endpoint:
            return self.reviews
        if '/files?' in endpoint:
            return FILES
        if '/comments?' in endpoint:
            return []
        return self.pr

    def gate(self):
        Path(os.environ['GITHUB_EVENT_PATH']).write_text(json.dumps(self.event))
        review.gate()

    def test_untrusted_commenter_and_rerun_actor_are_rejected(self):
        self.permission = 'read'
        self.gate()
        self.assertEqual(self.posts, [])
        self.assertFalse(Path(os.environ['GITHUB_OUTPUT']).exists())
        self.permission = 'write'
        with patch.dict(os.environ, GITHUB_TRIGGERING_ACTOR='outsider'), patch.object(
                review, 'api', side_effect=lambda route: {'permission': 'read' if '/outsider/' in route else 'write'}):
            self.gate()
        self.assertFalse(Path(os.environ['GITHUB_OUTPUT']).exists())

    def test_maintainer_can_request_fork_and_draft_review(self):
        self.pr['head']['repo']['full_name'] = 'outsider/fork'
        self.pr['draft'] = True
        self.gate()
        self.assertIn('ready=true', Path(os.environ['GITHUB_OUTPUT']).read_text())
        self.assertEqual(self.posts[-1][1], {'content': 'eyes'})

    def test_reaction_failure_does_not_block_authorized_review(self):
        def deny_reaction(route, payload=None, pages=False):
            if route.endswith('/reactions'):
                raise subprocess.CalledProcessError(1, ['gh', 'api'])
            return self.api(route, payload, pages)
        with patch.object(review, 'api', side_effect=deny_reaction):
            self.gate()
        outputs = Path(os.environ['GITHUB_OUTPUT']).read_text()
        self.assertIn('ready=true', outputs)
        self.assertIn('requested=true', outputs)
        self.assertEqual(self.posts[-1][1]['state'], 'pending')

    def test_automatic_fork_and_draft_reviews_are_skipped(self):
        self.event['pull_request'] = {'number': 12}
        with patch.dict(os.environ, GITHUB_EVENT_NAME='pull_request_target'):
            self.pr['head']['repo']['full_name'] = 'outsider/fork'
            self.gate()
            self.pr['head']['repo']['full_name'] = 'owner/repo'
            self.pr['draft'] = True
            self.gate()
        self.assertEqual(self.posts, [])

    def test_bad_commands_and_bot_comments_are_ignored(self):
        self.event['comment']['body'] = '@codex reviewer'
        self.gate()
        self.event['comment']['body'] = '@codex review'
        self.event['sender']['type'] = 'Bot'
        self.gate()
        self.assertEqual(self.posts, [])

    def test_missing_key_fails_with_setup_message(self):
        with patch.dict(os.environ, HAS_KEY='false'), self.assertRaisesRegex(RuntimeError, 'OPENAI_API_KEY'):
            self.gate()
        self.assertEqual(self.posts, [])

    def test_publish_posts_one_atomic_comment_review(self):
        review.publish()
        posted = [p for route, p in self.posts if route.endswith('/reviews')]
        self.assertEqual(len(posted), 1)
        self.assertEqual(posted[0]['event'], 'COMMENT')
        self.assertEqual(posted[0]['commit_id'], HEAD)
        self.assertIn('**medium — ', posted[0]['comments'][0]['body'])
        self.assertIn('run=42', posted[0]['body'])
        self.assertEqual(self.posts[-1][1]['state'], 'success')

    def test_stale_head_and_base_never_publish(self):
        for side in ['head', 'base']:
            original = self.pr[side]['sha']
            self.pr[side]['sha'] = LAST
            review.publish()
            self.pr[side]['sha'] = original
        self.assertTrue(all('/statuses/' in route for route, _ in self.posts))

    def test_duplicate_run_and_head_are_not_reposted(self):
        for body in [f'<!-- codex-review run=42 -->', f'<!-- codex-review head={HEAD} base={BASE} -->']:
            self.reviews = [{'user': {'login': review.BOT}, 'body': body}]
            review.publish()
        self.assertTrue(all('/statuses/' in route for route, _ in self.posts))

    def test_update_during_validation_never_posts(self):
        calls = 0
        original = self.api
        def moving_api(route, payload=None, pages=False):
            nonlocal calls
            if route.endswith('/pulls/12') and payload is None:
                calls += 1
                if calls == 2:
                    self.pr['head']['sha'] = LAST
            return original(route, payload, pages)
        with patch.object(review, 'api', side_effect=moving_api):
            review.publish()
        self.assertTrue(all('/statuses/' in route for route, _ in self.posts))

    def test_requested_review_bypasses_head_deduplication(self):
        self.reviews = [{'user': {'login': review.BOT}, 'body': f'<!-- codex-review head={HEAD} base={BASE} -->'}]
        with patch.dict(os.environ, REQUESTED='true'):
            review.publish()
        self.assertEqual(sum(route.endswith('/reviews') for route, _ in self.posts), 1)

    def test_failed_or_invalid_review_is_not_a_clean_review(self):
        for changes in [{'REVIEW_RESULT': 'failure'}, {'REVIEW_JSON': '{broken'},
                        {'PREPARE_STATE': ''}, {'REVIEW_JSON': json.dumps({'summary': 'P2', 'findings': []})}]:
            with patch.dict(os.environ, changes), self.assertRaises((RuntimeError, ValueError)):
                review.publish()
            self.assertEqual(self.posts[-1][1]['state'], 'failure')
        self.assertTrue(all('/statuses/' in route for route, _ in self.posts))

    def test_prepare_full_incremental_and_force_push(self):
        original_cwd = Path.cwd()
        os.chdir(self.temp.name)
        self.addCleanup(os.chdir, original_cwd)
        Path('source').mkdir()
        def git(*args):
            return subprocess.check_output(['git', '-C', 'source', *args], text=True).strip()
        git('init', '-q')
        git('config', 'user.name', 'Test')
        git('config', 'user.email', 'test@example.invalid')
        Path('source/main.rs').write_text('base\n')
        git('add', '.')
        git('commit', '-qm', 'base')
        base = git('rev-parse', 'HEAD')
        Path('source/main.rs').write_text('first\n')
        git('commit', '-qam', 'first')
        last = git('rev-parse', 'HEAD')
        Path('source/main.rs').write_text('second\n')
        Path('source/Cargo.lock').write_text('generated\n')
        git('add', '.')
        git('commit', '-qam', 'second')
        head = git('rev-parse', 'HEAD')
        self.pr['head']['sha'], self.pr['base']['sha'] = head, base
        with patch.dict(os.environ, HEAD=head, BASE=base):
            review.prepare()
            self.assertEqual(json.loads(Path('review-input/scope.json').read_text())['mode'], 'full')
            self.assertIn('+second', Path('review-input/pr.diff').read_text())
            self.assertNotIn('Cargo.lock', Path('review-input/pr.diff').read_text())
            self.reviews = [{'user': {'login': review.BOT}, 'body': f'<!-- codex-review head={last} base={base} -->'}]
            review.prepare()
            self.assertEqual(json.loads(Path('review-input/scope.json').read_text())['mode'], 'incremental')
            delta = Path('review-input/new-commits.patch').read_text()
            self.assertIn('-first', delta)
            self.assertNotIn('-base', delta)
            # A requested review always covers the whole PR.
            with patch.dict(os.environ, REQUESTED='true'):
                review.prepare()
                self.assertEqual(json.loads(Path('review-input/scope.json').read_text())['mode'], 'full')
            # A prior head not in the current history forces a full review.
            self.reviews[0]['body'] = f'<!-- codex-review head={LAST} base={base} -->'
            review.prepare()
            self.assertEqual(json.loads(Path('review-input/scope.json').read_text())['mode'], 'full')


if __name__ == '__main__':
    unittest.main()
