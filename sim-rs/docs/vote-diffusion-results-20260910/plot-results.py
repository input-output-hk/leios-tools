#!/usr/bin/env python3
"""Render study figures from results.json; no simulation or network access."""
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

ROOT = Path(__file__).resolve().parent
OUT = ROOT / 'figures'
SOURCE = ROOT / 'results.json'
DATA = json.loads(SOURCE.read_text())
ROWS = DATA['results']
FANOUTS = ['all', '22', '16', '8']
COMMITTEES = ['top-stake-seats', 'everyone']
INK, MUTED, GRID = '#203247', '#627286', '#e0e6ec'
BLUE, TEAL, AMBER, GRAY = '#4169b1', '#008778', '#b56b12', '#69798f'
assert DATA['completed'] == len(ROWS) == 108 and not DATA['parse_errors']
assert len({r['run'] for r in ROWS}) == 108

plt.rcParams.update({
    'font.family': 'DejaVu Sans', 'font.size': 11.5,
    'text.color': INK, 'axes.labelcolor': INK, 'xtick.color': MUTED,
    'ytick.color': MUTED, 'axes.edgecolor': GRID,
    'axes.spines.top': False, 'axes.spines.right': False,
    'axes.spines.left': False, 'axes.spines.bottom': False,
    'axes.titleweight': 'bold', 'axes.titlesize': 13,
    'axes.labelsize': 11, 'axes.axisbelow': True,
    'svg.fonttype': 'path', 'svg.hashsalt': 'leios-vote-study-20260910',
    'savefig.facecolor': 'white', 'figure.facecolor': 'white',
})

def group(nodes, committee, transport, fanout='all'):
    rows = sorted((r for r in ROWS if (r['nodes'], r['committee'], r['transport'], r['fanout'])
                   == (nodes, committee, transport, fanout)), key=lambda r: r['seed'])
    assert [r['seed'] for r in rows] == [0, 1, 2]
    return rows


def aggregate(rows):
    total = sum(r['ebs_generated'] for r in rows)
    accepted = sum(r['accepted'] for r in rows)
    return {
        'ebs': total,
        'q50': sum(r['quorum_median']['by_inclusion'] for r in rows),
        'q95': sum(r['quorum_p95']['by_inclusion'] for r in rows),
        'verifications_per_accepted': sum(r['verifications'] for r in rows) / accepted,
        'verifications': sum(r['verifications'] for r in rows),
        'l1_endorsements': sum(r['l1_endorsements'] for r in rows),
        'wire_gb': sum(r['wire_mb_rounded'] for r in rows) / 1000,
    }


def heading(fig, title, subtitle):
    fig.text(.06, .955, title, fontsize=19, weight='bold', va='top')
    fig.text(.06, .911, subtitle, fontsize=11.2, color=MUTED, va='top')


def grid(ax, axis='y'):
    ax.grid(axis=axis, color=GRID, linewidth=.7)
    ax.tick_params(length=0, pad=7)


def save(fig, name):
    OUT.mkdir(exist_ok=True)
    fig.savefig(OUT / (name + '.png'), dpi=180, metadata={'Software': 'Matplotlib; plot-results.py'})
    plt.close(fig)


def fanout_overview():
    fig, axes = plt.subplots(2, 2, figsize=(11.2, 8.0), sharex=True)
    fig.subplots_adjust(left=.10, right=.96, top=.80, bottom=.20, hspace=.34, wspace=.22)
    heading(fig, 'Less duplicate verification, weaker quorum availability',
            '1500 nodes · push with duplicate checks after verification · three matched seeds')
    titles = ['Stake-weighted reference\n458 eligible voters', 'Everyone-votes stress test\n1500 eligible voters']
    for column, committee in enumerate(COMMITTEES):
        top, bottom = axes[:, column]
        top.set_title(titles[column], loc='left', pad=19, linespacing=1.5)
        groups = [group(1500, committee, 'push-late-dedupe', f) for f in FANOUTS]
        totals = [aggregate(g) for g in groups]
        assert all(a['ebs'] == 72 for a in totals)
        ratios = [a['verifications_per_accepted'] for a in totals]
        top.plot(range(4), ratios, color=AMBER, lw=2, marker='o', ms=7, zorder=3)
        for x, (g, a) in enumerate(zip(groups, totals)):
            for offset, r in zip([-.11, 0, .11], g):
                top.scatter(x + offset, r['verifications'] / r['accepted'], s=22,
                            facecolors='white', edgecolors=AMBER, linewidths=1, zorder=4)
            top.annotate(f"{a['verifications_per_accepted']:.2f}×", (x, ratios[x]),
                         xytext=(0, 12), textcoords='offset points', ha='center',
                         color=AMBER, weight='bold', fontsize=11)
        top.set_ylim(0, 14)
        top.set_yticks([0, 4, 8, 12])
        for key, color, marker, offset in [('q50', BLUE, 's', -.025), ('q95', TEAL, 'o', .025)]:
            percentages = [100 * a[key] / a['ebs'] for a in totals]
            xs = [x + offset for x in range(4)]
            bottom.plot(xs, percentages, color=color, lw=2, marker=marker, ms=7,
                        markerfacecolor='white' if key == 'q50' else color, zorder=3)
            field = 'quorum_median' if key == 'q50' else 'quorum_p95'
            for x, g in enumerate(groups):
                for jitter, r in zip([-.10, 0, .10], g):
                    bottom.scatter(x + jitter + offset, 100 * r[field]['by_inclusion'] / r['ebs_generated'],
                                   s=18, facecolors='white', edgecolors=color, marker=marker,
                                   linewidths=.8, zorder=4)
        for x, a in enumerate(totals):
            if a['q50'] == a['q95']:
                label = f"{a['q95']}/72 both"
                y = 100 * a['q95'] / 72
                bottom.annotate(label, (x, y), xytext=(0, 14), textcoords='offset points',
                                ha='center', fontsize=10.5, weight='bold', color=INK)
            else:
                bottom.annotate(f"{a['q50']}/72", (x, 100 * a['q50'] / 72), xytext=(0, 14),
                                textcoords='offset points', ha='center', color=BLUE, weight='bold', fontsize=11)
                bottom.annotate(f"{a['q95']}/72", (x, 0), xytext=(0, 10), textcoords='offset points',
                                ha='center', color=TEAL, weight='bold', fontsize=11)
        bottom.set_ylim(-7, 106)
        bottom.set_yticks([0, 25, 50, 75, 100], labels=['0%', '25%', '50%', '75%', '100%'])
        bottom.set_xticks(range(4), labels=['All peers', '22', '16', '8'])
        bottom.set_xlabel('Maximum peers per vote', labelpad=9)
        for ax in [top, bottom]:
            ax.set_xlim(-.4, 3.4)
            ax.axvspan(.72, 1.28, color='#fff3df', zorder=0)
            grid(ax)
    axes[0, 0].set_ylabel('Completed verifications\nper accepted arrival', labelpad=12)
    axes[1, 0].set_ylabel('EBs attaining observer quorum\nby the 14s inclusion boundary', labelpad=12)
    fig.legend(handles=[Line2D([0], [0], color=BLUE, marker='s', markerfacecolor='white', lw=2,
                              label='Q50: nodes holding 50% of stake'),
                        Line2D([0], [0], color=TEAL, marker='o', lw=2,
                              label='Q95: nodes holding 95% of stake')],
               loc='lower center', bbox_to_anchor=(.53, .066), ncol=2, frameon=False, fontsize=10.5)
    fig.text(.06, .041, 'Small hollow marks: individual seeds. Lines and labels: pooled totals; all 72 generated EBs remain in the denominator.',
             fontsize=9.2, color=MUTED)
    fig.text(.06, .019, 'Q50/Q95 measure where an EB quorum is available, not how much voting stake a certificate requires. Lines join tested caps only.',
             fontsize=9.2, color=MUTED)
    save(fig, 'fanout-overview')


def transport_comparison():
    fig, axes = plt.subplots(1, 2, figsize=(11.2, 6.4), sharey=True)
    fig.subplots_adjust(left=.27, right=.96, top=.78, bottom=.28, wspace=.27)
    heading(fig, 'Push saves time, at about eight times the vote traffic',
            'Unrestricted diffusion · push suppresses duplicates before verification · three matched seeds')
    labels = []
    for row, (nodes, committee) in enumerate([(750, 'top-stake-seats'), (750, 'everyone'),
                                             (1500, 'top-stake-seats'), (1500, 'everyone')]):
        a, b = group(nodes, committee, 'announce-then-request'), group(nodes, committee, 'push')
        summary = aggregate(a)
        assert summary['q95'] == aggregate(b)['q95']
        assert summary['l1_endorsements'] == aggregate(b)['l1_endorsements']
        committee_label = 'Stake-weighted' if committee == 'top-stake-seats' else 'Everyone votes'
        labels.append(f"{nodes} nodes · {committee_label}\nQ95: {summary['q95']}/{summary['ebs']} EBs in both arms")
        y = 3 - row
        for offset, baseline, push in zip([-.15, 0, .15], a, b):
            for ax, x1, x2 in [(axes[0], baseline['quorum_p95']['mean_s'], push['quorum_p95']['mean_s']),
                                (axes[1], 1, push['wire_mb_rounded'] / baseline['wire_mb_rounded'])]:
                ax.plot([x1, x2], [y + offset] * 2, color='#ccd6df', lw=1.1, zorder=1)
                ax.scatter(x1, y + offset, s=35, color=GRAY, marker='s', zorder=3)
                ax.scatter(x2, y + offset, s=35, color=TEAL, marker='o', zorder=3)
    axes[0].set_yticks([3, 2, 1, 0], labels=labels)
    axes[0].tick_params(axis='y', labelsize=10.5, pad=13)
    axes[0].set_xlim(3.0, 4.17)
    axes[0].set_xticks([3.0, 3.25, 3.5, 3.75, 4.0])
    axes[0].set_title('Quorum time', loc='left', pad=15)
    axes[0].set_xlabel('Mean Q95 time from t0 (seconds)', labelpad=12)
    axes[1].set_xlim(.3, 8.8)
    axes[1].set_xticks([1, 2, 4, 6, 8], labels=['1×', '2×', '4×', '6×', '8×'])
    axes[1].set_title('Vote mini-protocol traffic', loc='left', pad=15)
    axes[1].set_xlabel('Bytes relative to the paired baseline', labelpad=12)
    for ax in axes:
        ax.set_ylim(-.55, 3.55)
        grid(ax, 'x')
    fig.legend(handles=[Line2D([0], [0], marker='s', color=GRAY, lw=0, label='Announce / request'),
                        Line2D([0], [0], marker='o', color=TEAL, lw=0, label='Push · early deduplication')],
               loc='lower center', bbox_to_anchor=(.53, .115), ncol=2, frameon=False, fontsize=11)
    fig.text(.06, .068, 'Each thin segment joins the same seed. Timing means include only EBs reaching Q95; labels retain the missed EBs.',
             fontsize=9.5, color=MUTED)
    fig.text(.06, .032, 'All attained Q95 quorums were before the 7s voting deadline. Equal counts do not establish identical EB identities.',
             fontsize=9.5, color=MUTED)
    save(fig, 'transport-comparison')


def endorsement_comparison():
    fig, axes = plt.subplots(1, 2, figsize=(11.2, 6.1), sharey=True)
    fig.subplots_adjust(left=.085, right=.965, top=.75, bottom=.29, wspace=.18)
    heading(fig, 'Endorsement counts expose the cost of lower fanout',
            '1500 nodes · actual L1 blocks carrying an endorsement · totals across three matched seeds')
    for column, committee in enumerate(COMMITTEES):
        ax = axes[column]
        ax.set_title('Stake-weighted reference · 458 voters' if column == 0 else 'Everyone-votes stress · 1500 voters',
                     loc='left', pad=15, fontsize=12)
        ax.axvspan(.6, 1.4, color='#fff3df', zorder=0)
        for transport, color, offset in [('push', TEAL, -.19), ('push-late-dedupe', AMBER, .19)]:
            values = [aggregate(group(1500, committee, transport, f))['l1_endorsements'] for f in FANOUTS]
            xs = [x + offset for x in range(4)]
            bars = ax.bar(xs, values, width=.34, color=color, zorder=3)
            ax.bar_label(bars, labels=[str(v) for v in values], padding=6, color=color, weight='bold', fontsize=12)
        ax.set_ylim(0, 30)
        ax.set_yticks([0, 5, 10, 15, 20, 25, 30])
        ax.set_xticks(range(4), labels=['All peers', '22', '16', '8'])
        ax.set_xlabel('Maximum peers per vote', labelpad=12)
        grid(ax)
    axes[0].set_ylabel('L1 endorsements across three seeds', labelpad=12)
    fig.legend(handles=[Patch(facecolor=TEAL, label='Duplicate checks before verification'),
                        Patch(facecolor=AMBER, label='Duplicate checks after verification')],
               loc='lower center', bbox_to_anchor=(.53, .105), ncol=2, frameon=False, fontsize=10.5)
    fig.text(.06, .064, 'These are endorsement counts, not the number of EBs with a quorum somewhere. Both columns contain 72 generated EBs.',
             fontsize=9.5, color=MUTED)
    fig.text(.06, .028, 'Per-seed results remain in results.json. The committees differ in both voting weight and vote volume.', fontsize=9.5, color=MUTED)
    save(fig, 'endorsement-comparison')


if __name__ == '__main__':
    fanout_overview()
    transport_comparison()
    endorsement_comparison()
    print('Rendered three PNG figures from results.json.')
