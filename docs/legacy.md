# Legacy code register

Training Companion has one maintained code source: this trisplit repository.
New collection, coaching, reports and notification features belong here.

| Former component | Maintained replacement | Preserved legacy |
| --- | --- | --- |
| fingu's independent swim collection and analysis | Shared lakehouse inputs and `apps/coach` | Original fingu repository and history; private DB backups outside Git |
| Standalone Python overview generation in the personal workspace | `scripts/build_report.py`, `scripts/build_html.py` | Original personal `reports/_scripts/` files, for historical reference only |
| Strava-era webhook/OAuth paths | intervals.icu collector feeding the shared pipeline | Existing Strava source paths and original fingu repository |

This register freezes the **development source**, not the currently running
sender. Until the [cutover runbook](consolidation.md) is completed, old fingu may
still be running. Do not start another sender or archive/delete a live deploy
based on this file alone. The private workspace's STATUS.md records the actual
operational state.

The personal training-companion workspace, its notes/templates and current
health/equipment records remain active. The pipeline in this repository is
also active; its functionality is extended rather than retired.

The initial swim rest algorithm and starting Node dependency lock were imported from
fingu, whose source revision at consolidation was
`8e20f6195b479dae146ce131b93cbd8e7455f790`. The original repository retains the
complete Git history. No personal DB, tracked node_modules or old credentials
were imported into this repository.
