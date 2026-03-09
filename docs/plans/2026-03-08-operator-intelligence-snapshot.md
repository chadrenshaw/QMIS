## Objective

Implement an operator-facing intelligence layer on top of the existing DuckDB dashboard snapshot so the CLI emphasizes interpreted state, dominant forces, relationship shifts, and actionable watch items instead of raw statistical tables.

## Constraints

- Keep the existing collectors, analysis pipeline, and schema unchanged.
- Build on top of `load_dashboard_snapshot()` data already assembled in `src/qmis/dashboard/cli.py`.
- Preserve Rich terminal rendering and existing operator console compatibility.
- Add tests first for the interpretation layer and the new CLI sections.

## Plan

1. Add failing tests for the new interpreter behaviors.
   - Create focused tests for world-state interpretation, force aggregation, relationship-break grouping, risk indicator generation, and watchlist output.
   - Update dashboard render tests to assert the new section titles and filtered significant-correlation behavior.

2. Implement `src/qmis/signals/interpreter.py`.
   - Build helpers that consume the dashboard snapshot structure.
   - Interpret astronomy/natural context into concise world-state fields.
   - Aggregate strong stable correlations into thematic market forces.
   - Collapse `relationship_break` anomalies into higher-level summaries.
   - Translate raw scores/signals into operator-friendly risk indicators.
   - Generate a short prioritized watchlist.

3. Extend `src/qmis/dashboard/cli.py`.
   - Integrate the interpreter output into `load_dashboard_snapshot()`.
   - Add renderers for world snapshot, market forces, relationship changes, risk indicators, significant correlations, experimental signals, and watchlist.
   - Keep the regime panel and supporting detail sections coherent with the new snapshot-first layout.

4. Verify and refine.
   - Run targeted unit tests for interpreter/dashboard changes first.
   - Run the full test suite after the new CLI renders cleanly.
   - Adjust copy/layout so the output stays concise and readable in the terminal.
