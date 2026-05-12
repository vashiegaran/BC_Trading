-- ═══════════════════════════════════════════════════════════════════
--  Migration 036: Reduced static-gate shadow lane metadata
--  No live execution path reads this. The Rust detector writes qualifying
--  counterfactual graduation entries into bc_paper_trades with
--  entry_trigger='reduced_static_gate_shadow'. Existing price tracking patches
--  all rows by mint, so this lane gets the same outcome columns as other BC
--  paper lanes without extra price requests.
-- ═══════════════════════════════════════════════════════════════════

CREATE INDEX IF NOT EXISTS idx_bc_paper_trades_reduced_static_gate_shadow
    ON bc_paper_trades(created_at DESC)
    WHERE entry_trigger = 'reduced_static_gate_shadow';

COMMENT ON INDEX idx_bc_paper_trades_reduced_static_gate_shadow IS
    'Fast analysis slice for v18.9.4 reduced static-gate shadow entries.';

COMMENT ON COLUMN bc_paper_trades.entry_trigger IS
    'Which signal fired this row: volume_50sol, progress_60pct, progress_75pct, progress_90pct, graduation_raw, graduation_goplus, label/probe shadows, or reduced_static_gate_shadow.';
