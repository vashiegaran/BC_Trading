-- ═══════════════════════════════════════════════════════════════════
--  Add narrative_result JSONB to positions and moonbag_positions
--  Stores the full OpenAI narrative check output as JSON.
--  Run once in the Supabase SQL editor.
-- ═══════════════════════════════════════════════════════════════════

-- ── positions: store latest narrative check output ────────────────
ALTER TABLE positions
    ADD COLUMN IF NOT EXISTS narrative_result JSONB;

COMMENT ON COLUMN positions.narrative_result IS
    'Latest OpenAI narrative check output (score, state, reasons, risk_flags, etc.)';

-- ── moonbag_positions: store narrative check at promotion time ────
ALTER TABLE moonbag_positions
    ADD COLUMN IF NOT EXISTS narrative_result JSONB;

COMMENT ON COLUMN moonbag_positions.narrative_result IS
    'OpenAI narrative check output — updated on each re-check during moonbag tracking';
