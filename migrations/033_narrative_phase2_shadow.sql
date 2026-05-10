-- ═══════════════════════════════════════════════════════════════════
--  Migration 033: Narrative-cluster Phase 2 shadow marker
--  Marks the wider score>=70 / gap<=300 / liq<=85 narrative profile.
--  This remains observe-only; live execution is still controlled by the
--  strict narrative_cluster_live_canary profile.
-- ═══════════════════════════════════════════════════════════════════

ALTER TABLE narrative_cluster_shadow
    ADD COLUMN IF NOT EXISTS phase2_shadow_passed BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS phase2_shadow_profile TEXT,
    ADD COLUMN IF NOT EXISTS phase2_shadow_checked_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_ncs_phase2_shadow_passed
    ON narrative_cluster_shadow(phase2_shadow_passed, created_at DESC);

COMMENT ON COLUMN narrative_cluster_shadow.phase2_shadow_passed IS
  'True when the wider v18.9 narrative Phase 2 shadow profile matched. Observe-only; never live execution.';

COMMENT ON COLUMN narrative_cluster_shadow.phase2_shadow_profile IS
  'Human-readable profile key for the Phase 2 narrative shadow marker.';
