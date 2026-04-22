-- Add sniper_score and filter_name columns to sniper_candidates for easier querying.
-- sniper_score: computed score at filter time (previously only in sniper_features JSONB).
-- filter_name: which hard filter rejected the token (null if passed).

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'sniper_candidates' AND column_name = 'sniper_score'
    ) THEN
        ALTER TABLE sniper_candidates ADD COLUMN sniper_score DOUBLE PRECISION;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'sniper_candidates' AND column_name = 'filter_name'
    ) THEN
        ALTER TABLE sniper_candidates ADD COLUMN filter_name TEXT;
    END IF;
END $$;
