-- 009: Add exit_attempts column to positions table.
--
-- Without this column, every exit PATCH that includes exit_attempts
-- causes PostgREST to return HTTP 400 (unknown column), which silently
-- drops the ENTIRE update — status, pnl_sol, pnl_pct, exit_price, etc.
-- This was the root cause of positions stuck as "open" when already
-- closed on-chain.
--
-- Run in Supabase SQL Editor.

ALTER TABLE positions
ADD COLUMN IF NOT EXISTS exit_attempts INTEGER;
