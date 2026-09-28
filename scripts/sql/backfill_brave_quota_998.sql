-- scripts/sql/backfill_brave_quota_998.sql
--
-- One-time correction for the Brave monthly quota undercount that predates
-- the external_api_health migration (commit 0ad2351, 2026-09-22).
--
-- Root cause: before 0ad2351, Brave usage was tracked in an unlocked local
-- JSON file (data/brave_quota.json) that raced across processes and was
-- already known to be behind reality at migration time (commit message:
-- real Brave dashboard showed 998 calls this month vs. ~719 the local
-- counter showed). 0ad2351 switched the quota gate to
-- get_month_request_count("brave"), which sums external_api_health rows —
-- but that table only has rows for calls recorded by
-- record_external_request() going forward from when instrumentation
-- landed, so it never inherited the pre-migration usage. Result: the
-- >= _BRAVE_QUOTA_LIMIT (950) gate compared against an undercounted total,
-- kept letting calls through, and every one hit a real HTTP 402 Payment
-- Required from Brave's own server (account genuinely out of quota).
--
-- This is a one-time DATA correction, not a code change: it tops up
-- today's external_api_health row for service='brave' with just enough to
-- make SUM(requests_made) across the current calendar month equal the real
-- Brave dashboard count (998, confirmed by the user 2026-09-22). Delta is
-- computed fresh each run and is a no-op if already caught up, so this is
-- safe to re-run (e.g. if the dashboard count needs bumping again later
-- this month) without double-counting.
--
-- Recorded into requests_other_err rather than requests_ok — these are
-- untyped historical calls being backfilled purely for quota-counting
-- purposes, not real observed 200s, so they don't inflate the "ok rate" in
-- pipeline_metrics.py's health report.
--
-- Run manually against the target DB (VM production, or local for testing):
--   psql "$DATABASE_URL" -f scripts/sql/backfill_brave_quota_998.sql
--
-- Update v_target below if the real dashboard count has moved by the time
-- this is actually run.

DO $$
DECLARE
    v_target  INTEGER := 998;   -- real Brave dashboard count, month-to-date
    v_already INTEGER;
    v_delta   INTEGER;
    v_today   DATE := CURRENT_DATE;
BEGIN
    SELECT COALESCE(SUM(requests_made), 0) INTO v_already
    FROM external_api_health
    WHERE service = 'brave'
      AND date >= date_trunc('month', v_today)::date
      AND date <  (date_trunc('month', v_today) + INTERVAL '1 month')::date;

    v_delta := v_target - v_already;

    IF v_delta <= 0 THEN
        RAISE NOTICE 'external_api_health already at or above % for brave this month (currently %) — no correction needed', v_target, v_already;
        RETURN;
    END IF;

    INSERT INTO external_api_health (date, service)
    VALUES (v_today, 'brave')
    ON CONFLICT (date, service) DO NOTHING;

    UPDATE external_api_health SET
        requests_made      = requests_made      + v_delta,
        requests_other_err = requests_other_err + v_delta
    WHERE date = v_today AND service = 'brave';

    -- Keep avg_response_ms consistent with the same formula
    -- record_external_request() uses (total_ms unchanged by this backfill,
    -- since these aren't real timed calls — avg will dip slightly, which is
    -- expected and harmless for a one-time correction).
    UPDATE external_api_health SET
        avg_response_ms = CASE
            WHEN requests_made > 0 THEN total_ms / requests_made
            ELSE 0
        END
    WHERE date = v_today AND service = 'brave';

    RAISE NOTICE 'external_api_health: backfilled % missing brave requests into % (was % this month, now %)',
        v_delta, v_today, v_already, v_target;
END $$;
