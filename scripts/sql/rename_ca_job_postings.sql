-- One-time cleanup: job_postings rows saved by the 07:00 monitor fallback under
-- the tracking key 'ca:{id}' instead of the real company name (fixed in code
-- 2026-10-11).  Run on the VM AFTER deploying the fix.

-- 1) Preview: what exists
SELECT company, status, count(*) FROM job_postings
WHERE company LIKE 'ca:%' GROUP BY company, status ORDER BY company, status;

-- 2) Rename (skips rows whose job_url already exists under the real name, so a
--    duplicate written by the workers never violates a unique constraint)
BEGIN;
UPDATE job_postings jp
SET company = ca.company_name
FROM company_ats ca
WHERE jp.company = 'ca:' || ca.id::text
  AND ca.company_name IS NOT NULL AND ca.company_name <> ''
  AND NOT EXISTS (
      SELECT 1 FROM job_postings x
      WHERE x.company = ca.company_name AND x.job_url = jp.job_url
  );

-- 3) Anything left (duplicates of worker-saved rows) — review, then delete if wanted
SELECT company, status, count(*) FROM job_postings
WHERE company LIKE 'ca:%' GROUP BY company, status ORDER BY company, status;
COMMIT;
