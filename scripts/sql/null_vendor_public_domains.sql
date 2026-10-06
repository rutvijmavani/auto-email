-- Issue 3 one-time cleanup (2026-10-06): vendor/email roots stored as fein_domain_map.public_domain.
-- Run on the VM with psql. Step 1 is a read-only preview; step 2 changes data and ends in ROLLBACK
-- until you replace it with COMMIT after checking the counts.
--
-- Keepers (NOT touched): the vendors themselves (Akamai, Cloudflare, Fastly, Microsoft, Mimecast, Proofpoint,
-- Google LLC, AT&T Mobility/Services, Cricket Wireless), Rafter/Docmation (name-gate review decides),
-- Forged Fiber 37 LLC (att.com plausible).
-- Nulled rows also get last_enriched_at = NULL so fuzzy_match's _populate_enrichment_queue re-queues them
-- (only those at/above STALENESS_DISCOVERY_MIN_PETITIONS); the vendor guard in
-- discover_public_domain_gated stops the same value coming back.

-- Step 1: preview
CREATE TEMP TABLE _vendor_pd AS
SELECT f.employer_fein,
       (SELECT MIN(e.employer_name) FROM dol_h1b_employers e WHERE e.employer_fein = f.employer_fein) AS employer_name,
       f.assigned_domain, f.public_domain, f.public_domain_method
FROM fein_domain_map f
WHERE f.public_domain IN (
        'cloudflaressl.com', 'icloud.com', 'google.com', 'business.site', 'myworkday.com', 'msn.com',
        'att.net', 'att.com', 'googlemail.com', 'aol.com', 'live.com', 'comcast.net', 'verizon.net',
        'sbcglobal.net', 'outlook.com', 'hotmail.com', 'gmail.com', 'yahoo.com', 'office365.com')
  -- A keeper is exempt only on the root it legitimately owns (name regex paired with that root), so e.g.
  -- "Microsoft Partner Services LLC" on icloud.com is still nulled. Vendors on their own roots
  -- (cloudflare.com, microsoft.com, akamai.com ...) are not in the root list above, so they are never selected.
  AND NOT EXISTS (
        SELECT 1
        FROM dol_h1b_employers e
        JOIN (VALUES ('^google( llc| inc\.?)?$',                  'google.com'),
                     ('at&t|cricket wireless|forged fiber',       'att.com'),
                     ('rafter|docmation',                         'rafter.one')) AS k(name_rx, root)
          ON e.employer_name ~* k.name_rx
        WHERE e.employer_fein = f.employer_fein
          AND f.public_domain = k.root);

SELECT public_domain, public_domain_method, COUNT(*) FROM _vendor_pd GROUP BY 1, 2 ORDER BY 3 DESC;
SELECT * FROM _vendor_pd ORDER BY public_domain, employer_name;

-- Step 2: apply (inspect the preview first; expected about 43 rows)
BEGIN;
UPDATE fein_domain_map f
SET public_domain = NULL,
    public_domain_method = NULL,
    public_domain_host = NULL,
    last_enriched_at = NULL
FROM _vendor_pd v
WHERE f.employer_fein = v.employer_fein
  AND f.public_domain = v.public_domain;  -- only rows still unchanged since the preview
-- Change to COMMIT once the row count above matches the preview.
ROLLBACK;
