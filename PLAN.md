# PLAN — recall-blast-radius

**Tier:** Heavy (new product, Cinderhaven extension, regulatory domain)
**Status:** Phases 1–7 complete (recall.lailarallc.com live, SSL verified 2026-06-12; Dagster integration deferred). Social/marketing posts handled outside CC.

---

## Current Focus

Phase 7 complete. **2026-07-18:** post-launch maintenance arc done — security hardening (env-only DB config; leaked-but-dead credential removed from source/history), platform schema migration (`sku_id→sku`, `retailer_name→name`; reseed + dbt + cache), and redeploy. All verified live. No pending CC work on recall itself.

Parked (needs explicit go-ahead — shared infra): cinderhaven-db's own `pg`-check / flex monitor credential state.

---

## Phases

### Phase 0 — Research ✅ DONE (2026-06-10)
- [x] FSMA 204 date: **July 20, 2028** (FR Doc. 2025-14967 + Continuing Appropriations Act of 2026)
- [x] Retailer mandates: Walmart all-food Aug 1 2025 (past); Kroger GS1 all-food (past)
- [x] Recall cost: **$10M+** (2010 Deloitte/GMA/FMI, conservative floor)
- [x] FTL coverage: hot sauces + spice blends NOT covered; upstream fresh ingredient caveat is the credibility marker
- [x] Trace direction: **forward only** in v1
- [x] Gate checklist: TBD (not blocking build)
- [x] Lot picker: **preset scenarios + free picker**
- [x] Genealogy seed: **400** (42/200/300 taken)

### Phase 1 — Genealogy data model ✅ DONE (2026-06-10)
- [x] Design lot genealogy schema — `data/schema/genealogy_ddl.sql`
- [x] Lot-code format realism — 3 co-packer formats (julian_line, sequential_date, yearweek_seq)
- [x] Recursive CTE traversal — `data/schema/trace_forward.sql`
- [x] Seed data generator — `pipeline/generate_genealogy.py` (seed=400)
- [x] dbt staging models — stg_ingredient_lots, stg_production_batches, stg_fg_lots
- [x] dbt genealogy mart — fct_blast_radius, fct_blast_radius_scope
- [x] dbt: sources.yml with tests (not-null, unique, referential integrity, accepted_values)
- [x] Dagster: genealogy_seed asset (loads all tables) + genealogy_graph stub
- [x] Register seed=400 in CINDERHAVEN_CANONICAL.md — **TODO: do this in cinderhaven-data-platform**
- [x] Run end-to-end: docker compose up → generate_genealogy → dbt run → verify counts

### Phase 2 — Graph construction ✅ DONE (2026-06-10)
- [x] Python: build NetworkX graph from genealogy mart
- [x] Implement trace-forward traversal (lot → cases in channel)
- [x] Implement scope computation (units, sold-through estimate, cost model)
- [x] Generate three preset scenario graphs (A: single lot, B: shared ingredient, C: packaging lot)
- [x] Aggregation strategy for large graphs (collapse shipments → direct fg_lot→retailer edges >60 nodes)
- [ ] Node aggregation / collapsible levels for D3 — deferred to Phase 4 (frontend)

### Phase 3 — FastAPI traversal API ✅ DONE (2026-06-10)
- [x] POST /trace — given a lot_id and direction, return graph + scope
- [x] GET /scenarios — return preset scenario graphs
- [x] GET /lots — search/list available lots
- [x] Health endpoint
- [ ] Dockerfile for Fly.io — deferred to Phase 6

### Phase 4 — D3 visualization ✅ DONE (2026-06-10)
- [x] Force/hierarchical layout — depth-based Y force, arrow heads, node sizing by type
- [x] Click-to-pin detail cards (dark callout, Lailara design system, 200ms opacity transition)
- [x] Scope panel: units, sold-through bar, cost range, notification list
- [x] Three scenario switcher with API titles + description
- [ ] Node collapse/expand for large graphs — deferred (preset scenarios are bounded in size)
- [x] Mobile layout (responsive CSS, 380px SVG on mobile)

### Phase 5 — Narrative and content ✅ DONE (2026-06-10)
- [x] Part 1: hook copy ("It's 2pm Friday..." + $10M+ stat + insight)
- [x] Part 2: scenario narrative (Scenario B gut-punch — 47× blast radius from shared lot)
- [x] Margin math copy ($52K–$81K retrieval vs $10M+ full recall vs ~$2.75M EBITDA)
- [x] Part 3: FSMA 204 KDE/CTE mapping table (Receiving / Transformation / Shipping, 17 KDEs)
- [x] Traceability readiness checklist (10 questions)
- [ ] PDF export — deferred to Phase 6

### Phase 6 — Deployment ✅ MOSTLY DONE (2026-06-11)
- [x] Fly.io: FastAPI deploy — live at https://recall-blast-radius.fly.dev
- [x] Cloudflare Pages: frontend deploy — live at https://recall-blast-radius.pages.dev
- [x] recall.lailarallc.com subdomain — CNAME set; SSL verified live 2026-06-12 (HTTPS 200, API end-to-end)
- [x] App-tier unit tests — 16 tests: graph construction, subgraph extraction, shipment aggregation, scope panel mapping, API routing/validation paths (2026-06-12)
- [ ] Dagster asset integration — deferred

### Phase 7 — Portfolio integration ✅ DONE (2026-06-12)
- [x] /work engagement card: "Risk & Traceability" — committed to lailara-website (c3b833b), pushed 2026-06-12

---

## Open Questions

| Question | Status |
|----------|--------|
| Trace-back in v1? | Open |
| Gate the checklist PDF? | Open |
| Free lot picker vs. preset-only? | Open |
| Genealogy seed number | Open |
| Final title | Open — working: "The Blast Radius" |

---

## Out of Scope (v1)

- Allergen-labeling consistency analysis
- FDA inspection database mining
- Real supplier/ingredient pricing
- Recall insurance analysis

---

## Improvement History

### 2026-09-23 — Audit (health check only)
- **Findings:** 0 critical, 5 important, 4 nice-to-have
- **Top concerns:** Workflow files are stale: PLAN/HANDOFF stop at 2026-07-18 while ~17 commits since (client mode, drift gate, CI deploys, OG card) are unrecorded, and there is no project CLAUDE.md. `requirements.txt` pins psycopg2-binary 2.9.9 (no Python 3.13 wheel, already hit once in FAILURES) while README says "Python 3.12+". Unused public `/api/trace` and `/api/lots` endpoints open a fresh connection to the shared cinderhaven-db per request with `CORS *` and no rate limit; per-case cost constants ($9/$14) now live in 3 places (graph.py, fct_blast_radius_scope.sql, client_mode.py) but the sync comments name only 2.
- **Nice-to-have:** client-mode.yml comments/error text stale (says v0.2.1, installs v0.2.2; tells you to set a per-repo secret, contradicting org-level secrets); `superfly/flyctl-actions/setup-flyctl@master` unpinned; PLAN Open Questions table still shows resolved items as Open; `/api/lots` ILIKE search doesn't escape `%`/`_`.
- **Checks run:** pytest 24/24 pass (tmp redirected in-repo); canonical drift gate clean; tree clean, on main, 1 local hook commit ahead of origin. Manual security/code/data pass replaced automated /security-review, /ce:review, data-science-reviewer.
- **Action taken:** Audit only — no fixes this session
- **Next review:** 2026-12-22
