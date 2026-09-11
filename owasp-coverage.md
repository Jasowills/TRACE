# OWASP Top 10 (2025) coverage — TRACE

Threat model (lightweight): TRACE is a **local-only demo tool**. No network
listeners (MCP over stdio; Redis/Postgres on localhost), no auth system, no
user accounts, no URL-accepting features, no outbound fetches, no browser UI.
Attack surface = MCP tool inputs (`event_id`, `since`/`until`) + Redis stream
content + npm dependencies. Each category maps to a test/gate or a recorded
justification — no category is silently absent.

| ID | Category | Status | Evidence |
|----|----------|--------|----------|
| A01 | Broken Access Control | N/A — no users, roles, or cross-tenant data; single local operator | — |
| A02 | Security Misconfiguration | Covered (accepted risk) | Default `trace/trace` creds are localhost-only by compose port binding; recorded here. `tests/security.test.ts` A10 asserts no stack/SQL leak in tool output |
| A03 | Supply Chain | Gated | `package-lock.json` committed; CI installs via `npm ci`; CI `security` job fails on high/critical in **production** deps (`npm audit --omit=dev`). Dev-only vite/vitest advisories: accepted risk (no prod HTTP server; local dev/CI only), re-reviewed on dependency bumps |
| A04 | Cryptographic Failures | N/A — no TLS/auth/session scope | Postgres password is a documented local-dev placeholder (`.env.example`), never a real secret; secret-grep in CI |
| A05 | Injection | Tested | `tests/security.test.ts`: SQLi payloads via `trace_event`/`since`/`until`/unicode ids — all queries parameterized; hostile `event_id` fails closed (UUID gate), hostile timestamps rejected (strict ISO gate) |
| A06 | Insecure Design | N/A — no auth endpoints, no URL fetching, no business-logic abuse surface | — |
| A07 | Authentication Failures | N/A — no auth mechanism exists | — |
| A08 | Integrity Failures | Covered | No CDN scripts, no deserialization of untrusted code (`JSON.parse` guarded + warn in worker); lockfile covers artifact integrity |
| A09 | Logging/Alerting | Partial | Failures logged (worker errors, malformed-entry warns); no alerting pipeline — accepted (local demo, human watches the run) |
| A10 | Exceptional Conditions | Tested | `tests/security.test.ts` error-contract test; ADV-0001/ADV-0007 fixes: worker exits 1 on stuck drain, MCP errors carry detail without internals |

DAST (ZAP) and authenticated Playwright suites are intentionally absent: there
is no HTTP surface to scan. If TRACE ever gains one, this table must gain rows
for it — that is the tripwire.
