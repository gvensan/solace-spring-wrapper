# Code Quality Sweep: Find Issues, Fix Them, Add/Improve Tests, Verify

## Goal
Perform a comprehensive pass over the entire codebase to identify and resolve issues, improve reliability, increase test coverage, and ensure everything is production-ready.

## Scope
- Scan all source code, tests, configuration, and documentation.
- Fix bugs, potential bugs, deprecations, warnings, and code smells.
- Improve or add tests (unit + integration) where coverage is low or edge cases are missing.
- Update documentation if changes affect usage.
- Respect existing architecture and public APIs.

## Instructions for the Agent
1. Run full build and tests first: `mvn clean verify` (and integration profiles if broker available).
2. Analyze output for:
   - Compiler warnings
   - Test failures / flakiness
   - JaCoCo coverage gaps (aim for >80-95% on main packages)
   - Static analysis issues (even without external tools — look for nulls, resource leaks, exception handling, thread safety, etc.)
   - Deprecated Solace API usage
   - Inconsistent naming, missing Javadoc, TODOs/FIXMEs
3. For each issue found:
   - Plan the fix
   - Implement + add tests if needed
   - Re-run relevant tests
   - Commit with clear message (e.g., "fix: ...", "test: ...")
4. Prioritize:
   - Critical bugs / broken functionality
   - Security / reliability issues
   - Test gaps in core publisher/consumer paths
   - Then nice-to-haves (docs, minor cleanups)

## Success Criteria
- `mvn clean verify` completes with zero errors/warnings (or justified suppressions)
- JaCoCo coverage ≥ 95% for main source packages
- All integration tests pass (when broker is configured)
- No new TODOs introduced; existing ones addressed or documented
- README / docs remain accurate
- Changes are backward compatible

## Project Rules (reference CLAUDE.md)
- Follow Spring Boot conventions
- Keep resilience and observability focus
- Do not break public API