# Self-Improving Test Quality Loop

## Goal
Boost test coverage, eliminate gaps/flakiness, and strengthen verification of the **annotation-driven core** (@SolacePublish / @SolaceConsumer, SpEL, auto-config, resilience).

## Current Baseline (from src/test/TESTS.md)
- Overall: ~70% instruction / 59% branch coverage
- Strong areas: annotation (100%), health, exceptions
- Weaker areas: connection (40%), serialization (52%), config (59%), consumer/publisher (~75%)

## Focus Areas (Annotation-Driven Core)
- Annotation processing & SpEL evaluation (result, #p0/#paramName, condition, userProperties, destination, etc.)
- @EnableSolaceAnnotations + auto-configuration
- Publisher resilience (retries, backpressure, async confirm, direct vs persistent)
- Consumer behavior (ack modes, retries, isolation, queue auto-create, manual ack)
- Lifecycle (start/stop, registry, manager, @PostConstruct/@PreDestroy)
- Connection pooling & isolation modes
- Serialization + MessageProperties
- Edge cases: errors, reconnects, conditionals, property expressions

## Loop Instructions (Iterative & Self-Improving)
1. Run: `mvn clean verify -Pintegration` (unit + integration).
2. Generate coverage: `mvn jacoco:report` and analyze `target/site/jacoco/index.html`.
3. Identify gaps:
   - Low-coverage classes in `com.solace.wrapper.annotation`, `processor`, `consumer`, `publisher`, `connection`, `config`, `serialization`.
   - Missing annotation combinations, SpEL variations, error paths.
4. For each gap:
   - Add/improve **unit tests** (prefer mocks via existing test utilities).
   - Add **integration tests** that exercise annotations end-to-end where valuable.
   - Use parameterized tests heavily for SpEL, delivery modes, ack modes, etc.
   - Follow the project's test style (detailed logging with ──────── separators).
5. After every change:
   - Re-run affected tests + full suite.
   - Verify no regression in annotation behavior.
6. Self-improvement:
   - Enhance test utilities/base classes if patterns repeat.
   - Document new patterns in `src/test/TESTS.md` or `example-usage/`.
   - Suggest coverage improvements in commit messages.

## Success Criteria
- ≥ 95% instruction coverage on main packages (core, annotation, config, consumer, publisher).
- All tests green (unit + integration).
- No flaky tests.
- Strong annotation + SpEL test matrix.
- Tests remain fast and follow project conventions.
- `src/test/TESTS.md` updated with any new patterns.

## Rules
- Never break public API.
- Prioritize testing *through annotations* over direct class tests.
- Follow existing test style (see `src/test/TESTS.md`).
- Use `TestBroker.assumeAvailable()` for integration tests.