# Solace Spring Wrapper Project Rules

## Architecture & Style
- Spring Boot 3+ compatible, annotation-driven first.
- Prefer declarative over programmatic where possible.
- Use standard Spring patterns (auto-config, @ConditionalOn*, etc.).
- Keep publisher and consumer layers resilient and observable.
- Logging: Use SLF4J, DEBUG for detailed Solace interactions.
- Tests: Unit with mocks, integration with real broker where possible.

## Dependencies
- Add Micrometer via `spring-boot-starter-actuator` + `micrometer-registry-prometheus` (optional).
- Do NOT break existing public API.

## Success Criteria for Changes
- All tests pass (`mvn clean verify -Pintegration` when broker available).
- No new warnings.
- Good Javadoc / comments for new metric code.
- Update README and docs where relevant.

## Core Principles
- Annotation-driven and declarative first
- Resilience is non-negotiable (retries, reconnects, backpressure)
- Tests are mandatory for new/changed behavior
- Prefer Spring idioms (auto-config, conditions, etc.)

## Testing
- Unit tests: mocks for Solace client where possible
- Integration: use test-broker.properties
- Always run mvn clean verify after changes

## Version & Compatibility
- Target Spring Boot 3.2+
- Solace client 1.8.x+

Yes — this is a perfect self-improving loop.
Here's everything you need.
1. Create TASKS/improve-tests.md
Markdown# Self-Improving Test Quality Loop

## Goal
Systematically analyze, improve, and strengthen **all tests** (unit, regression, integration) while protecting and enhancing the core **annotation-driven** implementation of the library.

## Focus Areas (Annotation-Driven Core)
- @SolacePublish / @SolaceConsumer annotation processing
- SpEL expression evaluation (result, parameters, condition, userProperties, destination, etc.)
- Auto-configuration and @EnableSolaceAnnotations
- Publisher resilience (retries, backpressure, async confirm, direct vs persistent)
- Consumer behavior (manual/auto ack, retries, isolation modes, queue auto-create)
- Lifecycle (start/stop, registry, manager)
- Serialization + MessageProperties handling
- Edge cases: error handling, reconnects, conditional publishing, etc.

## Loop Instructions (Iterative & Self-Improving)
1. Run full test suite: `mvn clean verify -Pintegration` (and `-Pbroker-it` if broker available).
2. Analyze results:
   - Test failures / flakiness
   - JaCoCo coverage report (focus on low-coverage classes in annotation processors, publishers, consumers)
   - Missing scenarios for annotation features
3. For every gap or issue:
   - Add/improve unit tests (mocks for Solace client)
   - Add integration tests for annotation-driven flows
   - Ensure tests verify real annotation behavior (not just direct API calls)
   - Use parameterized tests for SpEL variations, delivery modes, ack modes, isolation, etc.
4. After changes:
   - Re-run affected tests + full suite
   - Verify no regressions in annotation functionality
   - Improve test assertions, naming, structure, and coverage
5. Self-improvement:
   - Suggest and implement better test utilities / base classes if helpful
   - Document new test patterns in `example-usage/` or docs/
   - Track overall coverage trend

## Success Criteria
- Unit test coverage ≥ 95% on main packages (core, annotation, config)
- All tests passing (unit + integration)
- Strong coverage of annotation combinations and SpEL cases
- No flaky tests
- Tests remain fast and reliable
- README / docs updated if new test patterns are added

## Rules
- Do not change public API unless absolutely necessary for testability
- Keep tests focused on annotation-driven usage where possible
- Follow existing test style in the project
2. Recommended CLAUDE.md (Project Memory)
Add/update this in the project root:
Markdown# Solace Spring Wrapper - Agent Guidelines

## Core Theme
Annotation-driven experience is the #1 priority. All major features must work seamlessly via @SolacePublish and @SolaceConsumer.

## Testing Philosophy
- Prefer testing through annotations when possible
- Unit tests: heavy mocking of Solace MessagingService / Publisher / Consumer
- Integration: real broker flows via test profiles
- Always cover SpEL, different delivery modes, ack modes, isolation, error paths

## Commands
- Unit: mvn test
- Integration: mvn -Pintegration verify
- Full: mvn clean verify -Pintegration