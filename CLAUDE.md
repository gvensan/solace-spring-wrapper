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