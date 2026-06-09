# Polish, Complete & Strengthen Micrometer Metrics

## Goal
Review the existing metrics package, complete any gaps, ensure full annotation-driven integration, improve coverage, and make it production-grade.

## Current State (Analyze First)
- Check `com.solace.wrapper.metrics.*`
- Check integration with @SolacePublish / @SolaceConsumer
- Check auto-configuration and conditional activation
- Check existing tests for metrics

## Tasks
1. **Review & Document**
   - Run coverage report
   - Analyze what metrics are already implemented (counters, timers, gauges, tags)
   - Document the current metrics in `docs/METRICS.md` (create if missing)

2. **Complete & Improve**
   - Add missing key metrics (latency, retries, backpressure, connection gauges, etc.)
   - Ensure strong annotation-driven support (metrics emitted automatically via annotations)
   - Add proper tags (deliveryMode, destination, outcome, etc.)
   - Add configuration (`solace.metrics.enabled`, etc.)
   - Make actuator dependency optional where possible

3. **Testing & Quality**
   - Add/Improve unit + integration tests for metrics (verify counters/timers increase correctly)
   - Aim for ≥ 95% coverage on metrics package
   - Test with and without Actuator on classpath

4. **Polish**
   - Update README with metrics examples (Prometheus queries, Grafana)
   - Javadoc + comments
   - Ensure no performance regression

## Success Criteria
- All metrics work seamlessly with annotations
- Full test coverage on metrics code
- `mvn clean verify -Pintegration` passes
- Documentation updated
- No breaking changes

## Rules
- Follow CLAUDE.md and annotation-first philosophy
- Minimal overhead