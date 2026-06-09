# Explore Feasibility + Implement Request-Reply Support

## Phase 1: Feasibility Exploration (Do first)
1. Analyze current publisher, consumer, annotation processors, MessageProperties (especially replyTo, correlationId), and Solace client capabilities.
2. Research Solace best practices for request-reply (temporary queues, correlation IDs, timeouts, direct vs persistent, etc.).
3. Propose design that stays true to annotation-driven core:
   - New annotation(s)? e.g. @SolaceRequestReply or enhanced @SolacePublish with reply handling.
   - Synchronous & async variants (with timeout, CompletableFuture, etc.).
   - Integration with existing SpEL, MessageProperties, resilience, and serialization.
   - Temporary reply queue management, correlation handling, timeout/error scenarios.
4. Evaluate pros/cons, risks (complexity, resource usage, testing), and alternatives.
5. Document summary (in this file or new docs/REQUEST-REPLY.md) before coding.

## Phase 2: Implementation
- Implement core request-reply logic in publisher layer (reuse existing resilience).
- Add annotation support for seamless usage (e.g. @SolaceRequestReply).
- Support both blocking + non-blocking (Future/Callback) patterns.
- Handle correlationId, replyTo, timeouts, and error propagation.
- Add comprehensive unit + integration tests focused on annotation-driven flows.
- Update README, docs/ANNOTATIONS.md, and example-usage.

## Success Criteria
- Phase 1 feasibility summary completed and documented.
- Request-reply works cleanly via annotations (core theme preserved).
- Supports common patterns: sync with timeout, async, with custom properties.
- No breaking changes to existing API.
- Coverage ≥ 95% on new code.
- `mvn clean verify -Pintegration` passes cleanly.
- Good documentation + example in example-usage/.

## Rules
- Annotation-driven first (metrics/metrics package already exists — integrate if relevant).
- Reuse existing publisher resilience, SpEL, serialization.
- Minimal new dependencies.
- Follow CLAUDE.md style and patterns.