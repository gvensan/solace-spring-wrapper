# Recorded loop prompts

> Historical record of the self-improving `/loop` prompts used to build this library.
> The referenced `TASKS/*.md` task files are kept locally only and are no longer tracked
> in the repository (see `.gitignore`); the paths below are preserved as-is for context.

TASKS/add-metrics.md

Implement everything in TASKS/add-metrics.md. Follow rules in CLAUDE.md. Use TDD where possible. Keep running mvn verify and fixing until all success criteria are satisfied. Stop only when done.

TASKS/code-quality-sweep.md

/loop
Read TASKS/code-quality-sweep.md and CLAUDE.md. 
Run `mvn clean verify` and analyze results. 
Find and fix issues, add tests where coverage is weak or edge cases missing. 
Re-verify after each major change. 
Continue looping until all success criteria are met.

TASKS/improve-tests.md

/loop
Read TASKS/improve-tests.md and CLAUDE.md thoroughly. 
Run `mvn clean verify -Pintegration` and analyze coverage + failures. 
Identify gaps in annotation-driven testing (SpEL, publishers, consumers, request-reply, lifecycle, use cases in example-usage, edge cases). 
Add or improve tests to increase coverage and correctness. 
Re-run tests after changes and fix any issues. 
Continue looping and self-improving until success criteria are met and coverage is strong.

TASKS/polish-and-complete-metrics.md

/loop
Read CLAUDE.md, TASKS/polish-and-complete-metrics.md, and the metrics package thoroughly. 
First analyze current implementation and coverage. 
Then complete, fix, and strengthen the Micrometer support with strong annotation integration. 
Test thoroughly and update docs. 
Continue until success criteria are met.

TASKS/explore-and-implement-request-reply.md

/loop
Read CLAUDE.md, TASKS/explore-and-implement-request-reply.md, and relevant source files (publisher, annotation, consumer, MessageProperties). 
First complete Phase 1: full feasibility exploration and proposal. 
Only proceed to Phase 2 implementation if feasibility looks good. 
Then implement, add tests (unit + integration), fix issues, and update documentation. 
Run mvn clean verify -Pintegration + jacoco after changes. 
Loop and self-improve until all success criteria are met.