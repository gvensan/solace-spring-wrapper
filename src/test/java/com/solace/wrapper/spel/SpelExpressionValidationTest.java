package com.solace.wrapper.spel;

import com.solace.wrapper.annotation.processor.SpelExpressionResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Unit tests for SpEL expression security validation.
 * Tests that dangerous patterns are rejected to prevent SpEL injection attacks.
 */
public class SpelExpressionValidationTest {

    private static final Logger log = LoggerFactory.getLogger(SpelExpressionValidationTest.class);

    private SpelExpressionResolver resolver;

    @BeforeEach
    void setUp() {
        resolver = new SpelExpressionResolver();
    }

    @Test
    void validateExpression_accepts_null_and_empty() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: validateExpression_accepts_null_and_empty\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that null and empty SpEL expressions are accepted without error.\n" +
                "  These represent 'no expression' and should pass validation silently.\n" +
                "\n" +
                "WHY THIS MATTERS:\n" +
                "  - @SolacePublish attributes have default empty values\n" +
                "  - Empty string means 'use default' or 'not specified'\n" +
                "  - Null/empty should NOT trigger security validation errors\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - null expression: accepted (no exception)\n" +
                "  - empty string: accepted (no exception)\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Validating null expression - should not throw");
        resolver.validateExpression(null);
        log.info("        Result: ACCEPTED");

        log.info("STEP 2: Validating empty string expression - should not throw");
        resolver.validateExpression("");
        log.info("        Result: ACCEPTED");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  null expression: ACCEPTED (no error)\n" +
                "  empty expression: ACCEPTED (no error)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  SpelExpressionResolver correctly handles absent expressions without\n" +
                "  treating them as potential security threats.\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void validateExpression_accepts_safe_expressions() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: validateExpression_accepts_safe_expressions\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that common safe SpEL patterns used in messaging are accepted.\n" +
                "  These represent the expected usage patterns in @SolacePublish/@SolaceConsumer.\n" +
                "\n" +
                "SAFE PATTERNS BEING TESTED:\n" +
                "  1. Variable access: #order.orderId, #result\n" +
                "  2. String operations: toUpperCase(), length()\n" +
                "  3. Arithmetic: #price * #quantity\n" +
                "  4. Comparisons: #value > 100, #status == 'active'\n" +
                "  5. Template expressions: orders/#{#category}/#{#id}\n" +
                "\n" +
                "WHY THESE ARE SAFE:\n" +
                "  - No system access (Runtime, ProcessBuilder)\n" +
                "  - No file I/O operations\n" +
                "  - No network operations\n" +
                "  - No reflection or class loading\n" +
                "  - Only data transformation and business logic\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  All expressions accepted without exception\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Testing simple variable access expressions");
        resolver.validateExpression("#order.orderId");
        resolver.validateExpression("order.customerId");
        resolver.validateExpression("#result");
        log.info("        All variable access patterns: ACCEPTED");

        log.info("STEP 2: Testing string operation expressions");
        resolver.validateExpression("#name.toUpperCase()");
        resolver.validateExpression("#value.length()");
        log.info("        All string operation patterns: ACCEPTED");

        log.info("STEP 3: Testing arithmetic and comparison expressions");
        resolver.validateExpression("#price * #quantity");
        resolver.validateExpression("#value > 100");
        resolver.validateExpression("#status == 'active'");
        log.info("        All arithmetic/comparison patterns: ACCEPTED");

        log.info("STEP 4: Testing template expressions");
        resolver.validateExpression("orders/#{#category}/#{#id}");
        resolver.validateExpression("#{#priority == 'high'}");
        log.info("        All template expression patterns: ACCEPTED");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Variable access patterns: ACCEPTED\n" +
                "  String operation patterns: ACCEPTED\n" +
                "  Arithmetic/comparison patterns: ACCEPTED\n" +
                "  Template expression patterns: ACCEPTED\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  All common messaging-related SpEL patterns pass validation.\n" +
                "  Users can safely use these patterns in annotation attributes.\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "T(java.lang.Runtime).getRuntime().exec('ls')",
        "T(Runtime).getRuntime()",
        "new ProcessBuilder('cmd').start()",
        "getClass().forName('java.lang.Runtime')",
        "Class.forName('java.lang.Runtime')",
        "System.exit(0)",
        "#obj.getClass().getClassLoader()",
        "T(java.io.File).new('/')",
        "T(java.net.URL).new('http://evil.com')",
        "T(javax.script.ScriptEngineManager).new()",
        "getenv('PATH')",
        "System.getProperty('user.home')",
        "#obj.getClass().getDeclaredMethod('foo')",
        "#obj.getClass().getDeclaredField('bar')",
        "#method.setAccessible(true)",
        "#obj.invoke(#target, #args)"
    })
    void validateExpression_rejects_dangerous_patterns(String dangerousExpression) {
        log.info("Testing dangerous pattern rejection: " + dangerousExpression);
        assertThatThrownBy(() -> resolver.validateExpression(dangerousExpression))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("SpEL expression contains potentially unsafe pattern");
    }

    @Test
    void validateExpression_case_insensitive_detection() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: validateExpression_case_insensitive_detection\n" +
                "PURPOSE: Verify dangerous patterns are detected regardless of case\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Testing lowercase 't' type access");
        assertThatThrownBy(() -> resolver.validateExpression("t(java.lang.RUNTIME)"))
            .isInstanceOf(IllegalArgumentException.class);

        log.info("STEP 2: Testing uppercase PROCESSBUILDER");
        assertThatThrownBy(() -> resolver.validateExpression("PROCESSBUILDER"))
            .isInstanceOf(IllegalArgumentException.class);

        log.info("STEP 3: Testing mixed case System.EXIT");
        assertThatThrownBy(() -> resolver.validateExpression("System.EXIT(0)"))
            .isInstanceOf(IllegalArgumentException.class);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: Case-insensitive detection works correctly\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void validateExpression_rejects_file_operations() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: validateExpression_rejects_file_operations\n" +
                "PURPOSE: Verify file I/O classes are blocked to prevent file system access\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Testing FileOutputStream rejection");
        assertThatThrownBy(() -> resolver.validateExpression("T(java.io.FileOutputStream)"))
            .isInstanceOf(IllegalArgumentException.class);

        log.info("STEP 2: Testing FileReader rejection");
        assertThatThrownBy(() -> resolver.validateExpression("T(java.io.FileReader)"))
            .isInstanceOf(IllegalArgumentException.class);

        log.info("STEP 3: Testing java.nio.file.Files rejection");
        assertThatThrownBy(() -> resolver.validateExpression("T(java.nio.file.Files)"))
            .isInstanceOf(IllegalArgumentException.class);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: File operation classes correctly blocked\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void validateExpression_rejects_network_operations() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: validateExpression_rejects_network_operations\n" +
                "PURPOSE: Verify network classes are blocked to prevent remote connections\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Testing Socket rejection");
        assertThatThrownBy(() -> resolver.validateExpression("T(java.net.Socket)"))
            .isInstanceOf(IllegalArgumentException.class);

        log.info("STEP 2: Testing URL rejection");
        assertThatThrownBy(() -> resolver.validateExpression("new java.net.URL('http://x')"))
            .isInstanceOf(IllegalArgumentException.class);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: Network operation classes correctly blocked\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void validateExpression_rejects_script_engine() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: validateExpression_rejects_script_engine\n" +
                "PURPOSE: Verify script engine classes are blocked to prevent code execution\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Testing ScriptEngine rejection");
        assertThatThrownBy(() -> resolver.validateExpression("T(javax.script.ScriptEngine)"))
            .isInstanceOf(IllegalArgumentException.class);

        log.info("STEP 2: Testing ScriptEngineManager rejection");
        assertThatThrownBy(() -> resolver.validateExpression("ScriptEngineManager"))
            .isInstanceOf(IllegalArgumentException.class);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: Script engine classes correctly blocked\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void resolveExpression_validates_before_evaluation() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: resolveExpression_validates_before_evaluation\n" +
                "PURPOSE: Verify validation occurs before expression evaluation (not just parsing)\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Creating evaluation context");
        var context = resolver.createEvaluationContext(
            getClass().getMethods()[0], // any method
            new Object[]{},
            null
        );

        log.info("STEP 2: Attempting to resolve dangerous expression - should fail during validation");
        assertThatThrownBy(() ->
            resolver.resolveExpression("T(java.lang.Runtime).getRuntime()", context, Object.class))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("unsafe pattern");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: Validation correctly prevents dangerous expression evaluation\n" +
                "───────────────────────────────────────────────────────────────\n");
    }
}
