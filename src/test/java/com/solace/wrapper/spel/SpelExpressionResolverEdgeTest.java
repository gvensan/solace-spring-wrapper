package com.solace.wrapper.spel;

import com.solace.wrapper.annotation.processor.SpelExpressionResolver;
import org.junit.jupiter.api.Test;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Edge-case coverage for {@link SpelExpressionResolver}: null/empty handling, literal vs SpEL
 * detection, template resolution, numeric/boolean type conversion, the between/array/system-function
 * prefixing paths, and method-based evaluation contexts with parameter-name discovery.
 */
class SpelExpressionResolverEdgeTest {

    private final SpelExpressionResolver resolver = new SpelExpressionResolver();

    @SuppressWarnings("unused")
    static class Sample {
        public void method(String orderId, int amount) { /* for reflection */ }
    }

    private static Method sampleMethod() throws Exception {
        return Sample.class.getMethod("method", String.class, int.class);
    }

    @Test
    void null_and_empty_expressions_resolve_to_null() {
        EvaluationContext ctx = new StandardEvaluationContext();
        assertThat(resolver.resolveExpression(null, ctx, String.class)).isNull();
        assertThat(resolver.resolveExpression("", ctx, String.class)).isNull();
    }

    @Test
    void numeric_and_boolean_literals_convert_to_expected_types() {
        EvaluationContext ctx = new StandardEvaluationContext();
        assertThat(resolver.resolveExpression("42", ctx, Integer.class)).isEqualTo(42);
        assertThat(resolver.resolveExpression("100", ctx, Long.class)).isEqualTo(100L);
        assertThat(resolver.resolveExpression("3.5", ctx, Double.class)).isEqualTo(3.5);
        assertThat(resolver.resolveExpression("true", ctx, Boolean.class)).isTrue();
        assertThat(resolver.resolveExpression("hello", ctx, String.class)).isEqualTo("hello");
    }

    @Test
    void unconvertible_number_returns_null() {
        EvaluationContext ctx = new StandardEvaluationContext();
        // "notanumber" is not a valid SpEL var or number -> falls back to literal -> Integer parse fails -> null
        assertThat(resolver.resolveExpression("notanumber", ctx, Integer.class)).isNull();
    }

    @Test
    void direct_variable_expression_resolves() {
        StandardEvaluationContext ctx = new StandardEvaluationContext();
        ctx.setVariable("name", "Alice");
        assertThat(resolver.resolveExpression("#name", ctx, String.class)).isEqualTo("Alice");
    }

    @Test
    void template_expression_replaces_placeholders() {
        StandardEvaluationContext ctx = new StandardEvaluationContext();
        ctx.setVariable("category", "vip");
        assertThat(resolver.resolveExpression("orders/#{#category}/new", ctx, String.class))
                .isEqualTo("orders/vip/new");
    }

    @Test
    void method_context_resolves_parameter_names_and_result() throws Exception {
        Method m = sampleMethod();
        EvaluationContext ctx = resolver.createEvaluationContext(m, new Object[]{"O1", 50}, "RES");

        assertThat(resolver.resolveExpression("#orderId", ctx, String.class)).isEqualTo("O1");
        // Bare variable gets auto-prefixed.
        assertThat(resolver.resolveExpression("orderId", ctx, String.class)).isEqualTo("O1");
        // Indexed fallback access.
        assertThat(resolver.resolveExpression("#arg1", ctx, Integer.class)).isEqualTo(50);
        // Result variable.
        assertThat(resolver.resolveExpression("#result", ctx, String.class)).isEqualTo("RES");
    }

    @Test
    void method_context_with_target_sets_target_variable() throws Exception {
        Method m = sampleMethod();
        Object target = new Sample();
        StandardEvaluationContext ctx =
                (StandardEvaluationContext) resolver.createEvaluationContext(m, new Object[]{"O1", 50}, null, target);
        assertThat(ctx.lookupVariable("target")).isSameAs(target);
        assertThat(ctx.lookupVariable("arg0")).isEqualTo("O1");
    }

    @Test
    void quoted_string_concatenation_is_preserved() throws Exception {
        Method m = sampleMethod();
        EvaluationContext ctx = resolver.createEvaluationContext(m, new Object[]{"O1", 50}, null);
        assertThat(resolver.resolveExpression("orderId + '_suffix'", ctx, String.class))
                .isEqualTo("O1_suffix");
    }

    @Test
    void array_access_expression_resolves() throws Exception {
        Method m = sampleMethod();
        EvaluationContext ctx = resolver.createEvaluationContext(m, new Object[]{"O1", 50}, null);
        assertThat(resolver.resolveExpression("args[0]", ctx, String.class)).isEqualTo("O1");
    }

    @Test
    void between_operator_is_converted() throws Exception {
        Method m = sampleMethod();
        EvaluationContext ctx = resolver.createEvaluationContext(m, new Object[]{"O1", 50}, null);
        assertThat(resolver.resolveExpression("amount between 1 and 100", ctx, Boolean.class)).isTrue();
        assertThat(resolver.resolveExpression("amount between 1 and 10", ctx, Boolean.class)).isFalse();
    }

    @Test
    void system_function_call_resolves() {
        EvaluationContext ctx = new StandardEvaluationContext();
        String value = resolver.resolveExpression("T(System).lineSeparator()", ctx, String.class);
        assertThat(value).isEqualTo(System.lineSeparator());
    }

    @Test
    void bean_only_context_is_created_without_bean_factory() {
        // No BeanFactory injected -> still returns a usable context (just without @bean support).
        assertThat(resolver.createBeanOnlyContext()).isNotNull();
    }
}
