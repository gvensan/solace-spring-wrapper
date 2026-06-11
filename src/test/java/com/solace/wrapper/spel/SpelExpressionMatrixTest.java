package com.solace.wrapper.spel;

import com.solace.wrapper.annotation.processor.SpelExpressionResolver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A broad, parameterized SpEL matrix exercising {@link SpelExpressionResolver}'s variable-prefixing
 * and evaluation logic across the expression shapes the annotations support: bare variables and
 * property chains, quoted-string concatenation, ternaries, method calls, the {@code between}
 * operator, array access, {@code T(...)} system functions, boolean operators, and template
 * placeholders.
 */
class SpelExpressionMatrixTest {

    private final SpelExpressionResolver resolver = new SpelExpressionResolver();

    public static class Order {
        public String id = "O1";
        public int amount = 50;
        public String type = "VIP";
        public String getId() { return id; }
        public int getAmount() { return amount; }
        public String getType() { return type; }
    }

    private StandardEvaluationContext ctx() {
        StandardEvaluationContext ctx = new StandardEvaluationContext();
        ctx.setVariable("order", new Order());
        ctx.setVariable("customerId", "C1");
        ctx.setVariable("region", "us-east");
        ctx.setVariable("args", new Object[]{"A0", "A1"});
        return ctx;
    }

    static Stream<Arguments> stringExpressions() {
        return Stream.of(
                Arguments.of("order.id", "O1"),
                Arguments.of("order.type", "VIP"),
                Arguments.of("customerId", "C1"),
                Arguments.of("#customerId", "C1"),
                Arguments.of("order.id + '_' + customerId", "O1_C1"),
                Arguments.of("order.id + '-suffix'", "O1-suffix"),
                Arguments.of("region.toUpperCase()", "US-EAST"),
                Arguments.of("customerId.toLowerCase()", "c1"),
                Arguments.of("order.type == 'VIP' ? 'priority' : 'standard'", "priority"),
                Arguments.of("order.amount > 100 ? 'big' : 'small'", "small"),
                Arguments.of("args[0]", "A0"),
                Arguments.of("args[1]", "A1"),
                Arguments.of("T(java.lang.String).valueOf(order.amount)", "50"),
                Arguments.of("plainliteral", "plainliteral"),
                Arguments.of("orders/#{order.type}/new", "orders/VIP/new"),
                Arguments.of("#{customerId}-#{region}", "C1-us-east")
        );
    }

    @ParameterizedTest(name = "[{index}] {0} -> {1}")
    @MethodSource("stringExpressions")
    void resolves_string_expressions(String expression, String expected) {
        assertEquals(expected, resolver.resolveExpression(expression, ctx(), String.class));
    }

    static Stream<Arguments> booleanExpressions() {
        return Stream.of(
                Arguments.of("order.amount between 1 and 100", true),
                Arguments.of("order.amount between 1 and 10", false),
                Arguments.of("order.amount > 10 and order.amount < 100", true),
                Arguments.of("order.amount > 100 or order.type == 'VIP'", true),
                Arguments.of("not (order.type == 'VIP')", false),
                Arguments.of("order.type matches '[A-Z]+'", true),
                Arguments.of("order.id == 'O1'", true)
        );
    }

    @ParameterizedTest(name = "[{index}] {0} -> {1}")
    @MethodSource("booleanExpressions")
    void resolves_boolean_expressions(String expression, boolean expected) {
        assertEquals(expected, resolver.resolveExpression(expression, ctx(), Boolean.class));
    }

    static Stream<Arguments> numericExpressions() {
        return Stream.of(
                Arguments.of("order.amount", 50),
                Arguments.of("order.amount + 5", 55),
                Arguments.of("order.amount * 2", 100),
                Arguments.of("order.amount mod 7", 1),
                Arguments.of("order.amount div 5", 10)
        );
    }

    @ParameterizedTest(name = "[{index}] {0} -> {1}")
    @MethodSource("numericExpressions")
    void resolves_numeric_expressions(String expression, int expected) {
        assertEquals(expected, resolver.resolveExpression(expression, ctx(), Integer.class));
    }

    @Test
    void unresolvable_property_keeps_template_placeholder() {
        // A #{...} placeholder that fails to resolve is preserved (not blanked).
        String result = resolver.resolveExpression("x/#{order.missingField}/y", ctx(), String.class);
        assertThat(result).contains("#{order.missingField}");
    }

    @Test
    void bean_reference_without_factory_falls_back_to_literal() {
        // No BeanFactory configured -> @bean references can't resolve; the resolver returns the
        // original expression rather than throwing.
        String result = resolver.resolveExpression("@myBean.prop", ctx(), String.class);
        assertThat(result).isEqualTo("@myBean.prop");
    }
}
