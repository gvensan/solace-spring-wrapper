package com.solace.wrapper.spel;

import com.solace.wrapper.annotation.processor.SpelExpressionResolver;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.expression.EvaluationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Covers internal branches of {@link SpelExpressionResolver}: the BeanFactory-backed
 * {@code @bean} resolution path and numeric/boolean type conversion of literal/SpEL results.
 */
class SpelExpressionResolverInternalsTest {

    public static class Cfg {
        public String getTopic() { return "orders/from/bean"; }
        public int getMax() { return 7; }
    }

    @Test
    void bean_reference_resolves_when_bean_factory_present() {
        SpelExpressionResolver resolver = new SpelExpressionResolver();
        BeanFactory bf = mock(BeanFactory.class);
        when(bf.getBean("cfg")).thenReturn(new Cfg());
        resolver.setBeanFactory(bf);

        EvaluationContext ctx = resolver.createBeanOnlyContext();
        assertThat(resolver.resolveExpression("@cfg.topic", ctx, String.class)).isEqualTo("orders/from/bean");
        assertThat(resolver.resolveExpression("#{@cfg.topic}", ctx, String.class)).isEqualTo("orders/from/bean");
    }

    @Test
    void method_context_uses_bean_resolver_when_present() throws Exception {
        SpelExpressionResolver resolver = new SpelExpressionResolver();
        BeanFactory bf = mock(BeanFactory.class);
        when(bf.getBean("cfg")).thenReturn(new Cfg());
        resolver.setBeanFactory(bf);

        java.lang.reflect.Method m = SpelExpressionResolverInternalsTest.class
                .getDeclaredMethod("sample", String.class);
        EvaluationContext ctx = resolver.createEvaluationContext(m, new Object[]{"O1"}, "RES", this);
        assertThat(resolver.resolveExpression("@cfg.max", ctx, Integer.class)).isEqualTo(7);
    }

    @SuppressWarnings("unused")
    private void sample(String orderId) { }

    @Test
    void numeric_and_boolean_literal_conversion() {
        SpelExpressionResolver resolver = new SpelExpressionResolver();
        org.springframework.expression.spel.support.StandardEvaluationContext ctx =
                new org.springframework.expression.spel.support.StandardEvaluationContext();

        assertThat(resolver.resolveExpression("100", ctx, Long.class)).isEqualTo(100L);
        assertThat(resolver.resolveExpression("3.5", ctx, Double.class)).isEqualTo(3.5);
        assertThat(resolver.resolveExpression("true", ctx, Boolean.class)).isTrue();
        // Unconvertible to a number falls back to null.
        assertThat(resolver.resolveExpression("notnum", ctx, Long.class)).isNull();
    }
}
