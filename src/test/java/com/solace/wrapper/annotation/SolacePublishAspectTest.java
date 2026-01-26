package com.solace.wrapper.annotation;

import com.solace.wrapper.annotation.processor.SolacePublishAspect;
import com.solace.wrapper.publisher.MessageProperties;
import com.solace.wrapper.publisher.SolacePublisher;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SolacePublishAspectTest {

    private static final Logger log = LoggerFactory.getLogger(SolacePublishAspectTest.class);

    static class TestBean {
        @SolacePublish(destination = "orders/new", clientName = "pub-client")
        public String publish() { return "payload"; }
    }

    @Test
    void publish_uses_client_name_override() throws Throwable {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: publish_uses_client_name_override\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that the @SolacePublish aspect correctly extracts and uses\n" +
                "  the clientName attribute when publishing messages.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. @SolacePublish(clientName = \"...\") - custom client name for connection isolation\n" +
                "  2. AOP aspect interception - @Around advice captures method return value\n" +
                "  3. publishWithContext() - passes clientName to publisher for connection routing\n" +
                "\n" +
                "WHY clientName MATTERS:\n" +
                "  - Allows multiple independent publisher connections per application\n" +
                "  - Each clientName gets its own session (connection isolation)\n" +
                "  - Useful for separating high-priority vs low-priority publishing\n" +
                "  - Enables different client identities for audit/tracking\n" +
                "\n" +
                "ANNOTATION BEING TESTED:\n" +
                "  @SolacePublish(destination = \"orders/new\", clientName = \"pub-client\")\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Aspect intercepts method, extracts return value as message payload\n" +
                "  - publishWithContext() called with clientName='pub-client'\n" +
                "  - Publisher key is 'client:pub-client' for connection routing\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Creating SolacePublishAspect with mock publisher");
        SolacePublishAspect aspect = new SolacePublishAspect();
        SolacePublisher publisher = mock(SolacePublisher.class);
        aspect.solacePublisher = publisher;
        aspect.setSpelResolver(new com.solace.wrapper.annotation.processor.SpelExpressionResolver());

        log.info("STEP 2: Setting up TestBean with @SolacePublish(destination='orders/new', clientName='pub-client')");
        TestBean bean = new TestBean();
        Method method = TestBean.class.getMethod("publish");
        SolacePublish annotation = method.getAnnotation(SolacePublish.class);

        log.info("STEP 3: Creating mock ProceedingJoinPoint to simulate AOP interception");
        ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        MethodSignature signature = mock(MethodSignature.class);
        when(joinPoint.proceed()).thenReturn("payload");
        when(joinPoint.getSignature()).thenReturn(signature);
        when(signature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[0]);

        log.info("STEP 4: Invoking aspect handleSolacePublish method");
        log.info("        This simulates Spring AOP intercepting a method call");
        Object result = aspect.handleSolacePublish(joinPoint, annotation);
        assertThat(result).isEqualTo("payload");
        log.info("        Method returned: '{}' (this becomes the message payload)", result);

        log.info("STEP 5: Verifying publishWithContext was called with correct parameters");
        verify(publisher).publishWithContext(
                eq("orders/new"),
                eq("payload"),
                any(MessageProperties.class),
                eq(false),
                eq("pub-client"),
                eq("client:pub-client")
        );

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Method return value: 'payload' (used as message body)\n" +
                "  Destination: 'orders/new' (from annotation)\n" +
                "  ClientName: 'pub-client' (from annotation)\n" +
                "  Publisher key: 'client:pub-client' (derived from clientName)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  The SolacePublishAspect correctly:\n" +
                "  1. Intercepted the method call via AOP @Around advice\n" +
                "  2. Extracted the return value as message payload\n" +
                "  3. Passed clientName to publishWithContext for connection isolation\n" +
                "  4. Derived publisher key as 'client:{clientName}' for routing\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }
}
