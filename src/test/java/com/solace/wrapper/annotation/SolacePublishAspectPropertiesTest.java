package com.solace.wrapper.annotation;

import com.solace.wrapper.annotation.processor.SolacePublishAspect;
import com.solace.wrapper.annotation.processor.SpelExpressionResolver;
import com.solace.wrapper.publisher.MessageProperties;
import com.solace.wrapper.publisher.SolacePublisher;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers the {@link SolacePublishAspect} message-property mapping, condition handling, async
 * dispatch, delivery-mode selection, and null-result short-circuit.
 */
class SolacePublishAspectPropertiesTest {

    static class Beans {
        @SolacePublish(
                destination = "orders/new",
                correlationId = "cid-1",
                replyTo = "reply/topic",
                timeToLive = 5000,
                priority = 4,
                applicationMessageType = "order",
                applicationMessageId = "amid-1",
                elidingEligible = "true",
                classOfService = "2",
                messageExpiration = "12345",
                sequenceNumber = "7",
                userProperties = {"region=us-east", "tier=gold"})
        public String full() { return "payload"; }

        @SolacePublish(destination = "orders/x", condition = "#result == 'skip'")
        public String conditional() { return "no"; }

        @SolacePublish(destination = "orders/async", deliveryMode = "PERSISTENT", async = true)
        public String asyncPersistent() { return "payload"; }

        @SolacePublish(destination = "orders/null")
        public String returnsNull() { return null; }
    }

    private SolacePublishAspect aspect(SolacePublisher publisher) {
        SolacePublishAspect a = new SolacePublishAspect();
        a.solacePublisher = publisher;
        a.setSpelResolver(new SpelExpressionResolver());
        return a;
    }

    private ProceedingJoinPoint joinPoint(Method method, Object result) throws Throwable {
        ProceedingJoinPoint jp = mock(ProceedingJoinPoint.class);
        MethodSignature sig = mock(MethodSignature.class);
        when(jp.proceed()).thenReturn(result);
        when(jp.getSignature()).thenReturn(sig);
        when(sig.getMethod()).thenReturn(method);
        when(jp.getArgs()).thenReturn(new Object[0]);
        return jp;
    }

    @Test
    void maps_all_message_properties() throws Throwable {
        SolacePublisher publisher = mock(SolacePublisher.class);
        Method m = Beans.class.getMethod("full");
        aspect(publisher).handleSolacePublish(joinPoint(m, "payload"), m.getAnnotation(SolacePublish.class));

        ArgumentCaptor<MessageProperties> captor = ArgumentCaptor.forClass(MessageProperties.class);
        verify(publisher).publishWithContext(eq("orders/new"), eq("payload"), captor.capture(),
                eq(false), isNull(), any());

        MessageProperties p = captor.getValue();
        assertThat(p.getCorrelationId()).isEqualTo("cid-1");
        assertThat(p.getReplyTo()).isEqualTo("reply/topic");
        assertThat(p.getTimeToLive()).isEqualTo(5000);
        assertThat(p.getPriority()).isEqualTo(4);
        assertThat(p.getApplicationMessageType()).isEqualTo("order");
        assertThat(p.getApplicationMessageId()).isEqualTo("amid-1");
        assertThat(p.getElidingEligible()).isTrue();
        assertThat(p.getClassOfService()).isEqualTo(2);
        assertThat(p.getMessageExpiration()).isEqualTo(12345L);
        assertThat(p.getSequenceNumber()).isEqualTo(7L);
        assertThat(p.getUserProperties()).containsEntry("region", "us-east").containsEntry("tier", "gold");
    }

    @Test
    void condition_false_skips_publish() throws Throwable {
        SolacePublisher publisher = mock(SolacePublisher.class);
        Method m = Beans.class.getMethod("conditional");
        aspect(publisher).handleSolacePublish(joinPoint(m, "no"), m.getAnnotation(SolacePublish.class));

        verify(publisher, never()).publishWithContext(any(), any(), any(), anyBoolean(), any(), any());
    }

    @Test
    void async_persistent_uses_async_publish() throws Throwable {
        SolacePublisher publisher = mock(SolacePublisher.class);
        Method m = Beans.class.getMethod("asyncPersistent");
        aspect(publisher).handleSolacePublish(joinPoint(m, "payload"), m.getAnnotation(SolacePublish.class));

        verify(publisher).publishWithContextAsync(eq("orders/async"), eq("payload"), any(),
                eq(true), isNull(), any());
    }

    @Test
    void null_result_skips_publish() throws Throwable {
        SolacePublisher publisher = mock(SolacePublisher.class);
        Method m = Beans.class.getMethod("returnsNull");
        Object out = aspect(publisher).handleSolacePublish(joinPoint(m, null), m.getAnnotation(SolacePublish.class));

        assertThat(out).isNull();
        verify(publisher, never()).publishWithContext(any(), any(), any(), anyBoolean(), any(), any());
    }
}
