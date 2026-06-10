package com.solace.wrapper.annotation;

import com.solace.wrapper.annotation.processor.SolaceConsumerProcessor;
import com.solace.wrapper.annotation.processor.SolacePublishAspect;
import com.solace.wrapper.annotation.processor.SolaceReplierProcessor;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.consumer.SolaceConsumerManager;
import com.solace.wrapper.publisher.SolacePublisher;
import com.solace.wrapper.serialization.MessageSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Verifies that @EnableSolaceAnnotations imports the annotation configuration
 * and registers the publish aspect and consumer processor beans.
 */
public class EnableSolaceAnnotationsTest {

    private static final Logger log = LoggerFactory.getLogger(EnableSolaceAnnotationsTest.class);

    @Configuration
    @EnableSolaceAnnotations
    static class TestConfig {
        // Provide lightweight mocks to satisfy autowiring without invoking Solace connections
        @Bean SolacePublisher solacePublisher() { return mock(SolacePublisher.class); }
        @Bean SolaceConsumerManager solaceConsumerManager() { return mock(SolaceConsumerManager.class); }
        @Bean SolaceConnectionManager solaceConnectionManager() { return mock(SolaceConnectionManager.class); }
        @Bean MessageSerializer messageSerializer() { return mock(MessageSerializer.class); }
    }

    @Test
    void enable_annotation_registers_aspect_and_processor() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: enable_annotation_registers_aspect_and_processor\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that @EnableSolaceAnnotations meta-annotation correctly imports\n" +
                "  the SolaceAnnotationConfiguration and registers all required beans.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. @EnableSolaceAnnotations - activates annotation processing\n" +
                "  2. SolacePublishAspect - AOP aspect for @SolacePublish methods\n" +
                "  3. SolaceConsumerProcessor - BeanPostProcessor for @SolaceConsumer methods\n" +
                "\n" +
                "HOW IT WORKS:\n" +
                "  @EnableSolaceAnnotations has @Import(SolaceAnnotationConfiguration.class)\n" +
                "  which registers the aspect and processor as Spring beans.\n" +
                "  This is the standard Spring pattern for enabling features via annotations.\n" +
                "\n" +
                "WHY THIS MATTERS:\n" +
                "  - Users add @EnableSolaceAnnotations to their @Configuration class\n" +
                "  - All @SolacePublish and @SolaceConsumer annotations 'just work'\n" +
                "  - No manual bean registration required\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - SolacePublishAspect bean is present in context\n" +
                "  - SolaceConsumerProcessor bean is present in context\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Creating Spring context with @EnableSolaceAnnotations on TestConfig");
        try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(TestConfig.class)) {
            log.info("STEP 2: Verifying SolacePublishAspect bean is registered");
            String[] aspectBeans = ctx.getBeanNamesForType(SolacePublishAspect.class);
            assertThat(aspectBeans).isNotEmpty();
            log.info("        Found SolacePublishAspect beans: " + String.join(", ", aspectBeans));

            log.info("STEP 3: Verifying SolaceConsumerProcessor bean is registered");
            String[] processorBeans = ctx.getBeanNamesForType(SolaceConsumerProcessor.class);
            assertThat(processorBeans).isNotEmpty();
            log.info("        Found SolaceConsumerProcessor beans: " + String.join(", ", processorBeans));

            log.info("STEP 4: Verifying SolaceReplierProcessor bean is registered");
            assertThat(ctx.getBeanNamesForType(SolaceReplierProcessor.class)).isNotEmpty();

            log.info("\n───────────────────────────────────────────────────────────────\n" +
                    "RESULT:\n" +
                    "  SolacePublishAspect registered: true (beans: " + String.join(", ", aspectBeans) + ")\n" +
                    "  SolaceConsumerProcessor registered: true (beans: " + String.join(", ", processorBeans) + ")\n" +
                    "\n" +
                    "ANALYSIS:\n" +
                    "  @EnableSolaceAnnotations correctly imports SolaceAnnotationConfiguration\n" +
                    "  which registers both the AOP aspect and bean post-processor.\n" +
                    "  This enables declarative messaging with @SolacePublish and @SolaceConsumer.\n" +
                    "\n" +
                    "STATUS: PASS\n" +
                    "───────────────────────────────────────────────────────────────\n");
        }
    }
}
