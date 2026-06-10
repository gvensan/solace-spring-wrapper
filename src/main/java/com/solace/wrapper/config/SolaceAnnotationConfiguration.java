package com.solace.wrapper.config;

import com.solace.wrapper.annotation.processor.SolaceConsumerProcessor;
import com.solace.wrapper.annotation.processor.SolacePublishAspect;
import com.solace.wrapper.annotation.processor.SolaceReplierProcessor;
import com.solace.wrapper.annotation.processor.SpelExpressionResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

/**
 * Configuration class for enabling Solace annotations.
 */
@Configuration
@EnableAspectJAutoProxy
public class SolaceAnnotationConfiguration {

    @Bean
    public SpelExpressionResolver spelExpressionResolver() {
        return new SpelExpressionResolver();
    }

    @Bean
    public SolacePublishAspect solacePublishAspect() {
        return new SolacePublishAspect();
    }

    @Bean
    public SolaceConsumerProcessor solaceConsumerProcessor() {
        return new SolaceConsumerProcessor();
    }

    @Bean
    public SolaceReplierProcessor solaceReplierProcessor() {
        return new SolaceReplierProcessor();
    }
}
