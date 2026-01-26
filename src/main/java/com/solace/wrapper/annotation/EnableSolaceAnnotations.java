package com.solace.wrapper.annotation;

import com.solace.wrapper.config.SolaceAnnotationConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * Annotation to enable Solace annotation processing.
 * Add this to your main application class or configuration class.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(SolaceAnnotationConfiguration.class)
public @interface EnableSolaceAnnotations {
}
