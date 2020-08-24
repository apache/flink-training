package com.ververica.flink.training.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Classes, methods or fields marked annotated with {@link DoNotChangeThis} should not be changed by training participants. They are either part of the required business logic, or usually outside of the scope of the Flink in a real-life scenario.
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.CONSTRUCTOR, ElementType.METHOD, ElementType.TYPE})
public @interface DoNotChangeThis {
}
