package com.github.sepgh.testudo.scheme.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Field {
    int id();
    String name() default "";
    String type() default "";
    boolean nullable() default false;
    String defaultValue() default "";

    // Meta
    String comment() default "";
    int maxLength() default -1;
    String charset() default "UTF-8";
}
