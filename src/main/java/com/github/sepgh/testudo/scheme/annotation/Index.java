package com.github.sepgh.testudo.scheme.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Index {
    boolean enable() default true;
    boolean unique() default false;
    boolean primary() default false;
    boolean lowCardinality() default false;
}
