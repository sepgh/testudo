package com.github.sepgh.testudo.scheme.annotation;

public @interface Index {
    boolean enable() default true;
    boolean unique() default false;
    boolean primary() default false;
    boolean lowCardinality() default false;
}
