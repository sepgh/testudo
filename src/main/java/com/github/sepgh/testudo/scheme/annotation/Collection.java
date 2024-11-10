package com.github.sepgh.testudo.scheme.annotation;

public @interface Collection {
    int id();
    String name() default "";
}
