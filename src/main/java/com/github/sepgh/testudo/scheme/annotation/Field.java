package com.github.sepgh.testudo.scheme.annotation;

public @interface Field {
    int id();
    String name() default "";
    String type() default "";
    boolean nullable() default false;

    // Meta
    String comment() default "";
    String defaultValue() default "";
    int maxLength() default -1;
    String charset() default "UTF-8";
}
