package com.github.sepgh.testudo.functional;

@FunctionalInterface
public interface CheckedFunction<I, O, E extends Exception> {
    O apply(I in) throws E;
}
