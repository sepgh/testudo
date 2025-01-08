package com.github.sepgh.testudo.operation.query;

import lombok.Getter;


@Getter
public enum Operation {
    EQ(true), NEQ(true), GT(true), LT(true), GTE(true), LTE(true), IS_NULL(false);
    private final boolean requiresValue;

    Operation(boolean requiresValue) {
        this.requiresValue = requiresValue;
    }
}
