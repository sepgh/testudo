package com.github.sepgh.testudo.serialization;

import lombok.Getter;

@Getter
public enum DefaultType {
    LONG("long"), INT("int"), CHAR("char"), BOOLEAN("boolean"), FLOAT("float"), DOUBLE("double");
    private final String name;

    DefaultType(String name) {
        this.name = name;
    }
}
