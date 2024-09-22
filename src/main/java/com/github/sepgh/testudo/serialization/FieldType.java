package com.github.sepgh.testudo.serialization;

import lombok.Getter;

@Getter
public enum FieldType {
    LONG("long"), UNSIGNED_LONG("ulong"), INT("int"), CHAR("char"), BOOLEAN("boolean"), FLOAT("float"), DOUBLE("double");
    private final String name;

    FieldType(String name) {
        this.name = name;
    }
}
