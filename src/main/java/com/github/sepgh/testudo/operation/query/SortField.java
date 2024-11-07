package com.github.sepgh.testudo.operation.query;

import com.github.sepgh.testudo.scheme.Scheme;

public record SortField(Scheme.Field field, Order order) {

}
