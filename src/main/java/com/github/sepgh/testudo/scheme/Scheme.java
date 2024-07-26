package com.github.sepgh.testudo.scheme;

import lombok.Builder;
import lombok.Data;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Data
public class Scheme {
    private String dbName;
    private int version;
    private List<Collection> collections;

    @Builder
    @Data
    public static class Collection {
        private int id;
        private String name;
        @Builder.Default
        private List<Field> fields = new ArrayList<>();

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Collection that = (Collection) o;
            return getId() == that.getId() && Objects.equals(getName(), that.getName()) && Objects.equals(getFields(), that.getFields());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getId(), getName(), getFields());
        }
    }

    @Data
    @Builder
    public static class Meta {
        private String comment;
        private String maxSize;
        private Integer max;
        private Integer min;
        @Builder.Default
        private String charset = StandardCharsets.UTF_8.name();

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Meta meta = (Meta) o;
            return getMax() == meta.getMax() && getMin() == meta.getMin() && Objects.equals(getComment(), meta.getComment()) && Objects.equals(getMaxSize(), meta.getMaxSize());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getComment(), getMaxSize(), getMax(), getMin());
        }
    }

    @Data
    @Builder
    public static class Field {
        private int id;
        private String name;
        private String type;
        private Meta meta;
        private boolean primary;
        private boolean index;
        private boolean indexUnique;
        private String defaultValue;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Field field = (Field) o;
            return getId() == field.getId() && isPrimary() == field.isPrimary() && isIndex() == field.isIndex() && isIndexUnique() == field.isIndexUnique() && Objects.equals(getName(), field.getName()) && Objects.equals(getType(), field.getType()) && Objects.equals(getMeta(), field.getMeta());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getId(), getName(), getType(), getMeta(), isPrimary(), isIndex(), isIndexUnique());
        }
    }
}
