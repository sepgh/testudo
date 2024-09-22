package com.github.sepgh.testudo.scheme;

import lombok.Builder;
import lombok.Data;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Data
@Builder
public class Scheme {
    private String dbName;
    private Integer version;
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

        public Optional<Field> getPrimaryField(){
            return fields.stream().filter(Field::isPrimary).findFirst();
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


    public static class ImmutableDefaultMeta extends Meta {
        public static Meta INSTANCE = ImmutableDefaultMeta.builder().build();

        private ImmutableDefaultMeta(String comment, String maxSize, Integer max, Integer min, String charset) {
            super(comment, maxSize, max, min, charset);
        }

        @Override
        public void setComment(String comment) {
            super.setComment(comment);
        }

        @Override
        public void setMaxSize(String maxSize) {
            super.setMaxSize(maxSize);
        }

        @Override
        public void setMax(Integer max) {
            super.setMax(max);
        }

        @Override
        public void setMin(Integer min) {
            super.setMin(min);
        }

        @Override
        public void setCharset(String charset) {
            super.setCharset(charset);
        }
    }

    @Data
    @Builder
    public static class Field {
        private int id;
        private String name;
        private String type;
        @Builder.Default
        private Meta meta = Meta.builder().build();
        private boolean primary;
        private boolean index;
        private boolean indexUnique;
        private String defaultValue;
        @Builder.Default
        private boolean supportZero = true;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Field field = (Field) o;
            return getId() == field.getId() && isPrimary() == field.isPrimary() && isIndex() == field.isIndex() && Objects.equals(getName(), field.getName()) && Objects.equals(getType(), field.getType()) && Objects.equals(getMeta(), field.getMeta());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getId(), getName(), getType(), getMeta(), isPrimary(), isIndex());
        }
    }
}
