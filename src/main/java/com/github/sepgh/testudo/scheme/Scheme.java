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
        private int maxLength;
        @Builder.Default
        private String charset = StandardCharsets.UTF_8.name();

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Meta meta = (Meta) o;
            return Objects.equals(getComment(), meta.getComment()) && Objects.equals(getMaxLength(), meta.getMaxLength());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getComment(), getMaxLength());
        }
    }


    public static class ImmutableDefaultMeta extends Meta {
        public static Meta INSTANCE = ImmutableDefaultMeta.builder().build();

        private ImmutableDefaultMeta(String comment, int maxLength, String charset) {
            super(comment, maxLength, charset);
        }

        @Override
        public void setComment(String comment) {
            super.setComment(comment);
        }

        @Override
        public void setMaxLength(int maxLength) {
            super.setMaxLength(maxLength);
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
        private boolean autoIncrement;
        @Builder.Default
        private boolean lowCardinality = false;
        @Builder.Default
        private boolean nullable = false;
        private String defaultValue;
        private transient String objectFieldName;

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
