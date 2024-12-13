package com.github.sepgh.testudo.scheme;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Scheme {
    private String dbName;
    private Integer version;
    @Builder.Default
    private List<Collection> collections = new ArrayList<>();

    public Optional<Collection> getCollection(String name) {
        return collections.stream().filter(c -> Objects.equals(c.getName(), name)).findFirst();
    }

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
            return fields.stream().filter(field -> field.index != null && field.index.isPrimary()).findFirst();
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
    public static class Index {
        private boolean primary;
        private boolean unique;
        private boolean autoIncrement;
        private boolean lowCardinality;

        public boolean isUnique() {
            return primary || unique;
        }

        public static Index fromAnnotation(com.github.sepgh.testudo.scheme.annotation.Index annotation){
            if (annotation.enable())
                return Index.builder()
                        .primary(annotation.primary())
                        .unique(annotation.unique())
                        .autoIncrement(annotation.autoIncrement())
                        .lowCardinality(annotation.lowCardinality())
                        .build();
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Index index = (Index) o;
            return isPrimary() == index.isPrimary() && isUnique() == index.isUnique() && isAutoIncrement() == index.isAutoIncrement() && isLowCardinality() == index.isLowCardinality();
        }

        @Override
        public int hashCode() {
            return Objects.hash(isPrimary(), isUnique(), isAutoIncrement(), isLowCardinality());
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
        private Index index;
        private boolean nullable;
        private String defaultValue;
        private transient String objectFieldName;

        public boolean isIndexed() {
            return this.index != null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Field field = (Field) o;
            return getId() == field.getId() && Objects.equals(getName(), field.getName()) && Objects.equals(getType(), field.getType()) && Objects.equals(getMeta(), field.getMeta());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getId(), getName(), getType(), getMeta(), getIndex());
        }
    }
}
