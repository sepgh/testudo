package com.github.sepgh.testudo.scheme;

import com.github.sepgh.testudo.scheme.annotation.Collection;
import com.github.sepgh.testudo.scheme.annotation.Field;
import com.github.sepgh.testudo.scheme.annotation.Index;
import com.github.sepgh.testudo.serialization.SerializerRegistry;
import com.google.common.base.Preconditions;
import lombok.Getter;

import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Stream;

@Getter
public class ModelToCollectionConverter {

    private final Class<?> modelClass;

    public ModelToCollectionConverter(Class<?> modelClass) {
        this.modelClass = modelClass;
    }

    public Scheme.Collection toCollection() {
        Scheme.Collection collection = this.extractCollection();
        List<Scheme.Field> fields = this.extractFields();
        fields.sort(Comparator.comparingInt(Scheme.Field::getId));
        collection.setFields(fields);
        return collection;
    }

    private List<Scheme.Field> extractFields() {
        Stream<java.lang.reflect.Field> stream = Arrays.stream(this.modelClass.getDeclaredFields())
                .filter(field -> Modifier.isPrivate(field.getModifiers()))
                .filter(f -> f.isAnnotationPresent(Field.class));

        List<Scheme.Field> fields = new ArrayList<>();
        stream.forEach(field -> {
            Optional<Scheme.Field> optional = this.getSchemeField(field);
            optional.ifPresent(fields::add);
        });

        return fields;
    }

    private Scheme.Collection extractCollection() {
        Optional<Annotation> collectionOptional = Arrays.stream(modelClass.getAnnotations())
                .filter(annotation -> annotation.annotationType().equals(Collection.class))
                .findFirst();

        if (collectionOptional.isEmpty()) {
            // Todo: err
        }

        Collection collection = (Collection) collectionOptional.get();
        return Scheme.Collection.builder()
                .id(collection.id())
                .name(collection.name())
                .build();
    }

    private Optional<Scheme.Field> getSchemeField(final java.lang.reflect.Field field) {
        Field fieldAnnotation = field.getAnnotation(Field.class);
        Scheme.Field schemeField = Scheme.Field.builder()
                .id(fieldAnnotation.id())
                .type(getFieldType(field, fieldAnnotation))
                .name(fieldAnnotation.name().isEmpty() ? field.getName() : fieldAnnotation.name())
                .nullable(fieldAnnotation.nullable())
                .defaultValue(fieldAnnotation.defaultValue())
                .objectFieldName(field.getName())
                .meta(Scheme.Meta.builder()
                        .charset(fieldAnnotation.charset())
                        .comment(fieldAnnotation.comment())
                        .maxLength(fieldAnnotation.maxLength())
                        .build())
                .build();

        List<Annotation> objFieldAnnotations = Arrays.asList(field.getAnnotations());

        Optional<Annotation> indexAnnotationOptional = objFieldAnnotations.stream().filter(annotation -> annotation.annotationType().equals(Index.class)).findFirst();
        if (indexAnnotationOptional.isEmpty())
            return Optional.of(schemeField);

        Index index = (Index) indexAnnotationOptional.get();

        schemeField.setIndex(Scheme.Index.fromAnnotation(index));
        return Optional.of(schemeField);
    }

    private String getFieldType(final java.lang.reflect.Field field, Field fieldAnnotation) {
        if (fieldAnnotation.type().isEmpty()) {
            Preconditions.checkArgument(Arrays.asList(field.getType().getInterfaces()).contains(Comparable.class), "Fields must have a type of Comparable, but %s doesn't.", field.getName());
            Optional<String> optional = SerializerRegistry.getInstance().getTypeOfClass(field.getType());
            if (optional.isPresent()) {
                return optional.get();
            }
            throw new RuntimeException("Can't find a default field type for class " + field.getType().getTypeName()); // OK
        }

        final String annotatedType = fieldAnnotation.type();
        Class<?> type = SerializerRegistry.getInstance().getSerializer(annotatedType).getType();

        if (type.equals(field.getType())) {
            return annotatedType;
        } else {
            throw new RuntimeException("Annotated type '%s' is not compatible with field type '%s'".formatted(annotatedType, field.getType().getTypeName()));  // OK
        }
    }


}
