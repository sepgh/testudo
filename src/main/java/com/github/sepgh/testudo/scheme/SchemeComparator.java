package com.github.sepgh.testudo.scheme;

import lombok.AllArgsConstructor;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@AllArgsConstructor
public class SchemeComparator {
    private final Scheme oldScheme;
    private final Scheme newScheme;

    // Todo: compare db name
    public boolean compare(SchemeComparisonListener comparisonListener) {

        // Cases where there are no changes
        if (oldScheme == newScheme || oldScheme.getVersion() == newScheme.getVersion()) {
            return false;
        }

        if (oldScheme.getCollections().size() == newScheme.getCollections().size()){
            boolean different = false;
            for (int i = 0; i < oldScheme.getCollections().size(); i++) {
                if (!oldScheme.getCollections().get(i).equals(newScheme.getCollections().get(i))) {
                    different = true;
                    break;
                }
            }
            if (!different) {
                return false;
            }
        }


        // Comparing each collection fields if still exists otherwise removing collection
        for (int i = 0; i < oldScheme.getCollections().size(); i++) {
            for (Scheme.Collection oldSchemeCollection : oldScheme.getCollections()) {
                Optional<Scheme.Collection> optionalCollection = newScheme.getCollections().stream().filter(collection -> collection.getId() == oldSchemeCollection.getId()).findFirst();

                if (optionalCollection.isEmpty()) {
                    comparisonListener.onChange(
                            DifferenceReason.COLLECTION_REMOVED,
                            oldSchemeCollection,
                            null,
                            null,
                            null
                    );
                    continue;
                }

                Scheme.Collection newCollection = optionalCollection.get();
                if (oldScheme.equals(newScheme))
                    continue;

                List<Scheme.Field> newCollectionUnComparedFields = new ArrayList<>(newCollection.getFields());

                oldSchemeCollection.getFields().forEach(field -> {
                    newCollectionUnComparedFields.remove(field);

                    Optional<Scheme.Field> optionalCollectionField = newCollection.getFields().stream().filter(field1 -> field1.getId() == field.getId()).findFirst();
                    if (optionalCollectionField.isEmpty()) {
                        comparisonListener.onChange(
                                DifferenceReason.FIELD_REMOVED,
                                oldSchemeCollection,
                                newCollection,
                                field,
                                null
                        );
                        return;
                    }

                    Scheme.Field newCollectionField = optionalCollectionField.get();
                    if (newCollectionField.isIndex() != field.isIndex() || newCollectionField.isIndexUnique() != field.isIndexUnique()){
                        comparisonListener.onChange(
                                DifferenceReason.FIELD_INDEX_CHANGED,
                                oldSchemeCollection,
                                newCollection,
                                field,
                                newCollectionField
                        );
                    }

                    if (!newCollectionField.getMeta().equals(field.getMeta())){
                        comparisonListener.onChange(
                                DifferenceReason.FIELD_META_CHANGED,
                                oldSchemeCollection,
                                newCollection,
                                field,
                                newCollectionField
                        );
                    }

                });

                newCollectionUnComparedFields.forEach(field -> {
                    comparisonListener.onChange(
                            DifferenceReason.NEW_FIELD, oldSchemeCollection, newCollection, null, field
                    );
                });
            }
        }

        return true;
    }

    public interface SchemeComparisonListener {
        void onChange(DifferenceReason differenceReason, @Nullable Scheme.Collection oldCollection, @Nullable Scheme.Collection newCollection, @Nullable Scheme.Field oldField, @Nullable Scheme.Field newField);
    }

    public enum DifferenceReason {
        NEW_FIELD, FIELD_META_CHANGED, FIELD_TYPE_CHANGED, FIELD_INDEX_CHANGED, FIELD_REMOVED, NEW_COLLECTION, COLLECTION_REMOVED
    }

}
