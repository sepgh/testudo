package io.github.sepgh.testudo.core.model;

public record FilePointer(int page, int slot) implements Comparable<FilePointer> {
    @Override
    public int compareTo(FilePointer o) {
        int positionComparison = Integer.compare(page, o.page);

        if (positionComparison != 0) {
            return positionComparison;
        }

        return Integer.compare(slot, o.slot);
    }
}
