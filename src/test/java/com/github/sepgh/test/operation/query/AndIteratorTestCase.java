package com.github.sepgh.test.operation.query;

import com.github.sepgh.testudo.operation.query.AndIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class AndIteratorTestCase {

    @Test
    public void notExisting() {
        List<Integer> l1 = Arrays.asList(5, 2, 10, 1, 19);
        List<Integer> l2 = Arrays.asList(100, 200);

        AndIterator<Integer> ai = new AndIterator<>(
                Arrays.asList(l1.iterator(), l2.iterator())
        );
        Assertions.assertFalse(ai.hasNext());
    }

    @Test
    public void existing() {
        List<Integer> l1 = Arrays.asList(5, 2, 10, 1, 19);
        List<Integer> l2 = Arrays.asList(100, 200, 10, 5);

        AndIterator<Integer> ai = new AndIterator<>(
                Arrays.asList(l1.iterator(), l2.iterator())
        );
        Assertions.assertTrue(ai.hasNext());
        Assertions.assertEquals(ai.next(), 5);
        Assertions.assertTrue(ai.hasNext());
        Assertions.assertEquals(ai.next(), 10);
    }

    @Test
    public void existing_threeLists() {
        List<Integer> l1 = Arrays.asList(5, 2, 10, 1, 19);
        List<Integer> l2 = Arrays.asList(100, 200, 10, 5, 19);
        List<Integer> l3 = Arrays.asList(19, 800, 10, 5);

        AndIterator<Integer> ai = new AndIterator<>(
                Arrays.asList(l1.iterator(), l2.iterator(), l3.iterator())
        );
        Assertions.assertTrue(ai.hasNext());
        Assertions.assertEquals(ai.next(), 5);
        Assertions.assertTrue(ai.hasNext());
        Assertions.assertEquals(ai.next(), 10);
        Assertions.assertTrue(ai.hasNext());
        Assertions.assertEquals(ai.next(), 19);
    }

}
