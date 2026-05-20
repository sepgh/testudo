package io.github.sepgh.testudo.core.view;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class TimestampViewTest {

    private MockPage page;
    private TimestampView view;

    @BeforeEach
    void setUp() {
        page = new MockPage(64);
        view = new TimestampView(page);
        view.pointTo(0);
    }

    @Test
    void size_returnsEightByteWidth() {
        assertEquals(Long.BYTES, view.size());
    }

    @Test
    void setAndGet_roundTrip_epoch() {
        Instant epoch = Instant.EPOCH;
        view.set(epoch);
        assertEquals(epoch, view.get());
    }

    @Test
    void setAndGet_roundTrip_currentTime() {
        Instant now = Instant.ofEpochMilli(System.currentTimeMillis());
        view.set(now);
        assertEquals(now, view.get());
    }

    @Test
    void setAndGet_roundTrip_pastTimestamp() {
        Instant past = Instant.ofEpochMilli(1_000_000L);
        view.set(past);
        assertEquals(past, view.get());
    }

    @Test
    void setAndGet_roundTrip_farFutureTimestamp() {
        Instant future = Instant.ofEpochMilli(Long.MAX_VALUE);
        view.set(future);
        assertEquals(future, view.get());
    }

    @Test
    void setAndGet_roundTrip_negativeEpochMilli() {
        Instant beforeEpoch = Instant.ofEpochMilli(-1_000L);
        view.set(beforeEpoch);
        assertEquals(beforeEpoch, view.get());
    }

    @Test
    void set_marksPageDirty() {
        page.resetDirty();
        view.set(Instant.EPOCH);
        assertTrue(page.isDirty());
    }

    @Test
    void compareTo_lessThan() {
        Instant earlier = Instant.ofEpochMilli(1000L);
        Instant later = Instant.ofEpochMilli(2000L);
        view.set(earlier);
        assertTrue(view.compareTo(later) < 0);
    }

    @Test
    void compareTo_greaterThan() {
        Instant earlier = Instant.ofEpochMilli(1000L);
        Instant later = Instant.ofEpochMilli(2000L);
        view.set(later);
        assertTrue(view.compareTo(earlier) > 0);
    }

    @Test
    void compareTo_equal() {
        Instant ts = Instant.ofEpochMilli(5000L);
        view.set(ts);
        assertEquals(0, view.compareTo(ts));
    }

    @Test
    void pointTo_differentOffset_independentStorage() {
        Instant ts1 = Instant.ofEpochMilli(1111L);
        Instant ts2 = Instant.ofEpochMilli(2222L);

        view.pointTo(0);
        view.set(ts1);

        view.pointTo(Long.BYTES);
        view.set(ts2);

        view.pointTo(0);
        assertEquals(ts1, view.get());

        view.pointTo(Long.BYTES);
        assertEquals(ts2, view.get());
    }
}
