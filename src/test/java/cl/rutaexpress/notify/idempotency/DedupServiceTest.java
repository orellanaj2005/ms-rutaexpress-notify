package cl.rutaexpress.notify.idempotency;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class DedupServiceTest {

    @Test
    void marksEventProcessedAndDetectsDuplicate() {
        DedupService dedupService = new DedupService();

        assertThat(dedupService.isDuplicate("evt-1")).isFalse();

        dedupService.markProcessed("evt-1");

        assertThat(dedupService.isDuplicate("evt-1")).isTrue();
        assertThat(dedupService.isDuplicate("evt-never-seen")).isFalse();
    }

    @Test
    void cleanupPurgesEntriesOlderThan24Hours() {
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        DedupService dedupService = new DedupService(clock);

        dedupService.markProcessed("old-event");

        // advance 25 hours: old-event should be purged
        clock.advance(Duration.ofHours(25));
        dedupService.markProcessed("recent-event");

        dedupService.cleanup();

        assertThat(dedupService.isDuplicate("old-event")).isFalse();
        assertThat(dedupService.isDuplicate("recent-event")).isTrue();
    }

    /** Minimal settable {@link Clock} for testing time-based cleanup without waiting. */
    private static final class MutableClock extends Clock {
        private final AtomicReference<Instant> instant;

        MutableClock(Instant start) {
            this.instant = new AtomicReference<>(start);
        }

        void advance(Duration duration) {
            instant.updateAndGet(current -> current.plus(duration));
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant.get();
        }
    }
}
