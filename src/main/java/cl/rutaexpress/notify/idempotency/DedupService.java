package cl.rutaexpress.notify.idempotency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory, single-instance idempotency/dedup store keyed by {@code eventId}.
 *
 * <p><b>Limitation (documented per spec, "sin DB"):</b> this store is a plain
 * {@link ConcurrentHashMap} local to this JVM. It resets on restart and does NOT provide correct
 * deduplication if notify-svc is ever scaled to more than one replica (each instance would have
 * its own independent map, so the same eventId could be processed once per replica). A
 * Redis-backed or DB-backed store would be required to dedup correctly across replicas; that is
 * out of scope here since no database is available for this service.
 */
@Service
public class DedupService {

    private static final Logger log = LoggerFactory.getLogger(DedupService.class);
    private static final Duration RETENTION = Duration.ofHours(24);

    private final Map<String, Instant> processedAt = new ConcurrentHashMap<>();
    private final Clock clock;

    public DedupService() {
        this(Clock.systemUTC());
    }

    public DedupService(Clock clock) {
        this.clock = clock;
    }

    public boolean isDuplicate(String eventId) {
        return processedAt.containsKey(eventId);
    }

    public void markProcessed(String eventId) {
        processedAt.put(eventId, Instant.now(clock));
    }

    /**
     * Purges entries older than 24h to bound memory growth. Runs every hour.
     */
    @Scheduled(fixedRate = 60 * 60 * 1000L)
    public void cleanup() {
        Instant cutoff = Instant.now(clock).minus(RETENTION);
        int before = processedAt.size();
        processedAt.values().removeIf(processedInstant -> processedInstant.isBefore(cutoff));
        int removed = before - processedAt.size();
        if (removed > 0) {
            log.info("Dedup cleanup: purged {} entries older than 24h ({} remaining)", removed, processedAt.size());
        }
    }

    int size() {
        return processedAt.size();
    }
}
