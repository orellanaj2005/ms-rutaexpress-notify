package cl.rutaexpress.notify.messaging;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

/**
 * Envelope shape published by ms-rutaexpress-shipments onto {@code cmd.direct}/{@code cmd.topic}.
 * {@code payload} is kept as a raw {@link JsonNode} so this service does not need to share
 * Java DTOs with the producer — each handler reads only the fields it needs, defensively.
 */
public record CommandEnvelope(
        String type,
        String eventId,
        Instant timestamp,
        String traceId,
        String correlationId,
        JsonNode payload) {
}
