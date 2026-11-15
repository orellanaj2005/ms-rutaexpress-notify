package cl.rutaexpress.notify.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

/**
 * Sobre de los eventos de {@code shipments.events} (contrato en docs/eventos.md de shipments).
 * Ignora los campos desconocidos para que el productor pueda agregar campos sin romper a notify.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ShipmentEventEnvelope(
        String eventId,
        String type,
        int version,
        String aggregateId,
        Instant timestamp,
        String traceId,
        String correlationId,
        String producer,
        JsonNode payload) {
}
