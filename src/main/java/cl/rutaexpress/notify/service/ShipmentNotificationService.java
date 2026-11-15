package cl.rutaexpress.notify.service;

import cl.rutaexpress.notify.config.KafkaNotifyProperties;
import cl.rutaexpress.notify.config.RabbitProperties;
import cl.rutaexpress.notify.consumer.common.NonRecoverableMessageException;
import cl.rutaexpress.notify.idempotency.DedupService;
import cl.rutaexpress.notify.messaging.ShipmentEventEnvelope;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Convierte los {@code ShipmentStatusChanged} de Kafka en el comando {@code EmailSendCommand} que
 * notify ya procesa por RabbitMQ. Kafka alimenta el flujo de comandos sin reemplazarlo: el comando
 * entra por {@code cmd.direct} con {@code email.send} y sigue la misma ruta de colas, reintentos y DLQ.
 *
 * <p>El comando usa el id {@code email-<shipmentId>-<status>}, el mismo que shipments usa para el
 * correo que publica directo por RabbitMQ. Así, si llegan los dos, {@code EmailCommandListener}
 * descarta el segundo por deduplicación y el cliente recibe un solo correo.
 */
@Service
public class ShipmentNotificationService {

    public static final String STATUS_CHANGED = "ShipmentStatusChanged";
    static final String EMAIL_COMMAND = "EmailSendCommand";

    private static final Logger log = LoggerFactory.getLogger(ShipmentNotificationService.class);

    private final ObjectMapper objectMapper;
    private final RabbitTemplate rabbitTemplate;
    private final DedupService dedupService;
    private final KafkaNotifyProperties kafkaProperties;
    private final RabbitProperties rabbitProperties;

    public ShipmentNotificationService(ObjectMapper objectMapper,
                                       RabbitTemplate rabbitTemplate,
                                       DedupService dedupService,
                                       KafkaNotifyProperties kafkaProperties,
                                       RabbitProperties rabbitProperties) {
        this.objectMapper = objectMapper;
        this.rabbitTemplate = rabbitTemplate;
        this.dedupService = dedupService;
        this.kafkaProperties = kafkaProperties;
        this.rabbitProperties = rabbitProperties;
    }

    /**
     * Procesa un mensaje de {@code shipments.events}. Lanza {@link NonRecoverableMessageException} si
     * el mensaje es inválido (va directo a notify.DLT) y deja pasar cualquier otra excepción para que
     * el error handler reintente.
     */
    public void handle(String rawValue) {
        ShipmentEventEnvelope envelope = parse(rawValue);

        if (!STATUS_CHANGED.equals(envelope.type())) {
            log.debug("eventId={} type={} no genera notificación, se ignora", envelope.eventId(), envelope.type());
            return;
        }
        if (dedupService.isDuplicate(envelope.eventId())) {
            log.info("eventId={} ya fue procesado, se ignora (duplicado)", envelope.eventId());
            return;
        }

        JsonNode payload = envelope.payload();
        String status = text(payload, "status");
        String shipmentId = text(payload, "shipmentId");
        if (status == null || shipmentId == null) {
            throw new NonRecoverableMessageException("ShipmentStatusChanged sin shipmentId o status");
        }

        if (kafkaProperties.emailStates().contains(status)) {
            String recipientEmail = text(payload, "recipientEmail");
            if (recipientEmail == null) {
                throw new NonRecoverableMessageException("ShipmentStatusChanged sin recipientEmail");
            }
            sendEmailCommand(envelope, shipmentId, status, recipientEmail, text(payload, "recipientName"));
        } else {
            log.debug("eventId={} estado {} no genera correo", envelope.eventId(), status);
        }
        dedupService.markProcessed(envelope.eventId());
    }

    private ShipmentEventEnvelope parse(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            throw new NonRecoverableMessageException("mensaje vacío");
        }
        ShipmentEventEnvelope envelope;
        try {
            envelope = objectMapper.readValue(rawValue, ShipmentEventEnvelope.class);
        } catch (JsonProcessingException e) {
            throw new NonRecoverableMessageException("el valor no es un EventEnvelope JSON válido: "
                    + e.getOriginalMessage());
        }
        if (envelope.eventId() == null || envelope.eventId().isBlank()) {
            throw new NonRecoverableMessageException("el evento no trae eventId");
        }
        if (envelope.type() == null || envelope.type().isBlank()) {
            throw new NonRecoverableMessageException("el evento " + envelope.eventId() + " no trae type");
        }
        if (envelope.payload() == null || !envelope.payload().isObject()) {
            throw new NonRecoverableMessageException("el evento " + envelope.eventId() + " no trae payload");
        }
        return envelope;
    }

    private void sendEmailCommand(ShipmentEventEnvelope event, String shipmentId, String status,
                                  String recipientEmail, String recipientName) {
        String commandId = "email-" + shipmentId + "-" + status;

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("shipmentId", Long.valueOf(shipmentId));
        payload.put("recipientEmail", recipientEmail);
        payload.put("recipientName", recipientName);
        payload.put("event", status);
        payload.put("message", messageFor(status));

        Map<String, Object> command = new LinkedHashMap<>();
        command.put("type", EMAIL_COMMAND);
        command.put("eventId", commandId);
        command.put("timestamp", Instant.now().toString());
        command.put("traceId", event.traceId());
        command.put("correlationId", shipmentId);
        command.put("payload", payload);

        String json;
        try {
            json = objectMapper.writeValueAsString(command);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("No se pudo serializar el comando " + commandId, e);
        }
        rabbitTemplate.convertAndSend(rabbitProperties.exchanges().direct(), rabbitProperties.email().routingKey(), json);
        log.info("Comando {} eventId={} publicado en {} desde el evento Kafka eventId={} traceId={}",
                EMAIL_COMMAND, commandId, rabbitProperties.exchanges().direct(), event.eventId(), event.traceId());
    }

    static String messageFor(String status) {
        return switch (status) {
            case "ACEPTADO" -> "Tu envío ha sido aceptado";
            case "EN_BODEGA" -> "Tu envío llegó a bodega";
            case "EN_RUTA" -> "Tu envío está en ruta";
            case "ENTREGADO" -> "Tu envío ha sido entregado";
            case "CANCELADO" -> "Tu envío fue cancelado";
            default -> "Tu envío cambió a " + status;
        };
    }

    private static String text(JsonNode payload, String field) {
        JsonNode node = payload.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        String value = node.asText();
        return value.isBlank() ? null : value;
    }
}
