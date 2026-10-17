package cl.rutaexpress.notify.consumer.common;

import cl.rutaexpress.notify.config.RabbitProperties;
import cl.rutaexpress.notify.idempotency.DedupService;
import cl.rutaexpress.notify.messaging.CommandEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.io.IOException;

/**
 * Flujo común de ACK, reintento y DLQ para los tres consumidores. Cada subclase indica su ruta
 * (cola, cola de reintento y DLQ) e implementa {@link #process(CommandEnvelope)}.
 *
 * <p>Flujo, con ACK manual:
 * <ol>
 *   <li>El cuerpo se lee como {@link CommandEnvelope}. Si no es JSON válido, o no trae eventId o
 *       payload, va directo a la DLQ (NACK sin reencolar): reintentar nunca lo arreglaría.</li>
 *   <li>Idempotencia: si el eventId ya se procesó, se confirma (ACK) sin repetir el trabajo.</li>
 *   <li>Éxito: se marca como procesado y se confirma (ACK).</li>
 *   <li>{@link NonRecoverableMessageException}: el mensaje es inválido. NACK sin reencolar, directo
 *       a la DLQ y sin reintentos.</li>
 *   <li>Cualquier otra excepción es recuperable: si quedan intentos, se publica una copia en la cola
 *       de reintento con {@code x-retry-count} incrementado y se confirma el original. Si se agotaron,
 *       NACK sin reencolar y RabbitMQ lo enruta a la DLQ por {@code x-dead-letter-exchange}.</li>
 * </ol>
 *
 * <p>Todo envío a DLQ deja un log ERROR con eventId, cola y motivo.
 */
public abstract class AbstractCommandListener {

    /** Cabecera con el número de reintentos ya hechos. La lee también audit para contar intentos. */
    public static final String HEADER_RETRY_COUNT = "x-retry-count";

    private static final Logger log = LoggerFactory.getLogger(AbstractCommandListener.class);

    protected final ObjectMapper objectMapper;
    protected final RabbitTemplate rabbitTemplate;
    protected final DedupService dedupService;
    protected final int maxAttempts;

    protected AbstractCommandListener(ObjectMapper objectMapper,
                                      RabbitTemplate rabbitTemplate,
                                      DedupService dedupService,
                                      RabbitProperties properties) {
        this.objectMapper = objectMapper;
        this.rabbitTemplate = rabbitTemplate;
        this.dedupService = dedupService;
        this.maxAttempts = properties.retry().maxAttempts();
    }

    protected void handle(Message message, Channel channel, RabbitProperties.Route route) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();

        CommandEnvelope envelope;
        try {
            envelope = objectMapper.readValue(message.getBody(), CommandEnvelope.class);
        } catch (IOException e) {
            sendToDlq(channel, deliveryTag, null, route, "el cuerpo no es JSON válido");
            return;
        }

        if (envelope.eventId() == null || envelope.eventId().isBlank()) {
            sendToDlq(channel, deliveryTag, null, route, "el mensaje no trae eventId");
            return;
        }
        if (envelope.payload() == null || !envelope.payload().isObject()) {
            sendToDlq(channel, deliveryTag, envelope.eventId(), route, "el mensaje no trae payload");
            return;
        }

        if (dedupService.isDuplicate(envelope.eventId())) {
            log.info("eventId={} ya fue procesado, se ignora (duplicado) en {}", envelope.eventId(), route.queue());
            channel.basicAck(deliveryTag, false);
            return;
        }

        try {
            process(envelope);
            dedupService.markProcessed(envelope.eventId());
            channel.basicAck(deliveryTag, false);
        } catch (NonRecoverableMessageException ex) {
            sendToDlq(channel, deliveryTag, envelope.eventId(), route, "error no recuperable: " + ex.getMessage());
        } catch (Exception ex) {
            int retryCount = readRetryCount(message);
            if (retryCount < maxAttempts) {
                log.warn("Fallo procesando eventId={} en {} (intento {} de {}): {}. Reintento en {}.",
                        envelope.eventId(), route.queue(), retryCount + 1, maxAttempts + 1, ex.getMessage(),
                        route.retryQueue());
                republishForRetry(message, route.retryQueue(), retryCount + 1);
                channel.basicAck(deliveryTag, false);
            } else {
                sendToDlq(channel, deliveryTag, envelope.eventId(), route,
                        "reintentos agotados (" + (retryCount + 1) + " intentos): " + ex.getMessage());
            }
        }
    }

    /**
     * Lógica de negocio del caso de uso. Debe lanzar {@link NonRecoverableMessageException} si el
     * payload es inválido, y cualquier otra excepción si la falla puede resolverse reintentando.
     */
    protected abstract void process(CommandEnvelope envelope) throws Exception;

    /** NACK sin reencolar: RabbitMQ lo publica en cmd.dead.dlx, que lo entrega a la DLQ de la ruta. */
    private void sendToDlq(Channel channel, long deliveryTag, String eventId, RabbitProperties.Route route,
                           String reason) throws IOException {
        log.error("Mensaje enviado a DLQ: eventId={} cola={} dlq={} motivo={}",
                eventId == null ? "desconocido" : eventId, route.queue(), route.dlq(), reason);
        channel.basicNack(deliveryTag, false, false);
    }

    private int readRetryCount(Message message) {
        Object header = message.getMessageProperties().getHeaders().get(HEADER_RETRY_COUNT);
        if (header instanceof Number number) {
            return number.intValue();
        }
        return 0;
    }

    private void republishForRetry(Message message, String retryQueueName, int newRetryCount) {
        MessageProperties properties = message.getMessageProperties();
        properties.setHeader(HEADER_RETRY_COUNT, newRetryCount);
        Message retryMessage = new Message(message.getBody(), properties);
        rabbitTemplate.convertAndSend("", retryQueueName, retryMessage);
    }
}
