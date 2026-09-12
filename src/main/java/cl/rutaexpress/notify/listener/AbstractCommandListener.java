package cl.rutaexpress.notify.listener;

import cl.rutaexpress.notify.idempotency.DedupService;
import cl.rutaexpress.notify.messaging.CommandEnvelope;
import cl.rutaexpress.notify.messaging.RabbitConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.io.IOException;

/**
 * Shared ack/nack/retry flow for the three command listeners. Each subclass wires the specific
 * queue/retry-queue/routing-key names and implements {@link #process(CommandEnvelope)} to
 * delegate to its business handler.
 *
 * <p>Flow (manual ack mode):
 * <ol>
 *   <li>Parse the body as a {@link CommandEnvelope}; a parse failure goes straight to the DLQ
 *       (basicNack, no requeue) since it can never succeed on retry.</li>
 *   <li>Idempotency check: if {@code eventId} was already processed, ack immediately without
 *       reprocessing.</li>
 *   <li>On success: mark processed, then ack.</li>
 *   <li>On handler failure: read {@code x-retry-count} (default 0). If below
 *       {@code retry.max-attempts}, republish the same envelope to the matching delay/retry queue
 *       with the header incremented, then ack the original (we've taken ownership of retrying it
 *       ourselves). If at/above the limit, nack without requeue so RabbitMQ's native
 *       dead-lettering (the queue's {@code x-dead-letter-exchange} argument) routes it to the
 *       correct DLQ.</li>
 * </ol>
 */
public abstract class AbstractCommandListener {

    private static final Logger log = LoggerFactory.getLogger(AbstractCommandListener.class);

    protected final ObjectMapper objectMapper;
    protected final RabbitTemplate rabbitTemplate;
    protected final DedupService dedupService;
    protected final int maxAttempts;

    protected AbstractCommandListener(ObjectMapper objectMapper,
                                       RabbitTemplate rabbitTemplate,
                                       DedupService dedupService,
                                       int maxAttempts) {
        this.objectMapper = objectMapper;
        this.rabbitTemplate = rabbitTemplate;
        this.dedupService = dedupService;
        this.maxAttempts = maxAttempts;
    }

    protected void handle(Message message, Channel channel, String queueName, String retryQueueName) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();

        CommandEnvelope envelope;
        try {
            envelope = objectMapper.readValue(message.getBody(), CommandEnvelope.class);
        } catch (IOException e) {
            log.warn("No se pudo parsear el mensaje recibido en {}; se envía a DLQ sin reintentar. error={}",
                    queueName, e.getMessage());
            channel.basicNack(deliveryTag, false, false);
            return;
        }

        if (dedupService.isDuplicate(envelope.eventId())) {
            log.info("eventId={} ya fue procesado, se ignora (duplicado) en {}", envelope.eventId(), queueName);
            channel.basicAck(deliveryTag, false);
            return;
        }

        try {
            process(envelope);
            dedupService.markProcessed(envelope.eventId());
            channel.basicAck(deliveryTag, false);
        } catch (Exception ex) {
            int retryCount = readRetryCount(message);
            if (retryCount < maxAttempts) {
                log.warn("Fallo procesando eventId={} en {} (intento {} de {}): {}. Reencolando en {}.",
                        envelope.eventId(), queueName, retryCount + 1, maxAttempts, ex.getMessage(), retryQueueName);
                republishForRetry(message, retryQueueName, retryCount + 1);
                channel.basicAck(deliveryTag, false);
            } else {
                log.warn("Fallo procesando eventId={} en {} tras {} intentos: {}. Enviando a la DLQ correspondiente.",
                        envelope.eventId(), queueName, retryCount, ex.getMessage());
                channel.basicNack(deliveryTag, false, false);
            }
        }
    }

    /**
     * Business-specific handling for the parsed envelope. Implementations should read payload
     * fields defensively (e.g. {@code payload.path("field").asText()}) and throw on failure so
     * the retry/DLQ flow above can take over.
     */
    protected abstract void process(CommandEnvelope envelope) throws Exception;

    private int readRetryCount(Message message) {
        Object header = message.getMessageProperties().getHeaders().get(RabbitConstants.HEADER_RETRY_COUNT);
        if (header instanceof Number number) {
            return number.intValue();
        }
        return 0;
    }

    private void republishForRetry(Message message, String retryQueueName, int newRetryCount) {
        MessageProperties properties = message.getMessageProperties();
        properties.setHeader(RabbitConstants.HEADER_RETRY_COUNT, newRetryCount);
        Message retryMessage = new Message(message.getBody(), properties);
        rabbitTemplate.convertAndSend("", retryQueueName, retryMessage);
    }
}
