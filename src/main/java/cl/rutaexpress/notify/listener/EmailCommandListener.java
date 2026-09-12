package cl.rutaexpress.notify.listener;

import cl.rutaexpress.notify.handler.EmailSender;
import cl.rutaexpress.notify.idempotency.DedupService;
import cl.rutaexpress.notify.messaging.CommandEnvelope;
import cl.rutaexpress.notify.messaging.RabbitConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Consumes {@code q.cmd.email}: type {@code "EmailSendCommand"}, payload
 * {@code {shipmentId, recipientEmail, recipientName, event, message}}.
 */
@Component
public class EmailCommandListener extends AbstractCommandListener {

    private final EmailSender emailSender;

    public EmailCommandListener(ObjectMapper objectMapper,
                                 RabbitTemplate rabbitTemplate,
                                 DedupService dedupService,
                                 EmailSender emailSender,
                                 @Value("${retry.max-attempts:3}") int maxAttempts) {
        super(objectMapper, rabbitTemplate, dedupService, maxAttempts);
        this.emailSender = emailSender;
    }

    @RabbitListener(queues = RabbitConstants.QUEUE_EMAIL)
    public void onMessage(Message message, Channel channel) throws IOException {
        handle(message, channel, RabbitConstants.QUEUE_EMAIL, RabbitConstants.QUEUE_EMAIL_RETRY);
    }

    @Override
    protected void process(CommandEnvelope envelope) {
        JsonNode payload = envelope.payload();
        String shipmentId = payload.path("shipmentId").asText("");
        String recipientEmail = payload.path("recipientEmail").asText("");
        String recipientName = payload.path("recipientName").asText("");
        String event = payload.path("event").asText("");
        String message = payload.path("message").asText("");
        emailSender.send(shipmentId, recipientEmail, recipientName, event, message);
    }
}
