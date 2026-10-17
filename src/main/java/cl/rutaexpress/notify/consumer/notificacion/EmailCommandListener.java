package cl.rutaexpress.notify.consumer.notificacion;

import cl.rutaexpress.notify.config.RabbitProperties;
import cl.rutaexpress.notify.consumer.common.AbstractCommandListener;
import cl.rutaexpress.notify.consumer.common.NonRecoverableMessageException;
import cl.rutaexpress.notify.handler.EmailSender;
import cl.rutaexpress.notify.idempotency.DedupService;
import cl.rutaexpress.notify.messaging.CommandEnvelope;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Dominio notificación: consume {@code q.cmd.email} (tipo {@code EmailSendCommand}). Payload:
 * {@code {shipmentId, recipientEmail, recipientName, event, message}}. Sin destinatario va a la DLQ.
 */
@Component
public class EmailCommandListener extends AbstractCommandListener {

    private final EmailSender emailSender;
    private final RabbitProperties.Route route;

    public EmailCommandListener(ObjectMapper objectMapper,
                                RabbitTemplate rabbitTemplate,
                                DedupService dedupService,
                                EmailSender emailSender,
                                RabbitProperties properties) {
        super(objectMapper, rabbitTemplate, dedupService, properties);
        this.emailSender = emailSender;
        this.route = properties.email();
    }

    @RabbitListener(queues = "${rutaexpress.rabbitmq.email.queue}")
    public void onMessage(Message message, Channel channel) throws IOException {
        handle(message, channel, route);
    }

    @Override
    protected void process(CommandEnvelope envelope) {
        JsonNode payload = envelope.payload();
        String recipientEmail = payload.path("recipientEmail").asText("");
        if (recipientEmail.isBlank()) {
            throw new NonRecoverableMessageException("payload sin destinatario");
        }
        String shipmentId = payload.path("shipmentId").asText("");
        String recipientName = payload.path("recipientName").asText("");
        String event = payload.path("event").asText("");
        String message = payload.path("message").asText("");
        emailSender.send(shipmentId, recipientEmail, recipientName, event, message);
    }
}
