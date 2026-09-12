package cl.rutaexpress.notify.listener;

import cl.rutaexpress.notify.handler.LabelPdfGenerator;
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
 * Consumes {@code q.cmd.label}: type {@code "LabelGenCommand"}, payload
 * {@code {shipmentId, recipientName, destinationAddress, weightKg, serviceId}}.
 */
@Component
public class LabelCommandListener extends AbstractCommandListener {

    private final LabelPdfGenerator labelGenerator;

    public LabelCommandListener(ObjectMapper objectMapper,
                                 RabbitTemplate rabbitTemplate,
                                 DedupService dedupService,
                                 LabelPdfGenerator labelGenerator,
                                 @Value("${retry.max-attempts:3}") int maxAttempts) {
        super(objectMapper, rabbitTemplate, dedupService, maxAttempts);
        this.labelGenerator = labelGenerator;
    }

    @RabbitListener(queues = RabbitConstants.QUEUE_LABEL)
    public void onMessage(Message message, Channel channel) throws IOException {
        handle(message, channel, RabbitConstants.QUEUE_LABEL, RabbitConstants.QUEUE_LABEL_RETRY);
    }

    @Override
    protected void process(CommandEnvelope envelope) {
        JsonNode payload = envelope.payload();
        String shipmentId = payload.path("shipmentId").asText("");
        String recipientName = payload.path("recipientName").asText("");
        String destinationAddress = payload.path("destinationAddress").asText("");
        double weightKg = payload.path("weightKg").asDouble(0d);
        String serviceId = payload.path("serviceId").asText("");
        labelGenerator.generate(shipmentId, envelope.eventId(), recipientName, destinationAddress, weightKg, serviceId);
    }
}
