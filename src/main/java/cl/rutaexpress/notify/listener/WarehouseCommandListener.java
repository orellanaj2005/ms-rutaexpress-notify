package cl.rutaexpress.notify.listener;

import cl.rutaexpress.notify.handler.PickingTicketGenerator;
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
 * Consumes {@code q.cmd.warehouse}: type {@code "WarehouseTicketCommand"}, payload
 * {@code {shipmentId, originAddress, destinationAddress, weightKg}}.
 */
@Component
public class WarehouseCommandListener extends AbstractCommandListener {

    private final PickingTicketGenerator ticketGenerator;

    public WarehouseCommandListener(ObjectMapper objectMapper,
                                     RabbitTemplate rabbitTemplate,
                                     DedupService dedupService,
                                     PickingTicketGenerator ticketGenerator,
                                     @Value("${retry.max-attempts:3}") int maxAttempts) {
        super(objectMapper, rabbitTemplate, dedupService, maxAttempts);
        this.ticketGenerator = ticketGenerator;
    }

    @RabbitListener(queues = RabbitConstants.QUEUE_WAREHOUSE)
    public void onMessage(Message message, Channel channel) throws IOException {
        handle(message, channel, RabbitConstants.QUEUE_WAREHOUSE, RabbitConstants.QUEUE_WAREHOUSE_RETRY);
    }

    @Override
    protected void process(CommandEnvelope envelope) {
        JsonNode payload = envelope.payload();
        String shipmentId = payload.path("shipmentId").asText("");
        String originAddress = payload.path("originAddress").asText("");
        String destinationAddress = payload.path("destinationAddress").asText("");
        double weightKg = payload.path("weightKg").asDouble(0d);
        ticketGenerator.generate(shipmentId, envelope.eventId(), originAddress, destinationAddress, weightKg);
    }
}
