package cl.rutaexpress.notify.consumer.bodega;

import cl.rutaexpress.notify.config.RabbitProperties;
import cl.rutaexpress.notify.consumer.common.AbstractCommandListener;
import cl.rutaexpress.notify.consumer.common.NonRecoverableMessageException;
import cl.rutaexpress.notify.handler.PickingTicketGenerator;
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
 * Dominio bodega: consume {@code q.cmd.warehouse} (tipo {@code WarehouseTicketCommand}) y genera el
 * ticket de picking. Payload: {@code {shipmentId, originAddress, destinationAddress, weightKg}}.
 * Con peso inválido va a la DLQ.
 */
@Component
public class WarehouseCommandListener extends AbstractCommandListener {

    private final PickingTicketGenerator ticketGenerator;
    private final RabbitProperties.Route route;

    public WarehouseCommandListener(ObjectMapper objectMapper,
                                    RabbitTemplate rabbitTemplate,
                                    DedupService dedupService,
                                    PickingTicketGenerator ticketGenerator,
                                    RabbitProperties properties) {
        super(objectMapper, rabbitTemplate, dedupService, properties);
        this.ticketGenerator = ticketGenerator;
        this.route = properties.warehouse();
    }

    @RabbitListener(queues = "${rutaexpress.rabbitmq.warehouse.queue}")
    public void onMessage(Message message, Channel channel) throws IOException {
        handle(message, channel, route);
    }

    @Override
    protected void process(CommandEnvelope envelope) {
        JsonNode payload = envelope.payload();
        double weightKg = payload.path("weightKg").asDouble(0d);
        if (weightKg <= 0) {
            throw new NonRecoverableMessageException("peso inválido: " + payload.path("weightKg"));
        }
        String shipmentId = payload.path("shipmentId").asText("");
        String originAddress = payload.path("originAddress").asText("");
        String destinationAddress = payload.path("destinationAddress").asText("");
        ticketGenerator.generate(shipmentId, envelope.eventId(), originAddress, destinationAddress, weightKg);
    }
}
