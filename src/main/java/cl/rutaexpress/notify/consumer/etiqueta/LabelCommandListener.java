package cl.rutaexpress.notify.consumer.etiqueta;

import cl.rutaexpress.notify.config.RabbitProperties;
import cl.rutaexpress.notify.consumer.common.AbstractCommandListener;
import cl.rutaexpress.notify.consumer.common.NonRecoverableMessageException;
import cl.rutaexpress.notify.handler.LabelPdfGenerator;
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
 * Dominio etiqueta: consume {@code q.cmd.label} (tipo {@code LabelGenCommand}) y genera la etiqueta
 * PDF. Payload: {@code {shipmentId, recipientName, destinationAddress, weightKg, serviceId}}.
 * Con peso inválido va a la DLQ.
 */
@Component
public class LabelCommandListener extends AbstractCommandListener {

    private final LabelPdfGenerator labelGenerator;
    private final RabbitProperties.Route route;

    public LabelCommandListener(ObjectMapper objectMapper,
                                RabbitTemplate rabbitTemplate,
                                DedupService dedupService,
                                LabelPdfGenerator labelGenerator,
                                RabbitProperties properties) {
        super(objectMapper, rabbitTemplate, dedupService, properties);
        this.labelGenerator = labelGenerator;
        this.route = properties.label();
    }

    @RabbitListener(queues = "${rutaexpress.rabbitmq.label.queue}")
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
        String recipientName = payload.path("recipientName").asText("");
        String destinationAddress = payload.path("destinationAddress").asText("");
        String serviceId = payload.path("serviceId").asText("");
        labelGenerator.generate(shipmentId, envelope.eventId(), recipientName, destinationAddress, weightKg, serviceId);
    }
}
