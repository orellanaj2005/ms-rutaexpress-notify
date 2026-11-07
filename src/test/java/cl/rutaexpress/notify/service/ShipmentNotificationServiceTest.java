package cl.rutaexpress.notify.service;

import cl.rutaexpress.notify.config.KafkaNotifyProperties;
import cl.rutaexpress.notify.consumer.TestRabbitProperties;
import cl.rutaexpress.notify.consumer.common.NonRecoverableMessageException;
import cl.rutaexpress.notify.idempotency.DedupService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class ShipmentNotificationServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private RabbitTemplate rabbitTemplate;
    private DedupService dedupService;
    private ShipmentNotificationService service;

    @BeforeEach
    void setUp() {
        rabbitTemplate = mock(RabbitTemplate.class);
        dedupService = new DedupService();
        service = new ShipmentNotificationService(objectMapper, rabbitTemplate, dedupService,
                new KafkaNotifyProperties("shipments.events", "notify.DLT", Set.of("EN_RUTA", "ENTREGADO", "CANCELADO")),
                TestRabbitProperties.create());
    }

    @Test
    void statusInEmailStatesPublishesExistingEmailCommand() throws Exception {
        service.handle(statusChanged("evt-1", "EN_RUTA", "ana@correo.cl"));

        ArgumentCaptor<Object> body = ArgumentCaptor.forClass(Object.class);
        verify(rabbitTemplate).convertAndSend(eq("cmd.direct"), eq("email.send"), body.capture());
        JsonNode command = objectMapper.readTree((String) body.getValue());
        assertThat(command.get("type").asText()).isEqualTo("EmailSendCommand");
        assertThat(command.get("eventId").asText()).isEqualTo("email-42-EN_RUTA");
        assertThat(command.get("traceId").asText()).isEqualTo("trace-1");
        assertThat(command.at("/payload/recipientEmail").asText()).isEqualTo("ana@correo.cl");
        assertThat(command.at("/payload/shipmentId").asLong()).isEqualTo(42L);
        assertThat(command.at("/payload/event").asText()).isEqualTo("EN_RUTA");
        assertThat(dedupService.isDuplicate("evt-1")).isTrue();
    }

    @Test
    void statusOutsideEmailStatesDoesNotPublish() {
        service.handle(statusChanged("evt-2", "EN_BODEGA", "ana@correo.cl"));

        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    void otherEventTypesAreIgnored() {
        service.handle("""
                {"eventId":"evt-3","type":"ShipmentCreated","version":1,"payload":{"shipmentId":42,"status":"CREADO"}}
                """);

        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    void redeliveredEventIsProcessedOnlyOnce() {
        String message = statusChanged("evt-4", "ENTREGADO", "ana@correo.cl");
        service.handle(message);
        service.handle(message);

        verify(rabbitTemplate).convertAndSend(eq("cmd.direct"), eq("email.send"), anyString());
    }

    @Test
    void invalidMessagesAreNotRetryable() {
        assertThatThrownBy(() -> service.handle("esto no es json")).isInstanceOf(NonRecoverableMessageException.class);
        assertThatThrownBy(() -> service.handle("{\"type\":\"ShipmentStatusChanged\",\"payload\":{}}"))
                .isInstanceOf(NonRecoverableMessageException.class);
        assertThatThrownBy(() -> service.handle(statusChanged("evt-5", "EN_RUTA", null)))
                .isInstanceOf(NonRecoverableMessageException.class);
        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    void rabbitFailurePropagatesForRetryAndIsNotMarkedProcessed() {
        doThrow(new AmqpConnectException(new RuntimeException("rabbit caído")))
                .when(rabbitTemplate).convertAndSend(anyString(), anyString(), (Object) anyString());

        assertThatThrownBy(() -> service.handle(statusChanged("evt-6", "EN_RUTA", "ana@correo.cl")))
                .isInstanceOf(AmqpConnectException.class);
        assertThat(dedupService.isDuplicate("evt-6")).isFalse();
    }

    public static String statusChanged(String eventId, String status, String recipientEmail) {
        String email = recipientEmail == null ? "null" : "\"" + recipientEmail + "\"";
        return """
                {"eventId":"%s","type":"ShipmentStatusChanged","version":1,"aggregateId":"42",
                 "timestamp":"2026-11-05T09:15:40.002Z","traceId":"trace-1","correlationId":"42",
                 "producer":"ms-rutaexpress-shipments","campoNuevo":"se ignora",
                 "payload":{"shipmentId":42,"previousStatus":"EN_BODEGA","status":"%s","serviceType":"3",
                            "changedBy":"operador@rutaexpress.cl","changedAt":"2026-11-05T09:15:40.002Z",
                            "recipientEmail":%s,"recipientName":"Ana"}}
                """.formatted(eventId, status, email);
    }
}
