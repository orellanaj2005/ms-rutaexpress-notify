package cl.rutaexpress.notify.listener;

import cl.rutaexpress.notify.handler.PickingTicketGenerator;
import cl.rutaexpress.notify.idempotency.DedupService;
import cl.rutaexpress.notify.messaging.RabbitConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.time.Instant;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class WarehouseCommandListenerTest {

    private static final int MAX_ATTEMPTS = 3;

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private RabbitTemplate rabbitTemplate;
    private DedupService dedupService;
    private PickingTicketGenerator ticketGenerator;
    private Channel channel;
    private WarehouseCommandListener listener;

    @BeforeEach
    void setUp() {
        rabbitTemplate = mock(RabbitTemplate.class);
        dedupService = mock(DedupService.class);
        ticketGenerator = mock(PickingTicketGenerator.class);
        channel = mock(Channel.class);
        listener = new WarehouseCommandListener(objectMapper, rabbitTemplate, dedupService, ticketGenerator, MAX_ATTEMPTS);
    }

    @Test
    void successPathAcksAndNeverNacks() throws Exception {
        Message message = buildMessage("evt-1", null);
        when(dedupService.isDuplicate("evt-1")).thenReturn(false);

        listener.onMessage(message, channel);

        verify(ticketGenerator).generate(eq("SHP-1"), eq("evt-1"), eq("Origin 1"), eq("Dest 1"), eq(12.5));
        verify(dedupService).markProcessed("evt-1");
        verify(channel).basicAck(1L, false);
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    void duplicateEventShortCircuitsAndStillAcks() throws Exception {
        Message message = buildMessage("evt-dup", null);
        when(dedupService.isDuplicate("evt-dup")).thenReturn(true);

        listener.onMessage(message, channel);

        verifyNoInteractions(ticketGenerator);
        verify(channel).basicAck(1L, false);
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    void failureBelowMaxAttemptsRepublishesToRetryQueueAndAcksOriginal() throws Exception {
        Message message = buildMessage("evt-2", 1);
        when(dedupService.isDuplicate("evt-2")).thenReturn(false);
        doThrow(new RuntimeException("disk full"))
                .when(ticketGenerator).generate(any(), any(), any(), any(), anyDouble());

        listener.onMessage(message, channel);

        verify(rabbitTemplate).convertAndSend(eq(""), eq(RabbitConstants.QUEUE_WAREHOUSE_RETRY), any(Message.class));
        verify(channel).basicAck(1L, false);
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
        verify(dedupService, never()).markProcessed(anyString());
    }

    @Test
    void failureAtMaxAttemptsNacksWithoutRequeueAndDoesNotRepublish() throws Exception {
        Message message = buildMessage("evt-3", MAX_ATTEMPTS);
        when(dedupService.isDuplicate("evt-3")).thenReturn(false);
        doThrow(new RuntimeException("disk full"))
                .when(ticketGenerator).generate(any(), any(), any(), any(), anyDouble());

        listener.onMessage(message, channel);

        verify(channel).basicNack(1L, false, false);
        verify(rabbitTemplate, never()).convertAndSend(anyString(), anyString(), any(Message.class));
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
    }

    private Message buildMessage(String eventId, Integer retryCount) throws Exception {
        Map<String, Object> payload = Map.of(
                "shipmentId", "SHP-1",
                "originAddress", "Origin 1",
                "destinationAddress", "Dest 1",
                "weightKg", 12.5);
        Map<String, Object> envelope = Map.of(
                "type", "WarehouseTicketCommand",
                "eventId", eventId,
                "timestamp", Instant.now().toString(),
                "traceId", "trace-1",
                "correlationId", "corr-1",
                "payload", payload);
        byte[] body = objectMapper.writeValueAsBytes(envelope);

        MessageProperties properties = new MessageProperties();
        properties.setDeliveryTag(1L);
        if (retryCount != null) {
            properties.setHeader(RabbitConstants.HEADER_RETRY_COUNT, retryCount);
        }
        return new Message(body, properties);
    }
}
