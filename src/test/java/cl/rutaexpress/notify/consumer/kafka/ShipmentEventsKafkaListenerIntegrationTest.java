package cl.rutaexpress.notify.consumer.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import static cl.rutaexpress.notify.service.ShipmentNotificationServiceTest.statusChanged;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * Contra un broker Kafka embebido: un mensaje inválido termina en notify.DLT con su causa, y el
 * consumidor sigue procesando el siguiente evento válido, que genera el comando de email.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.rabbitmq.listener.simple.auto-startup=false"
})
@EmbeddedKafka(partitions = 3, topics = {"shipments.events", "notify.DLT"})
class ShipmentEventsKafkaListenerIntegrationTest {

    @MockitoBean
    RabbitTemplate rabbitTemplate;

    @Autowired
    EmbeddedKafkaBroker broker;

    @Autowired
    KafkaListenerEndpointRegistry registry;

    @Test
    void invalidMessageGoesToDltAndConsumerKeepsWorking() {
        ContainerTestUtils.waitForAssignment(registry.getListenerContainer("notify-shipment-events"), 3);

        KafkaTemplate<String, String> producer = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(broker), new StringSerializer(), new StringSerializer()));
        producer.send("shipments.events", "42", "{mensaje mal formado");
        producer.send("shipments.events", "42", statusChanged("evt-ok", "ENTREGADO", "ana@correo.cl"));
        producer.flush();

        verify(rabbitTemplate, timeout(15_000)).convertAndSend(eq("cmd.direct"), eq("email.send"),
                contains("email-42-ENTREGADO"));

        Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "dlt-reader", false);
        props.put("auto.offset.reset", "earliest");
        try (Consumer<String, String> dltConsumer = new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(), new StringDeserializer()).createConsumer()) {
            broker.consumeFromAnEmbeddedTopic(dltConsumer, "notify.DLT");
            ConsumerRecord<String, String> dead = KafkaTestUtils.getSingleRecord(dltConsumer, "notify.DLT",
                    Duration.ofSeconds(15));
            assertThat(dead.value()).isEqualTo("{mensaje mal formado");
            assertThat(new String(dead.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE).value(),
                    StandardCharsets.UTF_8)).contains("JSON válido");
            assertThat(new String(dead.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC).value(),
                    StandardCharsets.UTF_8)).isEqualTo("shipments.events");
        }
        verify(rabbitTemplate).convertAndSend(anyString(), anyString(), contains("email-42-ENTREGADO"));
    }
}
