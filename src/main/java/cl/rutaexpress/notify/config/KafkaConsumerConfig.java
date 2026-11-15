package cl.rutaexpress.notify.config;

import cl.rutaexpress.notify.consumer.common.NonRecoverableMessageException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Manejo de errores del consumidor de {@code shipments.events}. El consumer factory y el container
 * factory los arma Spring Boot desde {@code spring.kafka} en application.yaml (grupo
 * ms-rutaexpress-notify, {@link ErrorHandlingDeserializer}, concurrencia 3); Boot toma el
 * {@link CommonErrorHandler} de esta clase y lo aplica al container.
 *
 * <p>Política:
 * <ul>
 *   <li>Fallas transitorias (por ejemplo RabbitMQ no disponible): 3 reintentos con backoff
 *       exponencial de 1 s, 2 s y 4 s; si siguen fallando, el mensaje va a {@code notify.DLT}.</li>
 *   <li>Mensajes inválidos ({@link NonRecoverableMessageException}) y errores de deserialización:
 *       directo a {@code notify.DLT}, sin reintentos.</li>
 * </ul>
 * En los dos casos se confirma el offset y el consumidor sigue con el siguiente mensaje.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean
    public KafkaTemplate<Object, Object> deadLetterKafkaTemplate(KafkaProperties kafkaProperties) {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties(),
                new RawOrStringSerializer(), new RawOrStringSerializer()));
    }

    @Bean
    public CommonErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> deadLetterKafkaTemplate,
                                                KafkaNotifyProperties properties) {
        // Partición -1: Kafka elige la partición de notify.DLT, que no tiene por qué coincidir con el origen.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(deadLetterKafkaTemplate,
                (record, ex) -> new TopicPartition(properties.dltTopic(), -1));

        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxAttempts(3);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler((record, ex) -> {
            log.error("Mensaje enviado a {}: topic={} partition={} offset={} key={} motivo={}",
                    properties.dltTopic(), record.topic(), record.partition(), record.offset(), record.key(),
                    rootMessage(ex));
            recoverer.accept(record, ex);
        }, backOff);
        errorHandler.addNotRetryableExceptions(NonRecoverableMessageException.class);
        errorHandler.setRetryListeners((ConsumerRecord<?, ?> record, Exception ex, int attempt) ->
                log.warn("Reintento {} del mensaje topic={} partition={} offset={}: {}",
                        attempt, record.topic(), record.partition(), record.offset(), rootMessage(ex)));
        return errorHandler;
    }

    private static String rootMessage(Throwable ex) {
        Throwable cause = ex;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause.getMessage();
    }

    /**
     * Escribe al DLT el valor original: un {@code byte[]} cuando falló la deserialización (Spring
     * conserva los bytes crudos) o el texto recibido en cualquier otro caso.
     */
    static final class RawOrStringSerializer implements Serializer<Object> {

        private final StringSerializer strings = new StringSerializer();

        @Override
        public byte[] serialize(String topic, Object data) {
            if (data == null) {
                return null;
            }
            if (data instanceof byte[] bytes) {
                return bytes;
            }
            return strings.serialize(topic, data.toString());
        }
    }
}
