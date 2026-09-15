package cl.rutaexpress.notify.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import static cl.rutaexpress.notify.messaging.RabbitConstants.EXCHANGE_DIRECT;
import static cl.rutaexpress.notify.messaging.RabbitConstants.EXCHANGE_DLX;
import static cl.rutaexpress.notify.messaging.RabbitConstants.EXCHANGE_TOPIC;
import static cl.rutaexpress.notify.messaging.RabbitConstants.PATTERN_EMAIL;
import static cl.rutaexpress.notify.messaging.RabbitConstants.PATTERN_LABEL;
import static cl.rutaexpress.notify.messaging.RabbitConstants.PATTERN_WAREHOUSE;
import static cl.rutaexpress.notify.messaging.RabbitConstants.QUEUE_EMAIL;
import static cl.rutaexpress.notify.messaging.RabbitConstants.QUEUE_EMAIL_DLQ;
import static cl.rutaexpress.notify.messaging.RabbitConstants.QUEUE_EMAIL_RETRY;
import static cl.rutaexpress.notify.messaging.RabbitConstants.QUEUE_LABEL;
import static cl.rutaexpress.notify.messaging.RabbitConstants.QUEUE_LABEL_DLQ;
import static cl.rutaexpress.notify.messaging.RabbitConstants.QUEUE_LABEL_RETRY;
import static cl.rutaexpress.notify.messaging.RabbitConstants.QUEUE_WAREHOUSE;
import static cl.rutaexpress.notify.messaging.RabbitConstants.QUEUE_WAREHOUSE_DLQ;
import static cl.rutaexpress.notify.messaging.RabbitConstants.QUEUE_WAREHOUSE_RETRY;
import static cl.rutaexpress.notify.messaging.RabbitConstants.ROUTING_EMAIL;
import static cl.rutaexpress.notify.messaging.RabbitConstants.ROUTING_LABEL;
import static cl.rutaexpress.notify.messaging.RabbitConstants.ROUTING_WAREHOUSE;

/**
 * Declares the full RabbitMQ topology owned by notify, per section 7 ("Topología RabbitMQ")
 * of docs/guia_javier_rutaexpress.md, followed here to the letter:
 *
 * <ul>
 *   <li>Exchanges: {@code cmd.direct} (direct), {@code cmd.topic} (topic), {@code cmd.dead.dlx} (direct, the DLX).</li>
 *   <li>Main queues q.cmd.email / q.cmd.warehouse / q.cmd.label, each dead-lettering to
 *       {@code cmd.dead.dlx} with NO fixed dead-letter routing key, so RabbitMQ preserves the
 *       original routing key when dead-lettering — which is what lets the DLQ bindings below work.</li>
 *   <li>Each main queue is bound to both {@code cmd.direct} (exact routing key) and
 *       {@code cmd.topic} (pattern), per the guide's binding table.</li>
 *   <li>One DLQ per main queue, bound to {@code cmd.dead.dlx} with the SAME routing key as the
 *       main queue's direct binding.</li>
 *   <li>One delay/"parking lot" queue per main queue (classic TTL + dead-letter trick used for
 *       in-app retry backoff, since plain RabbitMQ has no native delayed redelivery): messages
 *       sit here for {@code retry.delay-ms} then dead-letter back onto {@code cmd.direct} with
 *       the original routing key, landing back on the matching main queue. No consumer is ever
 *       attached to these.</li>
 * </ul>
 */
@Configuration
public class RabbitTopologyConfig {

    @Value("${retry.delay-ms:5000}")
    private long delayMs;

    // Jackson's autoconfiguration needs spring-web on the classpath (Jackson2ObjectMapperBuilder),
    // which this service doesn't have since it has no HTTP starter — declared explicitly instead.
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Bean
    public Declarables rabbitTopology() {
        // Exchanges
        DirectExchange cmdDirect = new DirectExchange(EXCHANGE_DIRECT, true, false);
        TopicExchange cmdTopic = new TopicExchange(EXCHANGE_TOPIC, true, false);
        DirectExchange cmdDeadDlx = new DirectExchange(EXCHANGE_DLX, true, false);

        // Main queues: dead-letter to cmd.dead.dlx, no fixed dead-letter routing key
        Queue emailQueue = QueueBuilder.durable(QUEUE_EMAIL)
                .withArgument("x-dead-letter-exchange", EXCHANGE_DLX)
                .build();
        Queue warehouseQueue = QueueBuilder.durable(QUEUE_WAREHOUSE)
                .withArgument("x-dead-letter-exchange", EXCHANGE_DLX)
                .build();
        Queue labelQueue = QueueBuilder.durable(QUEUE_LABEL)
                .withArgument("x-dead-letter-exchange", EXCHANGE_DLX)
                .build();

        // Main queue bindings: exact key on cmd.direct, pattern on cmd.topic
        Binding emailDirectBinding = BindingBuilder.bind(emailQueue).to(cmdDirect).with(ROUTING_EMAIL);
        Binding emailTopicBinding = BindingBuilder.bind(emailQueue).to(cmdTopic).with(PATTERN_EMAIL);
        Binding warehouseDirectBinding = BindingBuilder.bind(warehouseQueue).to(cmdDirect).with(ROUTING_WAREHOUSE);
        Binding warehouseTopicBinding = BindingBuilder.bind(warehouseQueue).to(cmdTopic).with(PATTERN_WAREHOUSE);
        Binding labelDirectBinding = BindingBuilder.bind(labelQueue).to(cmdDirect).with(ROUTING_LABEL);
        Binding labelTopicBinding = BindingBuilder.bind(labelQueue).to(cmdTopic).with(PATTERN_LABEL);

        // DLQ queues (plain durable queues, no special args)
        Queue emailDlq = QueueBuilder.durable(QUEUE_EMAIL_DLQ).build();
        Queue warehouseDlq = QueueBuilder.durable(QUEUE_WAREHOUSE_DLQ).build();
        Queue labelDlq = QueueBuilder.durable(QUEUE_LABEL_DLQ).build();

        // DLQ bindings: same routing key as the corresponding main queue's direct binding
        Binding emailDlqBinding = BindingBuilder.bind(emailDlq).to(cmdDeadDlx).with(ROUTING_EMAIL);
        Binding warehouseDlqBinding = BindingBuilder.bind(warehouseDlq).to(cmdDeadDlx).with(ROUTING_WAREHOUSE);
        Binding labelDlqBinding = BindingBuilder.bind(labelDlq).to(cmdDeadDlx).with(ROUTING_LABEL);

        // Delay/parking-lot queues for in-app retry backoff. TTL is fixed at declaration time
        // from the retry.delay-ms property; changing it after the queue already exists in the
        // broker requires deleting/recreating the queue (RabbitMQ does not allow changing queue
        // arguments in place).
        Queue emailRetryQueue = QueueBuilder.durable(QUEUE_EMAIL_RETRY)
                .withArgument("x-message-ttl", delayMs)
                .withArgument("x-dead-letter-exchange", EXCHANGE_DIRECT)
                .withArgument("x-dead-letter-routing-key", ROUTING_EMAIL)
                .build();
        Queue warehouseRetryQueue = QueueBuilder.durable(QUEUE_WAREHOUSE_RETRY)
                .withArgument("x-message-ttl", delayMs)
                .withArgument("x-dead-letter-exchange", EXCHANGE_DIRECT)
                .withArgument("x-dead-letter-routing-key", ROUTING_WAREHOUSE)
                .build();
        Queue labelRetryQueue = QueueBuilder.durable(QUEUE_LABEL_RETRY)
                .withArgument("x-message-ttl", delayMs)
                .withArgument("x-dead-letter-exchange", EXCHANGE_DIRECT)
                .withArgument("x-dead-letter-routing-key", ROUTING_LABEL)
                .build();

        return new Declarables(
                cmdDirect, cmdTopic, cmdDeadDlx,
                emailQueue, warehouseQueue, labelQueue,
                emailDirectBinding, emailTopicBinding,
                warehouseDirectBinding, warehouseTopicBinding,
                labelDirectBinding, labelTopicBinding,
                emailDlq, warehouseDlq, labelDlq,
                emailDlqBinding, warehouseDlqBinding, labelDlqBinding,
                emailRetryQueue, warehouseRetryQueue, labelRetryQueue);
    }
}
