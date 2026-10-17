package cl.rutaexpress.notify.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Caso de uso: ticket de picking para la bodega cuando el envío llega a EN_BODEGA.
 *
 * <p>Ruta del mensaje:
 * <pre>
 * shipments --(warehouse.ticket)--> cmd.direct --> q.cmd.warehouse --> notify (WarehouseCommandListener)
 *                   (warehouse.#) --> cmd.topic  --^
 *
 * falla recuperable:    notify --> q.cmd.warehouse.retry --(TTL)--> cmd.direct --> q.cmd.warehouse
 * falla no recuperable
 * o reintentos agotados: q.cmd.warehouse --(NACK)--> cmd.dead.dlx --(warehouse.ticket)--> q.cmd.warehouse.dlq
 *                                                                 \--> q.audit.deadletter (audit)
 * </pre>
 */
@Configuration
public class WarehouseMessagingConfig {

    /** Cola principal. Sin routing key de dead-letter fija, para conservar warehouse.ticket al morir. */
    @Bean
    public Queue warehouseQueue(RabbitProperties properties) {
        return QueueBuilder.durable(properties.warehouse().queue())
                .withArgument("x-dead-letter-exchange", properties.exchanges().deadLetter())
                .build();
    }

    @Bean
    public Binding warehouseDirectBinding(@Qualifier("warehouseQueue") Queue warehouseQueue,
                                          @Qualifier("cmdDirectExchange") DirectExchange cmdDirect,
                                          RabbitProperties properties) {
        return BindingBuilder.bind(warehouseQueue).to(cmdDirect).with(properties.warehouse().routingKey());
    }

    @Bean
    public Binding warehouseTopicBinding(@Qualifier("warehouseQueue") Queue warehouseQueue,
                                         TopicExchange cmdTopic,
                                         RabbitProperties properties) {
        return BindingBuilder.bind(warehouseQueue).to(cmdTopic).with(properties.warehouse().pattern());
    }

    /** DLQ: conserva los mensajes muertos para revisarlos o reprocesarlos. */
    @Bean
    public Queue warehouseDlq(RabbitProperties properties) {
        return QueueBuilder.durable(properties.warehouse().dlq()).build();
    }

    @Bean
    public Binding warehouseDlqBinding(@Qualifier("warehouseDlq") Queue warehouseDlq,
                                       @Qualifier("deadLetterExchange") DirectExchange deadLetter,
                                       RabbitProperties properties) {
        return BindingBuilder.bind(warehouseDlq).to(deadLetter).with(properties.warehouse().routingKey());
    }

    /**
     * Cola de espera para reintentos: sin consumidor. El mensaje expira tras retry.delay-ms y vuelve
     * a cmd.direct con warehouse.ticket. El TTL queda fijo al declararla; cambiarlo exige borrar la cola.
     */
    @Bean
    public Queue warehouseRetryQueue(RabbitProperties properties) {
        return QueueBuilder.durable(properties.warehouse().retryQueue())
                .withArgument("x-message-ttl", properties.retry().delayMs())
                .withArgument("x-dead-letter-exchange", properties.exchanges().direct())
                .withArgument("x-dead-letter-routing-key", properties.warehouse().routingKey())
                .build();
    }
}
