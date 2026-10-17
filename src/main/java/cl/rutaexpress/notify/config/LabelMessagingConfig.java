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
 * Caso de uso: etiqueta de despacho en PDF cuando el envío sale EN_RUTA.
 *
 * <p>Ruta del mensaje:
 * <pre>
 * shipments --(label.gen)--> cmd.direct --> q.cmd.label --> notify (LabelCommandListener)
 *              (label.*) --> cmd.topic  --^
 *
 * falla recuperable:    notify --> q.cmd.label.retry --(TTL)--> cmd.direct --(label.gen)--> q.cmd.label
 * falla no recuperable
 * o reintentos agotados: q.cmd.label --(NACK)--> cmd.dead.dlx --(label.gen)--> q.cmd.label.dlq
 *                                                             \--> q.audit.deadletter (audit)
 * </pre>
 */
@Configuration
public class LabelMessagingConfig {

    /** Cola principal. Sin routing key de dead-letter fija, para conservar label.gen al morir. */
    @Bean
    public Queue labelQueue(RabbitProperties properties) {
        return QueueBuilder.durable(properties.label().queue())
                .withArgument("x-dead-letter-exchange", properties.exchanges().deadLetter())
                .build();
    }

    @Bean
    public Binding labelDirectBinding(@Qualifier("labelQueue") Queue labelQueue,
                                      @Qualifier("cmdDirectExchange") DirectExchange cmdDirect,
                                      RabbitProperties properties) {
        return BindingBuilder.bind(labelQueue).to(cmdDirect).with(properties.label().routingKey());
    }

    @Bean
    public Binding labelTopicBinding(@Qualifier("labelQueue") Queue labelQueue,
                                     TopicExchange cmdTopic,
                                     RabbitProperties properties) {
        return BindingBuilder.bind(labelQueue).to(cmdTopic).with(properties.label().pattern());
    }

    /** DLQ: conserva los mensajes muertos para revisarlos o reprocesarlos. */
    @Bean
    public Queue labelDlq(RabbitProperties properties) {
        return QueueBuilder.durable(properties.label().dlq()).build();
    }

    @Bean
    public Binding labelDlqBinding(@Qualifier("labelDlq") Queue labelDlq,
                                   @Qualifier("deadLetterExchange") DirectExchange deadLetter,
                                   RabbitProperties properties) {
        return BindingBuilder.bind(labelDlq).to(deadLetter).with(properties.label().routingKey());
    }

    /**
     * Cola de espera para reintentos: sin consumidor. El mensaje expira tras retry.delay-ms y vuelve
     * a cmd.direct con label.gen. El TTL queda fijo al declararla; cambiarlo exige borrar la cola.
     */
    @Bean
    public Queue labelRetryQueue(RabbitProperties properties) {
        return QueueBuilder.durable(properties.label().retryQueue())
                .withArgument("x-message-ttl", properties.retry().delayMs())
                .withArgument("x-dead-letter-exchange", properties.exchanges().direct())
                .withArgument("x-dead-letter-routing-key", properties.label().routingKey())
                .build();
    }
}
