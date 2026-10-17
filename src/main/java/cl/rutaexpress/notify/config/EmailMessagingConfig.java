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
 * Caso de uso: notificación por correo al destinatario de un envío.
 *
 * <p>Ruta del mensaje:
 * <pre>
 * shipments --(email.send)--> cmd.direct --> q.cmd.email --> notify (EmailCommandListener)
 *                (email.*) --> cmd.topic  --^
 *
 * falla recuperable:    notify --> q.cmd.email.retry --(TTL)--> cmd.direct --(email.send)--> q.cmd.email
 * falla no recuperable
 * o reintentos agotados: q.cmd.email --(NACK)--> cmd.dead.dlx --(email.send)--> q.cmd.email.dlq
 *                                                             \--> q.audit.deadletter (audit)
 * </pre>
 */
@Configuration
public class EmailMessagingConfig {

    /** Cola principal. Sin routing key de dead-letter fija, para conservar email.send al morir. */
    @Bean
    public Queue emailQueue(RabbitProperties properties) {
        return QueueBuilder.durable(properties.email().queue())
                .withArgument("x-dead-letter-exchange", properties.exchanges().deadLetter())
                .build();
    }

    @Bean
    public Binding emailDirectBinding(@Qualifier("emailQueue") Queue emailQueue,
                                      @Qualifier("cmdDirectExchange") DirectExchange cmdDirect,
                                      RabbitProperties properties) {
        return BindingBuilder.bind(emailQueue).to(cmdDirect).with(properties.email().routingKey());
    }

    @Bean
    public Binding emailTopicBinding(@Qualifier("emailQueue") Queue emailQueue,
                                     TopicExchange cmdTopic,
                                     RabbitProperties properties) {
        return BindingBuilder.bind(emailQueue).to(cmdTopic).with(properties.email().pattern());
    }

    /** DLQ: conserva los mensajes muertos para revisarlos o reprocesarlos. */
    @Bean
    public Queue emailDlq(RabbitProperties properties) {
        return QueueBuilder.durable(properties.email().dlq()).build();
    }

    @Bean
    public Binding emailDlqBinding(@Qualifier("emailDlq") Queue emailDlq,
                                   @Qualifier("deadLetterExchange") DirectExchange deadLetter,
                                   RabbitProperties properties) {
        return BindingBuilder.bind(emailDlq).to(deadLetter).with(properties.email().routingKey());
    }

    /**
     * Cola de espera para reintentos: sin consumidor. El mensaje expira tras retry.delay-ms y vuelve
     * a cmd.direct con email.send. El TTL queda fijo al declararla; cambiarlo exige borrar la cola.
     */
    @Bean
    public Queue emailRetryQueue(RabbitProperties properties) {
        return QueueBuilder.durable(properties.email().retryQueue())
                .withArgument("x-message-ttl", properties.retry().delayMs())
                .withArgument("x-dead-letter-exchange", properties.exchanges().direct())
                .withArgument("x-dead-letter-routing-key", properties.email().routingKey())
                .build();
    }
}
