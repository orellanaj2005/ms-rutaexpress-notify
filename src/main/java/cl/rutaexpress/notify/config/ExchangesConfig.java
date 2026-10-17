package cl.rutaexpress.notify.config;

import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Exchanges compartidos por los tres casos de uso (correo, bodega y etiqueta).
 *
 * <ul>
 *   <li>{@code cmd.direct}: shipments publica cada comando con su routing key exacta.</li>
 *   <li>{@code cmd.topic}: las mismas colas también reciben por patrón ({@code email.*}, ...).</li>
 *   <li>{@code cmd.dead.dlx}: recibe los mensajes que las colas principales rechazan y los reparte
 *       a la DLQ de su caso de uso (y audit recibe una copia en {@code q.audit.deadletter}).</li>
 * </ul>
 *
 * <p>Todos son durables y sin auto-delete. audit declara {@code cmd.dead.dlx} con los mismos
 * atributos; si difirieran, RabbitMQ rechazaría la segunda declaración.
 */
@Configuration
public class ExchangesConfig {

    @Bean
    public DirectExchange cmdDirectExchange(RabbitProperties properties) {
        return new DirectExchange(properties.exchanges().direct(), true, false);
    }

    @Bean
    public TopicExchange cmdTopicExchange(RabbitProperties properties) {
        return new TopicExchange(properties.exchanges().topic(), true, false);
    }

    @Bean
    public DirectExchange deadLetterExchange(RabbitProperties properties) {
        return new DirectExchange(properties.exchanges().deadLetter(), true, false);
    }
}
