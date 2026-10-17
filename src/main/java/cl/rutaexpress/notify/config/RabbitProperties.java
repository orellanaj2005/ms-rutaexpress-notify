package cl.rutaexpress.notify.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Nombres de la topología RabbitMQ, leídos desde {@code rutaexpress.rabbitmq} en application.yaml
 * con la misma estructura del contrato de topología del equipo. Ninguna clase escribe nombres de
 * colas, exchanges o routing keys a mano: todas los obtienen de aquí.
 */
@ConfigurationProperties(prefix = "rutaexpress.rabbitmq")
public record RabbitProperties(
        Exchanges exchanges,
        Route email,
        Route warehouse,
        Route label,
        Audit audit,
        Retry retry) {

    /** Exchanges compartidos: comandos directos, comandos por patrón y mensajes muertos. */
    public record Exchanges(String direct, String topic, String deadLetter) {
    }

    /** Ruta completa de un caso de uso: cola principal, enrutamiento, DLQ y cola de reintento. */
    public record Route(String queue, String routingKey, String pattern, String dlq, String retryQueue) {
    }

    /** Cola donde audit registra una copia de cada mensaje muerto (la declara audit). */
    public record Audit(String deadLetterQueue) {
    }

    /** Intentos máximos antes de enviar a la DLQ y espera entre reintentos. */
    public record Retry(int maxAttempts, long delayMs) {
    }
}
