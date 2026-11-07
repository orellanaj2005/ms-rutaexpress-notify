package cl.rutaexpress.notify.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Set;

/**
 * Configuración del consumidor Kafka de notify, leída desde {@code rutaexpress.kafka}.
 *
 * @param shipmentEventsTopic tópico que se escucha ({@code shipments.events})
 * @param dltTopic            tópico donde terminan los mensajes que no se pudieron procesar ({@code notify.DLT})
 * @param emailStates         estados de {@code ShipmentStatusChanged} que generan un correo al cliente
 */
@ConfigurationProperties(prefix = "rutaexpress.kafka")
public record KafkaNotifyProperties(String shipmentEventsTopic, String dltTopic, Set<String> emailStates) {
}
