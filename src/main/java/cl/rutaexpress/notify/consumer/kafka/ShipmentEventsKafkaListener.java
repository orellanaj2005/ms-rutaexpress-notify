package cl.rutaexpress.notify.consumer.kafka;

import cl.rutaexpress.notify.service.ShipmentNotificationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consume {@code shipments.events} en el grupo {@code ms-rutaexpress-notify} con concurrencia 3 (una
 * hebra por partición). Solo recibe y delega: la lógica está en {@link ShipmentNotificationService}
 * y los reintentos y el envío a {@code notify.DLT} en {@code KafkaConsumerConfig}.
 */
@Component
public class ShipmentEventsKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(ShipmentEventsKafkaListener.class);

    private final ShipmentNotificationService notificationService;

    public ShipmentEventsKafkaListener(ShipmentNotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @KafkaListener(id = "notify-shipment-events",
            topics = "${rutaexpress.kafka.shipment-events-topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "${rutaexpress.kafka.concurrency:3}")
    public void onMessage(ConsumerRecord<String, String> record) {
        log.debug("Recibido topic={} partition={} offset={} key={}",
                record.topic(), record.partition(), record.offset(), record.key());
        notificationService.handle(record.value());
    }
}
