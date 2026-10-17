package cl.rutaexpress.notify.consumer;

import cl.rutaexpress.notify.config.RabbitProperties;
import cl.rutaexpress.notify.config.RabbitProperties.Route;

/** Mismos valores del contrato que application.yaml, para las pruebas de los consumidores. */
public final class TestRabbitProperties {

    public static final int MAX_ATTEMPTS = 3;

    private TestRabbitProperties() {
    }

    public static RabbitProperties create() {
        return new RabbitProperties(
                new RabbitProperties.Exchanges("cmd.direct", "cmd.topic", "cmd.dead.dlx"),
                new Route("q.cmd.email", "email.send", "email.*", "q.cmd.email.dlq", "q.cmd.email.retry"),
                new Route("q.cmd.warehouse", "warehouse.ticket", "warehouse.#", "q.cmd.warehouse.dlq", "q.cmd.warehouse.retry"),
                new Route("q.cmd.label", "label.gen", "label.*", "q.cmd.label.dlq", "q.cmd.label.retry"),
                new RabbitProperties.Audit("q.audit.deadletter"),
                new RabbitProperties.Retry(MAX_ATTEMPTS, 5000));
    }
}
