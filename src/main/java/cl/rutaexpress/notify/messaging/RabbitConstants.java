package cl.rutaexpress.notify.messaging;

/**
 * Central place for RabbitMQ topology names (exchanges, queues, routing keys)
 * so config declarations, listeners and tests all reference the same literals.
 * See section 7 ("Topología RabbitMQ") of docs/guia_javier_rutaexpress.md.
 */
public final class RabbitConstants {

    private RabbitConstants() {
    }

    // Exchanges
    public static final String EXCHANGE_DIRECT = "cmd.direct";
    public static final String EXCHANGE_TOPIC = "cmd.topic";
    public static final String EXCHANGE_DLX = "cmd.dead.dlx";

    // Main queues
    public static final String QUEUE_EMAIL = "q.cmd.email";
    public static final String QUEUE_WAREHOUSE = "q.cmd.warehouse";
    public static final String QUEUE_LABEL = "q.cmd.label";

    // DLQ queues
    public static final String QUEUE_EMAIL_DLQ = "q.cmd.email.dlq";
    public static final String QUEUE_WAREHOUSE_DLQ = "q.cmd.warehouse.dlq";
    public static final String QUEUE_LABEL_DLQ = "q.cmd.label.dlq";

    // Delay ("parking lot") queues used for in-app retry backoff
    public static final String QUEUE_EMAIL_RETRY = "q.cmd.email.retry";
    public static final String QUEUE_WAREHOUSE_RETRY = "q.cmd.warehouse.retry";
    public static final String QUEUE_LABEL_RETRY = "q.cmd.label.retry";

    // Direct exact routing keys
    public static final String ROUTING_EMAIL = "email.send";
    public static final String ROUTING_WAREHOUSE = "warehouse.ticket";
    public static final String ROUTING_LABEL = "label.gen";

    // Topic patterns
    public static final String PATTERN_EMAIL = "email.*";
    public static final String PATTERN_WAREHOUSE = "warehouse.#";
    public static final String PATTERN_LABEL = "label.*";

    // Message header carrying the retry attempt count
    public static final String HEADER_RETRY_COUNT = "x-retry-count";
}
