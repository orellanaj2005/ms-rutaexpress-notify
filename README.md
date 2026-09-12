# ms-rutaexpress-notify

Servicio de notificaciones de RutaExpress. Es un **consumidor puro de RabbitMQ** (sin API HTTP
propia): escucha comandos publicados por `ms-rutaexpress-shipments` y ejecuta acciones locales
(enviar un email, generar un picking ticket de bodega, generar una etiqueta de envío en PDF).

## Stack técnico

- Java 21, Spring Boot 4.1.1 (parent `spring-boot-starter-parent`).
- `spring-boot-starter` + `spring-boot-starter-amqp` — **sin** `spring-boot-starter-web/webmvc**:
  este servicio no expone puertos ni endpoints HTTP, solo corre listeners de RabbitMQ durante el
  ciclo de vida del proceso (ver `infra/apps/compose.yml`, `notify-svc` no tiene `ports:`).
- **Sin base de datos.** La deduplicación de eventos es en memoria (ver más abajo).
- Apache PDFBox 3.0.x para generar las etiquetas de envío en PDF.
- Sin OAuth2/JWT: no tiene API HTTP entrante que proteger.
- No usa Lombok (siguiendo la convención del resto de los repos); usa records de Java 21 para los
  datos inmutables.

## Cómo correrlo localmente

```bash
./mvnw spring-boot:run
```

Variables de entorno relevantes (todas con default razonable, ver `application.yaml`):

| Variable | Default | Descripción |
|---|---|---|
| `RABBITMQ_HOST` | `100.62.195.17` | Host del RabbitMQ compartido del equipo (mismo host que `ms-rutaexpress-rabbitmq-admin`, puerto AMQP en vez del de management). |
| `RABBITMQ_PORT` | `5672` | Puerto AMQP. |
| `RABBITMQ_USERNAME` / `RABBITMQ_PASSWORD` | `guest` / `guest` | Credenciales AMQP. |
| `RETRY_MAX_ATTEMPTS` | `3` | Intentos de reproceso en memoria antes de mandar a la DLQ. |
| `RETRY_DELAY_MS` | `5000` | TTL (ms) de las colas de reintento/"parking lot". |
| `TICKETS_DIR` | `./data/tickets` | Carpeta donde se escriben los picking tickets. |
| `LABELS_DIR` | `./data/labels` | Carpeta donde se escriben las etiquetas PDF. |

Igual que en los demás repos, se puede usar un `.env` en la raíz (`spring.config.import:
optional:file:.env[.properties]`) para sobreescribir estas variables sin tocar `application.yaml`.

## Topología RabbitMQ

Esta guía sigue **al pie de la letra** la sección 7 ("Topología RabbitMQ") de
`docs/guia_javier_rutaexpress.md`, el documento de referencia acordado por el equipo. `notify` es
quien declara **toda** la topología (`config/RabbitTopologyConfig.java`, vía un bean
`Declarables` que `RabbitAdmin` auto-declara al arrancar). `ms-rutaexpress-shipments` (el
productor) solo declara por su lado el exchange `cmd.direct` — es idempotente y no genera
conflicto siempre que tipo/durabilidad coincidan con lo declarado acá.

### Exchanges (todos durables)

| Exchange | Tipo |
|---|---|
| `cmd.direct` | direct |
| `cmd.topic` | topic |
| `cmd.dead.dlx` | direct (es el DLX) |

### Colas principales (todas durables)

Cada cola principal se declara con el argumento `x-dead-letter-exchange: cmd.dead.dlx` **sin**
`x-dead-letter-routing-key` — así RabbitMQ preserva la routing key original del mensaje al
reenviarlo al DLX, que es justo lo que permite que los bindings de las DLQ (más abajo) funcionen.

| Cola | Binding en `cmd.direct` (routing key exacta) | Binding en `cmd.topic` (patrón) |
|---|---|---|
| `q.cmd.email` | `email.send` | `email.*` |
| `q.cmd.warehouse` | `warehouse.ticket` | `warehouse.#` |
| `q.cmd.label` | `label.gen` | `label.*` |

### DLQ (todas durables, sin argumentos especiales)

| DLQ | Binding en `cmd.dead.dlx` (misma routing key que el binding directo de la cola principal) |
|---|---|
| `q.cmd.email.dlq` | `email.send` |
| `q.cmd.warehouse.dlq` | `warehouse.ticket` |
| `q.cmd.label.dlq` | `label.gen` |

### Colas de retraso ("parking lot", solo uso interno de `notify`)

Patrón clásico TTL + dead-letter, ya que RabbitMQ plano no tiene reintento retrasado nativo. Cada
una es durable, con `x-message-ttl: ${retry.delay-ms:5000}` y dead-letter de vuelta a
`cmd.direct` con la routing key original. **Nunca tienen un `@RabbitListener` consumiéndolas** —
son solo un lugar de espera.

| Cola de retraso | Dead-letter de vuelta a routing key |
|---|---|
| `q.cmd.email.retry` | `email.send` |
| `q.cmd.warehouse.retry` | `warehouse.ticket` |
| `q.cmd.label.retry` | `label.gen` |

## Mecánica de ACK/NACK, reintentos y DLQ

`spring.rabbitmq.listener.simple.acknowledge-mode: manual`. Cada `@RabbitListener`
(`listener/EmailCommandListener`, `listener/WarehouseCommandListener`,
`listener/LabelCommandListener`, uno por cola) delega el flujo común a
`listener/AbstractCommandListener`:

1. Parsea el body como `messaging/CommandEnvelope`. Si falla el parseo, va directo a la DLQ
   (`basicNack(tag, false, false)`, sin reintentar) — un JSON corrupto nunca se va a arreglar solo.
2. **Chequeo de idempotencia primero**: si `envelope.eventId()` ya fue procesado
   (`idempotency/DedupService`), se hace `basicAck` de inmediato y no se reprocesa.
3. Si no, delega al handler correspondiente (email / picking ticket / etiqueta PDF).
4. Éxito: marca el `eventId` como procesado y hace `basicAck`.
5. Falla el handler: lee el header entero `x-retry-count` del mensaje (default 0 si no viene).
   - Si `retryCount < retry.max-attempts` (default 3): republica el mismo JSON del envelope a la
     cola de retraso correspondiente (`rabbitTemplate.convertAndSend("", "q.cmd.<x>.retry",
     message)`, exchange por defecto, routing key = nombre de la cola) con el header
     `x-retry-count` incrementado en 1, y hace `basicAck` del mensaje original (ya tomamos
     posesión de él al re-encolarlo nosotros mismos).
   - Si `retryCount >= retry.max-attempts`: `basicNack(tag, false, false)` (sin requeue) — esto
     dispara el dead-lettering **nativo** de RabbitMQ vía el argumento `x-dead-letter-exchange` de
     la cola, aterrizando automáticamente en la DLQ correcta.
   - En ambos casos se loguea en WARN con `eventId`, nombre de cola e intento.

Este mecanismo de reintento en la aplicación es una capa adicional sobre la red de seguridad
nativa del DLX (satisface el requerimiento de "ACK/NACK explícitos... y reenvío a la DLQ
correspondiente tras agotar reintentos").

## Envelope / forma del mensaje

```java
public record CommandEnvelope(String type, String eventId, Instant timestamp, String traceId,
                               String correlationId, JsonNode payload) {}
```

`payload` se mantiene como `JsonNode` (Jackson) a propósito: así `notify` no necesita compartir
DTOs de Java con el productor, cada handler lee defensivamente solo los campos que necesita
(`payload.path("campo").asText()` / `.asDouble()`, con fallback si falta un campo opcional).

### Tipos de comando y payloads esperados (producidos por `ms-rutaexpress-shipments`)

| Cola | `type` | Payload |
|---|---|---|
| `q.cmd.email` | `EmailSendCommand` | `{shipmentId, recipientEmail, recipientName, event, message}` |
| `q.cmd.warehouse` | `WarehouseTicketCommand` | `{shipmentId, originAddress, destinationAddress, weightKg}` |
| `q.cmd.label` | `LabelGenCommand` | `{shipmentId, recipientName, destinationAddress, weightKg, serviceId}` |

## Idempotencia / deduplicación (sin DB — restricción explícita)

`idempotency/DedupService` es un `ConcurrentHashMap<String, Instant>` (`eventId` ->
`processedAt`), con `isDuplicate(eventId)` y `markProcessed(eventId)`. Una tarea `@Scheduled`
(cada 1 hora, habilitada con `@EnableScheduling` en `NotifyApplication`) purga entradas de más de
24h para acotar el crecimiento de memoria.

**Limitación importante, documentada a propósito:** este store es **en memoria y de una sola
instancia**. Se reinicia con cada restart del servicio y **no funciona correctamente si se llega a
escalar `notify-svc` a más de una réplica** — cada instancia tendría su propio mapa
independiente, así que el mismo `eventId` podría procesarse una vez por réplica. Para dedup
correcto en múltiples réplicas haría falta un store respaldado por Redis o una base de datos, algo
fuera de alcance acá porque la especificación es explícitamente "sin DB".

## Handlers (livianos, stand-ins intencionales)

No se entregaron credenciales SMTP/SES ni de storage real para este ejercicio, así que los tres
handlers son implementaciones locales livianas, pensadas para ser reemplazadas más adelante:

- `handler/EmailSender` (interfaz) + `handler/LoggingEmailSender`: solo loguea en INFO lo que se
  habría enviado. Trabajo de seguimiento: implementación real contra SMTP o Amazon SES.
- `handler/PickingTicketGenerator`: genera un ticket de picking en texto plano en
  `${TICKETS_DIR:./data/tickets}/{shipmentId}-{eventId}.txt`. Trabajo de seguimiento: integración
  real contra el sistema de bodega/WMS, o subir el archivo a un storage real en vez del disco
  local.
- `handler/LabelPdfGenerator`: genera una etiqueta de envío en PDF (Apache PDFBox) en
  `${LABELS_DIR:./data/labels}/{shipmentId}-{eventId}.pdf`, con destinatario, dirección de
  destino, peso y servicio como líneas de texto. Trabajo de seguimiento: plantilla de etiqueta
  real y/o almacenamiento real en vez del disco local.

## Tests

No hay un broker real disponible en este entorno, así que las pruebas son unitarias puras con
Mockito, sin `@SpringBootTest` ni conexión real a RabbitMQ:

- `idempotency/DedupServiceTest`: marca un `eventId` como procesado y verifica `isDuplicate`;
  también prueba la limpieza por antigüedad usando un `Clock` inyectable falso.
- Un test por listener (`EmailCommandListenerTest`, `WarehouseCommandListenerTest`,
  `LabelCommandListenerTest`), cada uno construye el listener con `RabbitTemplate`, el handler de
  negocio y `DedupService` mockeados, más un `com.rabbitmq.client.Channel` mockeado, y llama al
  método del listener directamente con un `Message` armado a mano. Cada uno verifica:
  - éxito: `basicAck` una vez, `basicNack` nunca.
  - duplicado: el handler nunca se invoca y igual se hace `basicAck`.
  - falla con `x-retry-count` bajo el máximo: se republica a la cola `.retry` correcta vía
    `rabbitTemplate.convertAndSend` y se hace `basicAck` del mensaje original.
  - falla con `x-retry-count` en/sobre el máximo: `basicNack(tag, false, false)` y ninguna
    republicación.

No se agregó un test de contexto completo (`@SpringBootTest`) porque los `@RabbitListener`
arrancan sus contenedores en el `SmartLifecycle` del refresh del contexto e intentan conectarse
de inmediato al broker para declarar/consumir — eso lo haría depender de la disponibilidad real
del RabbitMQ compartido (o colgarse/fallar en este sandbox sin red hacia él), justo el
comportamiento flaky que la consigna permite evitar. Las pruebas unitarias de arriba cubren la
lógica de negocio sin esa dependencia.

Ejecutar todos los tests:

```bash
./mvnw test
```

(14 tests, todos verdes en este entorno.)

## Docker

```bash
docker build -t rutaexpress/notify:latest .
docker run --env-file .env rutaexpress/notify:latest
```

Sin `EXPOSE`: `notify-svc` no publica puertos en `infra/apps/compose.yml` (es un consumidor puro).
