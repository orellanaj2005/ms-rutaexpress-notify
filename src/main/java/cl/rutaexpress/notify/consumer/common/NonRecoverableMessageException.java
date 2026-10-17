package cl.rutaexpress.notify.consumer.common;

/**
 * Error que no se arregla reintentando: el mensaje en sí es inválido (sin destinatario, peso
 * inválido, sin payload...). {@link AbstractCommandListener} lo envía directo a la DLQ con NACK,
 * sin pasar por la cola de reintento.
 */
public class NonRecoverableMessageException extends RuntimeException {

    public NonRecoverableMessageException(String message) {
        super(message);
    }
}
