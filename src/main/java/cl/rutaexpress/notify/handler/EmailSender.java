package cl.rutaexpress.notify.handler;

/**
 * Sends a notification email. Kept as an interface so a real SMTP/SES implementation can be
 * swapped in later without touching the listener that calls it.
 */
public interface EmailSender {

    void send(String shipmentId, String recipientEmail, String recipientName, String event, String message);
}
