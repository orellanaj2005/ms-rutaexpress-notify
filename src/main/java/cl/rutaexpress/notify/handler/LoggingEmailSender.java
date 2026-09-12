package cl.rutaexpress.notify.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Lightweight stand-in for a real email integration (no SMTP/SES credentials were provided for
 * this exercise). Simply logs what would have been sent. Follow-up work: replace with a real
 * SMTP or Amazon SES-backed implementation of {@link EmailSender}.
 */
@Component
public class LoggingEmailSender implements EmailSender {

    private static final Logger log = LoggerFactory.getLogger(LoggingEmailSender.class);

    @Override
    public void send(String shipmentId, String recipientEmail, String recipientName, String event, String message) {
        log.info("Enviando email a {} ({}): {} [shipment={}, event={}]",
                recipientEmail, recipientName, message, shipmentId, event);
    }
}
