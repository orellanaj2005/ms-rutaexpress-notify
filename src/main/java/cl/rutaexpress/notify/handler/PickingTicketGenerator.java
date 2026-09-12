package cl.rutaexpress.notify.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

/**
 * Lightweight stand-in for a real warehouse/WMS integration: writes a plain text picking ticket
 * to local disk. Follow-up work: replace with a real integration against the warehouse system
 * (or push to real object storage instead of the local filesystem).
 */
@Component
public class PickingTicketGenerator {

    private static final Logger log = LoggerFactory.getLogger(PickingTicketGenerator.class);

    private final Path ticketsDir;

    public PickingTicketGenerator(@Value("${tickets.dir:./data/tickets}") String ticketsDir) {
        this.ticketsDir = Path.of(ticketsDir);
    }

    public Path generate(String shipmentId, String eventId, String originAddress, String destinationAddress, double weightKg) {
        try {
            Files.createDirectories(ticketsDir);
            Path file = ticketsDir.resolve(shipmentId + "-" + eventId + ".txt");
            String content = """
                    PICKING TICKET
                    ==============
                    Shipment ID: %s
                    Origin: %s
                    Destination: %s
                    Weight (kg): %s
                    Generated at: %s
                    """.formatted(shipmentId, originAddress, destinationAddress, weightKg, Instant.now());
            Files.writeString(file, content, StandardCharsets.UTF_8);
            log.info("Picking ticket generado en {} [shipment={}]", file, shipmentId);
            return file;
        } catch (IOException e) {
            throw new UncheckedIOException("No se pudo generar el picking ticket para shipment " + shipmentId, e);
        }
    }
}
