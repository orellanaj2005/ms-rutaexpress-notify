package cl.rutaexpress.notify.handler;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Lightweight stand-in for a real shipping label integration: generates a simple one-page PDF
 * with plain text lines using Apache PDFBox and writes it to local disk. Follow-up work: replace
 * with a real label template/integration (or push to real object storage instead of the local
 * filesystem).
 */
@Component
public class LabelPdfGenerator {

    private static final Logger log = LoggerFactory.getLogger(LabelPdfGenerator.class);
    private static final float FONT_SIZE = 12f;
    private static final float LEADING = 18f;
    private static final float MARGIN = 50f;

    private final Path labelsDir;

    public LabelPdfGenerator(@Value("${labels.dir:./data/labels}") String labelsDir) {
        this.labelsDir = Path.of(labelsDir);
    }

    public Path generate(String shipmentId, String eventId, String recipientName, String destinationAddress,
                          double weightKg, String serviceId) {
        try {
            Files.createDirectories(labelsDir);
            Path file = labelsDir.resolve(shipmentId + "-" + eventId + ".pdf");

            List<String> lines = List.of(
                    "SHIPPING LABEL",
                    "Shipment ID: " + shipmentId,
                    "Recipient: " + recipientName,
                    "Destination: " + destinationAddress,
                    "Weight (kg): " + weightKg,
                    "Service: " + serviceId);

            try (PDDocument document = new PDDocument()) {
                PDPage page = new PDPage(PDRectangle.A4);
                document.addPage(page);
                PDType1Font font = new PDType1Font(Standard14Fonts.FontName.HELVETICA);

                try (PDPageContentStream contentStream = new PDPageContentStream(document, page)) {
                    contentStream.beginText();
                    contentStream.setFont(font, FONT_SIZE);
                    contentStream.newLineAtOffset(MARGIN, page.getMediaBox().getHeight() - MARGIN);
                    for (String line : lines) {
                        contentStream.showText(line);
                        contentStream.newLineAtOffset(0, -LEADING);
                    }
                    contentStream.endText();
                }

                document.save(file.toFile());
            }

            log.info("Etiqueta PDF generada en {} [shipment={}]", file, shipmentId);
            return file;
        } catch (IOException e) {
            throw new UncheckedIOException("No se pudo generar la etiqueta PDF para shipment " + shipmentId, e);
        }
    }
}
