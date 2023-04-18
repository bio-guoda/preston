package bio.guoda.preston.cmd;

import bio.guoda.preston.process.ProcessorReadOnly;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.google.zxing.BarcodeFormat;
import com.google.zxing.WriterException;
import com.google.zxing.common.BitMatrix;
import com.google.zxing.qrcode.QRCodeWriter;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;

public class QRCodeGenerator {

    public static void generateQRCode(IRI iri, OutputStream output) throws WriterException, IOException {
        BitMatrix byteMatrix =
                new QRCodeWriter().encode(
                        "https://linker.bio/" + iri.getIRIString(),
                        BarcodeFormat.QR_CODE,
                        256,
                        256,
                        null);

        // Make the BufferedImage that are to hold the QRCode
        int matrixWidth = byteMatrix.getWidth();
        int matrixHeight = byteMatrix.getHeight();
        BufferedImage image = new BufferedImage(
                matrixWidth,
                matrixHeight,
                BufferedImage.TYPE_INT_RGB
        );

        image.createGraphics();

        Graphics2D graphics = (Graphics2D) image.getGraphics();
        graphics.setColor(Color.WHITE);
        graphics.fillRect(0, 0, matrixWidth, matrixHeight);
        graphics.setColor(Color.BLACK);

        for (int i = 0; i < matrixWidth; i++) {
            for (int j = 0; j < matrixWidth; j++) {
                if (byteMatrix.get(i, j)) {
                    graphics.fillRect(i, j, 1, 1);
                }
            }
        }
        ImageIO.write(image, "png", output);
    }

}
