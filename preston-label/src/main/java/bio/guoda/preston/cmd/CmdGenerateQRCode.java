package bio.guoda.preston.cmd;

import com.google.zxing.WriterException;
import org.apache.commons.rdf.api.IRI;
import picocli.CommandLine;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

@CommandLine.Command(
        name = "qrcode",
        aliases = {"label"},
        description = "generates a printable PNG image QRCode of provenance head or anchor. See also \"head\"."
)
public class CmdGenerateQRCode extends LoggingPersisting implements Runnable {

    @Override
    public void run() {
        AtomicReference<IRI> mostRecentLog = AnchorUtil.findHead(this);

        if (mostRecentLog.get() == null) {
            throw new RuntimeException("Cannot generate QRCode: no provenance logs found.");
        }

        try {
            QRCodeGenerator.generateQRCode(mostRecentLog.get(), getOutputStream());
        } catch (IOException | WriterException e) {
            throw new RuntimeException("failed to generate QRCode for [" + mostRecentLog.get().getIRIString() + "]", e);
        }

    }

}

