package bio.guoda.preston.cmd;

import bio.guoda.preston.store.VersionUtil;
import com.google.zxing.WriterException;
import org.apache.commons.rdf.api.IRI;
import picocli.CommandLine;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

@CommandLine.Command(
        name = "qrcode",
        aliases = {"label"},
        description = "generates a printable PNG image QRCode of most recent provenance log version. See also \"head\"."
)
public class CmdGenerateQRCode extends LoggingPersisting implements Runnable {

    @Override
    public void run() {
        AtomicReference<IRI> mostRecentLog = new AtomicReference<>();
        try {
            getProvenanceTracer()
                    .trace(
                            getProvenanceAnchor(),
                            statement -> {
                                IRI iri = VersionUtil.mostRecentVersionForStatement(statement);
                                if (iri != null) {
                                    mostRecentLog.set(iri);
                                }
                            }
                    );
        } catch (IOException e) {
            throw new RuntimeException("Failed to get version history.", e);
        }

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

