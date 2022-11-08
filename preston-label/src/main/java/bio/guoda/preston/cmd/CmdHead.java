package bio.guoda.preston.cmd;

import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

@CommandLine.Command(
        name = "head",
        description = "shows most recent provenance log version/hash"
)
public class CmdHead extends LoggingPersisting implements Runnable {

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
            throw new RuntimeException("Cannot find most recent version: no provenance logs found.");
        }

        try {
            IOUtils.write(mostRecentLog.get().getIRIString(), getOutputStream(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("failed to print most recent provenance log version/hash [" + mostRecentLog.get().getIRIString() + "]", e);
        }

    }

}

