package bio.guoda.preston.cmd;

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
        AtomicReference<IRI> mostRecentLog = AnchorUtil.findHead(this);

        if (mostRecentLog.get() == null) {
            throw new RuntimeException("failed to print most recent provenance log version/hash");
        }
        try {
            IOUtils.write(mostRecentLog.get().getIRIString(), getOutputStream(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("failed to print most recent provenance log version/hash [" + mostRecentLog.get().getIRIString() + "]", e);
        }

    }

}

