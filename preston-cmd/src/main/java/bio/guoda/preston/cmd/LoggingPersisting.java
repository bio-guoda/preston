package bio.guoda.preston.cmd;

import org.apache.commons.rdf.api.IRI;
import picocli.CommandLine;

import static bio.guoda.preston.RefNodeConstants.BIODIVERSITY_DATASET_GRAPH;

public class LoggingPersisting extends Persisting {

    @CommandLine.Option(
            names = {"-l", "--log"},
            description = "Log format"
    )
    private LogTypes logMode = LogTypes.nquads;

    protected LogTypes getLogMode() {
        return logMode;
    }

    private final IRI provenanceRoot = BIODIVERSITY_DATASET_GRAPH;

    public IRI getProvenanceRoot() {
        return this.provenanceRoot;
    }

}
