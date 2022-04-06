package bio.guoda.preston.cmd;

import com.beust.jcommander.Parameter;
import org.apache.commons.rdf.api.IRI;

import static bio.guoda.preston.RefNodeConstants.BIODIVERSITY_DATASET_GRAPH;

public class LoggingPersisting extends Persisting {

    @Parameter(names = {"-l", "--log",}, description = "log format", converter = LoggerConverter.class)
    private LogTypes logMode = LogTypes.nquads;

    protected LogTypes getLogMode() {
        return logMode;
    }

    private final IRI provenanceRoot = BIODIVERSITY_DATASET_GRAPH;

    public IRI getProvenanceRoot() {
        return this.provenanceRoot;
    }

}
