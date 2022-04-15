package bio.guoda.preston.cmd;

import com.beust.jcommander.Parameter;
import org.apache.commons.rdf.api.IRI;
import picocli.CommandLine;

import static bio.guoda.preston.RefNodeConstants.BIODIVERSITY_DATASET_GRAPH;

public class LoggingPersisting extends Persisting {

    public static final String LOG_FORMAT = "Log format";
    @Parameter(names = {"-l", "--log",}, description = LOG_FORMAT, converter = LoggerConverter.class)

    @CommandLine.Option(
            names = {"-l", "--log"},
            description = LOG_FORMAT
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
