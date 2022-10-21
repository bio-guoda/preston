package bio.guoda.preston.cmd;

import picocli.CommandLine;

public class LoggingPersisting extends Persisting {

    @CommandLine.Option(
            names = {"-l", "--log"},
            description = "Log format. Supported values: ${COMPLETION-CANDIDATES}."
    )
    private LogTypes logMode = LogTypes.nquads;

    protected LogTypes getLogMode() {
        return logMode;
    }

}
