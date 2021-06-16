package bio.guoda.preston.cmd;

import com.beust.jcommander.Parameter;

public class LoggingPersisting extends Persisting {

    @Parameter(names = {"-l", "--log",}, description = "log format", converter = LoggerConverter.class)
    private LogTypes logMode = LogTypes.nquads;

    protected LogTypes getLogMode() {
        return logMode;
    }

}
