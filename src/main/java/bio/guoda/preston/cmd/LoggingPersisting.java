package bio.guoda.preston.cmd;

import com.beust.jcommander.Parameter;

public class LoggingPersisting extends Persisting {

    @Parameter(names = {"-l", "--log",}, description = "log format", converter = LoggerConverter.class)
    private Logger logMode = Logger.nquads;

    protected Logger getLogMode() {
        return logMode;
    }

}
