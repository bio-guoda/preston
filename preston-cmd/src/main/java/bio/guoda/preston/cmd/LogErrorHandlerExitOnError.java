package bio.guoda.preston.cmd;

import bio.guoda.preston.process.LogErrorHandler;

public class LogErrorHandlerExitOnError implements LogErrorHandler {
    public static final LogErrorHandlerExitOnError EXIT_ON_ERROR = new LogErrorHandlerExitOnError();

    private LogErrorHandlerExitOnError() {

    }

    @Override
    public void handleError() {
        System.exit(0);
    }
}
