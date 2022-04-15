package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.LogErrorHandler;
import bio.guoda.preston.process.StatementsListener;
import picocli.CommandLine;

@CommandLine.Command(
        name = "ls",
        aliases = {"log", "logs"},
        description = "Show biodiversity dataset provenance logs"
)
public class CmdList extends LoggingPersisting implements Runnable {

    @Override
    public void run() {
        run(LogErrorHandlerExitOnError.EXIT_ON_ERROR);
    }

    public void run(LogErrorHandler handler) {
        StatementsListener listener = StatementLogFactory
                .createPrintingLogger(
                        getLogMode(),
                        getOutputStream(),
                        handler);

        ReplayUtil.replay(listener, this);
    }

}
