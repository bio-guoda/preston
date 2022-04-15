package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.LogErrorHandler;
import bio.guoda.preston.process.StatementsListener;
import com.beust.jcommander.Parameters;
import picocli.CommandLine;

@Parameters(separators = "= ", commandDescription = CmdList.SHOW_BIODIVERSITY_DATASET_PROVENANCE_LOGS)

@CommandLine.Command(
        name = "ls",
        aliases = {"log", "logs"},
        description = CmdList.SHOW_BIODIVERSITY_DATASET_PROVENANCE_LOGS
)

public class CmdList extends LoggingPersisting implements Runnable {

    public static final String SHOW_BIODIVERSITY_DATASET_PROVENANCE_LOGS = "Show biodiversity dataset provenance logs";

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
