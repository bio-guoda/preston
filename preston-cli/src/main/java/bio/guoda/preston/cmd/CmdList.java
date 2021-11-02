package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.LogErrorHandler;
import bio.guoda.preston.process.StatementsListener;
import com.beust.jcommander.Parameters;

@Parameters(separators = "= ", commandDescription = "show biodiversity dataset provenance logs")
public class CmdList extends LoggingPersisting implements Runnable {

    @Override
    public void run() {
        run(() -> System.exit(0));
    }

    public void run(LogErrorHandler handler) {
        StatementsListener listener = StatementLogFactory
                .createPrintingLogger(getLogMode(), System.out, handler);

        ReplayUtil.replay(listener, this);
    }

}
