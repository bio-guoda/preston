package bio.guoda.preston;

import bio.guoda.preston.cmd.Cmd;
import bio.guoda.preston.cmd.LogErrorHandler;
import bio.guoda.preston.cmd.LogTypes;
import bio.guoda.preston.process.StatementLogger;
import bio.guoda.preston.process.StatementLoggerNQuads;
import bio.guoda.preston.process.StatementLoggerTSV;
import bio.guoda.preston.process.StatementsListener;
import org.apache.commons.rdf.api.Quad;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;

public class StatementLogFactory {
    public static StatementsListener createPrintingLogger(LogTypes logMode) {
        return createPrintingLogger(logMode, System.out);
    }

    public static StatementsListener createPrintingLogger(LogTypes logMode, final PrintStream out) {
        return createPrintingLogger(logMode, out, Cmd::stopProcessing);
    }

    public static StatementsListener createPrintingLogger(LogTypes logMode, final PrintStream out, LogErrorHandler handler) {
        StatementsListener logger;
        if (LogTypes.tsv == logMode) {
            logger = new StatementLoggerTSV(out, handler);
        } else if (LogTypes.nquads == logMode) {
            logger = new StatementLoggerNQuads(out, handler);
        } else {
            logger = new StatementLogger(out, handler) {
                AtomicLong count = new AtomicLong(1);

                @Override
                public void on(Quad statement) {
                    long index = count.getAndIncrement();
                    if ((index % 80) == 0) {
                        print("\n");
                    } else {
                        print(".");
                    }
                }
            };
        }
        return logger;
    }
}
