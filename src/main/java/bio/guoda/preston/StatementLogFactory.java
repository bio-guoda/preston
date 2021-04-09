package bio.guoda.preston;

import bio.guoda.preston.cmd.LogErrorHandler;
import bio.guoda.preston.cmd.LogTypes;
import bio.guoda.preston.process.StatementLoggerNQuads;
import bio.guoda.preston.process.StatementLoggerTSV;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import org.apache.commons.rdf.api.Quad;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;

public class StatementLogFactory {
    public static StatementsListener createPrintingLogger(LogTypes logMode) {
        return createPrintingLogger(logMode, System.out);
    }

    public static StatementsListener createPrintingLogger(LogTypes logMode, final PrintStream out) {
        return createPrintingLogger(logMode, out, () -> {
            // ignore
        });
    }

    public static StatementsListener createPrintingLogger(LogTypes logMode, final PrintStream out, LogErrorHandler handler) {
        StatementsListener logger;
        if (LogTypes.tsv == logMode) {
            logger = new StatementLoggerTSV(out);
        } else if (LogTypes.nquads == logMode) {
            logger = new StatementLoggerNQuads(out);
        } else {
            logger = new StatementsListenerAdapter() {
                AtomicLong count = new AtomicLong(1);

                @Override
                public void on(Quad statement) {
                    long index = count.getAndIncrement();
                    if ((index % 80) == 0) {
                        out.print("\n");
                    } else {
                        out.print(".");
                    }
                }
            };
        }
        return logger;
    }
}
