package bio.guoda.preston;

import bio.guoda.preston.cmd.LogErrorHandler;
import org.apache.commons.rdf.api.Triple;
import bio.guoda.preston.cmd.Logger;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementLoggerNQuads;
import bio.guoda.preston.process.StatementLoggerTSV;
import org.apache.commons.rdf.api.TripleLike;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;

public class StatementLogFactory {
    public static StatementListener createPrintingLogger(Logger logMode) {
        return createPrintingLogger(logMode, System.out);
    }

    public static StatementListener createPrintingLogger(Logger logMode, final PrintStream out) {
        return createPrintingLogger(logMode, out, new LogErrorHandler() {
            @Override
            public void handleError() {
                // ignore
            }
        });
    }

    public static StatementListener createPrintingLogger(Logger logMode, final PrintStream out, LogErrorHandler handler) {
        StatementListener logger;
        if (Logger.tsv == logMode) {
            logger = new StatementLoggerTSV(out);
        } else if (Logger.nquads == logMode) {
            logger = new StatementLoggerNQuads(out);
        } else {
            logger = new StatementListener() {
                AtomicLong count = new AtomicLong(1);

                @Override
                public void on(TripleLike statement) {
                    long index = count.getAndIncrement();
                    if ((index % 80) == 0) {
                        out.println();
                    } else {
                        out.print(".");
                    }
                }
            };
        }
        return logger;
    }
}
