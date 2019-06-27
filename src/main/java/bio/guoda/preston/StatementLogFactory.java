package bio.guoda.preston;

import bio.guoda.preston.cmd.LogErrorHandler;
import org.apache.commons.rdf.api.Triple;
import bio.guoda.preston.cmd.Logger;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementLoggerNQuads;
import bio.guoda.preston.process.StatementLoggerTSV;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;

public class StatementLogFactory {
    public static StatementListener createLogger(Logger logMode) {
        return createLogger(logMode, System.out);
    }

    public static StatementListener createLogger(Logger logMode, final PrintStream out) {
        return createLogger(logMode, out, new LogErrorHandler() {
            @Override
            public void handleError() {
                // ignore
            }
        });
    }

    public static StatementListener createLogger(Logger logMode, final PrintStream out, LogErrorHandler handler) {
        StatementListener logger;
        if (Logger.tsv == logMode) {
            logger = new StatementLoggerTSV(out, handler);
        } else if (Logger.nquads == logMode) {
            logger = new StatementLoggerNQuads(out, handler);
        } else {
            logger = new StatementListener() {
                AtomicLong count = new AtomicLong(1);

                @Override
                public void on(Triple statement) {
                    long index = count.getAndIncrement();
                    if ((index % 80) == 0) {
                        out.println();
                    } else {
                        System.out.print(".");
                    }
                }
            };
        }
        return logger;
    }
}
