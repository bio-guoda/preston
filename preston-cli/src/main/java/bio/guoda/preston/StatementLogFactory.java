package bio.guoda.preston;

import bio.guoda.preston.process.LogErrorHandler;
import bio.guoda.preston.cmd.LogTypes;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementLogger;
import bio.guoda.preston.process.StatementLoggerNQuads;
import bio.guoda.preston.process.StatementLoggerTSV;
import bio.guoda.preston.process.StatementsListener;
import org.apache.commons.rdf.api.Quad;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;

public class StatementLogFactory {

    public static StatementsListener createPrintingLogger(LogTypes logMode, final OutputStream out, ProcessorState processorState) {
        return createPrintingLogger(logMode, new PrintStream(out), processorState::stopProcessing);
    }

    public static StatementsListener createPrintingLogger(LogTypes logMode, final OutputStream out, LogErrorHandler handler) {
        return createPrintingLogger(logMode, new PrintStream(out), handler);
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
