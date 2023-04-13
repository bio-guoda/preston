package bio.guoda.preston;

import bio.guoda.preston.process.LogErrorHandler;
import bio.guoda.preston.cmd.LogTypes;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementLogger;
import bio.guoda.preston.process.StatementLoggerNQuads;
import bio.guoda.preston.process.StatementsListener;
import org.apache.commons.rdf.api.Quad;

import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

public class StatementLogFactory {

    public static StatementsListener createPrintingLogger(LogTypes logMode, final OutputStream out, ProcessorState processorState) {
        return createPrintingLogger(logMode, out, processorState::stopProcessing);
    }

    public static StatementsListener createPrintingLogger(LogTypes logMode, final OutputStream out, LogErrorHandler handler) {
        StatementsListener logger;
        if (LogTypes.tsv == logMode) {
            logger = new StatementLoggerTSV(out, handler);
        } else {
            logger = new StatementLoggerNQuads(out, handler);
        }
        return logger;
    }
}
