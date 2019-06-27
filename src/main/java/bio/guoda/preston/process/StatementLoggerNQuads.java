package bio.guoda.preston.process;

import bio.guoda.preston.cmd.LogErrorHandler;
import bio.guoda.preston.cmd.ReplayUtil;
import org.apache.commons.rdf.api.Triple;

import java.io.PrintStream;

public class StatementLoggerNQuads implements StatementListener {

    private final PrintStream out;
    private final LogErrorHandler handler;

    public StatementLoggerNQuads(PrintStream printWriter) {
        this(printWriter, () -> {
            // ignore
        });
    }

    public StatementLoggerNQuads(PrintStream printWriter, LogErrorHandler handler) {
        this.out = printWriter;
        this.handler = handler;
    }

    @Override
    public void on(Triple statement) {
        ReplayUtil.checkAndHandle(out, handler);
        out.println(statement.toString());
    }

}
