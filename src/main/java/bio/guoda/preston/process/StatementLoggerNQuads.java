package bio.guoda.preston.process;

import bio.guoda.preston.cmd.ReplayUtil;
import org.apache.commons.rdf.api.Triple;

import java.io.PrintStream;

public class StatementLoggerNQuads implements StatementListener {

    private final PrintStream out;

    public StatementLoggerNQuads(PrintStream printWriter) {
        this.out = printWriter;
    }

    @Override
    public void on(Triple statement) {
        ReplayUtil.throwOnError(out);
        out.println(statement.toString());
    }

}
