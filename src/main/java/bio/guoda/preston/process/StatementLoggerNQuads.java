package bio.guoda.preston.process;

import org.apache.commons.rdf.api.Quad;

import java.io.PrintStream;

public class StatementLoggerNQuads implements StatementListener {

    private final PrintStream out;

    public StatementLoggerNQuads(PrintStream printWriter) {
        this.out = printWriter;
    }

    @Override
    public void on(Quad statement) {
        out.println(statement.toString());
    }

}
