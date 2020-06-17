package bio.guoda.preston.process;

import org.apache.commons.rdf.api.Quad;

import java.io.PrintStream;

public class StatementLoggerNQuads extends StatementsListenerAdapter {

    private final PrintStream out;

    public StatementLoggerNQuads(PrintStream printWriter) {
        this.out = printWriter;
    }

    @Override
    public void on(Quad statement) {
        out.print(statement.toString() + "\n");
    }

}
