package bio.guoda.preston.process;

import org.apache.commons.rdf.api.Quad;

import java.io.PrintStream;

public class StatementLoggerNQuads extends StatementLogger {

    public StatementLoggerNQuads(PrintStream printWriter) {
        super(printWriter);
    }

    public StatementLoggerNQuads(PrintStream printWriter, LogErrorHandler handler) {
        super(printWriter, handler);
    }

    @Override
    public void on(Quad statement) {
        print(statement.toString() + "\n");
    }

}
