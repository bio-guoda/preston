package bio.guoda.preston.process;

import org.apache.commons.rdf.api.Quad;

import java.io.OutputStream;
import java.io.PrintStream;

public class StatementLoggerNQuads extends StatementLogger {

    public StatementLoggerNQuads(OutputStream os) {
        super(os);
    }

    public StatementLoggerNQuads(OutputStream os, LogErrorHandler handler) {
        super(os, handler);
    }

    @Override
    public void on(Quad statement) {
        print(statement.toString() + "\n");
    }

}
