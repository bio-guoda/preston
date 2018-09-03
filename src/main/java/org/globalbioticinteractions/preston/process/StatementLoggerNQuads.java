package org.globalbioticinteractions.preston.process;

import org.apache.commons.rdf.api.Triple;

import java.io.PrintStream;

public class StatementLoggerNQuads implements StatementListener {

    private final PrintStream out;

    public StatementLoggerNQuads() {
        this(System.out);
    }

    public StatementLoggerNQuads(PrintStream printWriter) {
        this.out = printWriter;

    }

    @Override
    public void on(Triple statement) {
        out.println(statement.toString());
    }

}
