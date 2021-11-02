package bio.guoda.preston.process;

import java.io.PrintStream;

abstract public class StatementLogger extends StatementsListenerAdapter {

    private final PrintStream out;
    private final LogErrorHandler handler;

    public StatementLogger(PrintStream printWriter) {
        this(printWriter, () -> {
            // ignore
        });
    }

    public StatementLogger(PrintStream printWriter, LogErrorHandler handler) {
        this.out = printWriter;
        this.handler = handler;
    }

    protected void print(String message) {
        out.print(message);
        if (out.checkError()) {
            handler.handleError();
        }
    }

}
