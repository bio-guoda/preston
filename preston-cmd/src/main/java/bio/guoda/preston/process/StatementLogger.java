package bio.guoda.preston.process;

import java.io.OutputStream;

abstract public class StatementLogger extends StatementsListenerAdapter {

    private final OutputStream os;
    private final LogErrorHandler handler;

    public StatementLogger(OutputStream os) {
        this(os, () -> {
            // ignore
        });
    }

    public StatementLogger(OutputStream os, LogErrorHandler handler) {
        this.os = os;
        this.handler = handler;
    }

    protected void print(String message) {
        CmdUtil.print(message, os, handler);
    }

}
