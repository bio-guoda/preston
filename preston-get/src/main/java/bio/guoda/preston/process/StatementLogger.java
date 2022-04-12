package bio.guoda.preston.process;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

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
        try {
            IOUtils.write(message, os, StandardCharsets.UTF_8);
        } catch (IOException e) {
            handler.handleError();
        }
    }

}
