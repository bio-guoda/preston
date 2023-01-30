package bio.guoda.preston.rdf;

import org.apache.jena.riot.system.ErrorHandler;

public class ErrorHandlerNOOPFactory implements ErrorHandlerFactory {

    @Override
    public ErrorHandler createErrorHandler() {
        return new ErrorHandler() {
            @Override
            public void warning(String message, long line, long col) {
                // ignore
            }

            @Override
            public void error(String message, long line, long col) {
                // ignore
            }

            @Override
            public void fatal(String message, long line, long col) {
                // ignore
            }
        };
    }
}
