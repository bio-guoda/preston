package bio.guoda.preston.rdf;

import org.apache.jena.riot.system.ErrorHandler;
import org.apache.jena.riot.system.ErrorHandlerFactory;

public class ErrorHandlerLoggingFactory implements bio.guoda.preston.rdf.ErrorHandlerFactory {

    @Override
    public ErrorHandler createErrorHandler() {
        return ErrorHandlerFactory.errorHandlerStd;
    }
}
