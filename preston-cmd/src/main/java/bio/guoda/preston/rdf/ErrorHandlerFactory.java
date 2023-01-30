package bio.guoda.preston.rdf;

import org.apache.jena.riot.system.ErrorHandler;

public interface ErrorHandlerFactory {
    ErrorHandler createErrorHandler();
}
