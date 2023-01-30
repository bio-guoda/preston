package bio.guoda.preston.store;

import org.apache.jena.riot.system.ErrorHandler;

public interface ErrorHandlerFactory {
    ErrorHandler createErrorHandler();
}
