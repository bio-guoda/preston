package bio.guoda.preston.process;

import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.ProcessorStateAlwaysContinue;
import bio.guoda.preston.process.StatementsEmitter;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.apache.jena.atlas.iterator.IteratorResourceClosing;
import org.apache.jena.riot.system.ErrorHandler;
import org.apache.jena.riot.system.ErrorHandlerFactory;
import org.apache.jena.sparql.core.Quad;

import java.io.InputStream;
import java.util.Iterator;

public class EmittingStreamRDF {
    private final RDF rdf = new JenaRDF();
    private final RDF rdfSimple = new SimpleRDF();
    private final StatementsEmitter emitter;
    private final ProcessorState context;
    private final ErrorHandler errorHandler;

    public EmittingStreamRDF(StatementsEmitter emitter) {
        this(emitter, new ProcessorStateAlwaysContinue());
    }

    public EmittingStreamRDF(StatementsEmitter emitter, ProcessorState processorState) {
        this(emitter, processorState, new ErrorHandlerLoggingFactory());
    }

    public EmittingStreamRDF(StatementsEmitter emitter,
                             ProcessorState processorState,
                             bio.guoda.preston.store.ErrorHandlerFactory factory) {
        this.emitter = emitter;
        this.context = processorState;
        this.errorHandler = factory.createErrorHandler();
    }

    public void parseAndEmit(InputStream inputStream) {
        Iterator<Quad> iteratorNQuads = RDFUtil.asQuads(inputStream, errorHandler);
        Iterator<Quad> iteratorQuads = new IteratorResourceClosing<>(iteratorNQuads, inputStream);
        while (context.shouldKeepProcessing() && iteratorQuads.hasNext()) {
            Quad nextQuad = iteratorQuads.next();
            copyOnEmit(JenaRDF.asQuad(rdf, nextQuad));
        }
    }


    private void copyOnEmit(org.apache.commons.rdf.api.Quad quad) {
        org.apache.commons.rdf.api.Quad copyOfTriple = rdfSimple.createQuad(
                quad.getGraphName().orElse(null),
                quad.getSubject(),
                quad.getPredicate(),
                quad.getObject());
        emitter.emit(copyOfTriple);
    }

    private static class ErrorHandlerLoggingFactory implements bio.guoda.preston.store.ErrorHandlerFactory {

        @Override
        public ErrorHandler createErrorHandler() {
            return ErrorHandlerFactory.errorHandlerStd;
        }
    }
}
