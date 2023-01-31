package bio.guoda.preston.process;

import bio.guoda.preston.RDFUtil;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;

import java.io.InputStream;

public class EmittingStreamRDF {
    private final RDF rdfSimple = new SimpleRDF();
    private final StatementsEmitter emitter;
    private final ProcessorState context;

    public EmittingStreamRDF(StatementsEmitter emitter) {
        this(emitter, new ProcessorStateAlwaysContinue());
    }

    public EmittingStreamRDF(StatementsEmitter emitter,
                             ProcessorState processorState) {
        this.emitter = emitter;
        this.context = processorState;
    }

    public void parseAndEmit(InputStream inputStream) {
        AbstractRDFHandler handler = new AbstractRDFHandler() {
            @Override
            public void handleStatement(Statement statement) throws RDFHandlerException {
                if (!context.shouldKeepProcessing()) {
                    throw new StopProcessingException();
                }
                copyOnEmit(RDFUtil.asQuad(statement));
            }
        };
        RDFUtil.parseQuads(inputStream, handler);
    }


    private void copyOnEmit(org.apache.commons.rdf.api.Quad quad) {
        org.apache.commons.rdf.api.Quad copyOfTriple = rdfSimple.createQuad(
                quad.getGraphName().orElse(null),
                quad.getSubject(),
                quad.getPredicate(),
                quad.getObject());
        emitter.emit(copyOfTriple);
    }

}
