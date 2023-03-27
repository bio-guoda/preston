package bio.guoda.preston.process;

import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.rdf4j.RDF4J;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;

import java.io.IOException;
import java.io.InputStream;

import static bio.guoda.preston.RefNodeFactory.toIRI;

public class EmittingStreamOfAnyQuad extends EmittingStreamAbstract {

    public EmittingStreamOfAnyQuad(StatementEmitter emitter) {
        super(emitter);
    }

    public EmittingStreamOfAnyQuad(StatementEmitter emitter, ProcessorState processorState) {
        super(emitter, processorState);
    }

    @Override
    public void parseAndEmit(InputStream inputStream) {

        RDFParser rdfParser = Rio.createParser(RDFFormat.NQUADS);

        rdfParser.setRDFHandler(new RDFHandler() {
            @Override
            public void startRDF() throws RDFHandlerException {

            }

            @Override
            public void endRDF() throws RDFHandlerException {

            }

            @Override
            public void handleNamespace(String prefix, String uri) throws RDFHandlerException {

            }

            @Override
            public void handleStatement(Statement st) throws RDFHandlerException {
                if (!getContext().shouldKeepProcessing()) {
                    throw new RDFHandlerException("stop processing");
                }
                Quad quad = new RDF4J().asQuad(st);
                if (!(quad.getSubject() instanceof BlankNode) && !(quad.getObject() instanceof BlankNode)) {
                    copyOnEmit(quad);
                }
            }

            @Override
            public void handleComment(String comment) throws RDFHandlerException {

            }
        });

        try {
            rdfParser.parse(inputStream);
        } catch (IOException | RDFParseException | RDFHandlerException ex) {
            // ignore
        }
    }


}
