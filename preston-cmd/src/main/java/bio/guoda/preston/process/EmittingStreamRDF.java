package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.apache.tika.sax.StoppingEarlyException;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.ParseErrorListener;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.nquads.NQuadsParserFactory;

import java.io.IOException;
import java.io.InputStream;

public class EmittingStreamRDF {
    private final RDF rdf = new JenaRDF();
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
        RDFParser rdfParser = new NQuadsParserFactory().getParser();
        rdfParser.setRDFHandler(new RDFHandler() {
            @Override
            public void startRDF() throws RDFHandlerException {

            }

            @Override
            public void endRDF() throws RDFHandlerException {

            }

            @Override
            public void handleNamespace(String s, String s1) throws RDFHandlerException {

            }

            @Override
            public void handleStatement(Statement statement) throws RDFHandlerException {
                if (!context.shouldKeepProcessing()) {
                    throw new StopProcessingException();
                }
                copyOnEmit(asQuad(statement));
            }

            @Override
            public void handleComment(String s) throws RDFHandlerException {

            }
        });
        try {
            rdfParser.setStopAtFirstError(false);
            rdfParser.parse(inputStream);
        } catch (IOException e) {
            // optimistic parsing
        } catch (StopProcessingException e) {
            // handler signalling that parsing should stop
        }

    }

    private Quad asQuad(Statement statement) {
        BlankNodeOrIRI subj = parseSubj(statement.getSubject());

        IRI predicate = RefNodeFactory.toIRI(statement.getPredicate().stringValue());

        RDFTerm obj = parseObj(statement.getObject());

        IRI graph = null;
        if (statement.getContext() != null) {
            graph = RefNodeFactory.toIRI(statement.getContext().stringValue());
        }

        return RefNodeFactory.toStatement(
                graph,
                subj,
                predicate,
                obj
        );
    }


    private BlankNodeOrIRI parseSubj(org.eclipse.rdf4j.model.Resource node1) {
        BlankNodeOrIRI subj;
        if (node1.isBNode()) {
            subj = RefNodeFactory.toBlank(node1.stringValue());
        } else {
            subj = RefNodeFactory.toIRI(node1.stringValue());
        }
        return subj;
    }

    private RDFTerm parseObj(Value objNode1) {
        RDFTerm obj;
        if (objNode1.isLiteral()) {
            org.eclipse.rdf4j.model.Literal literal = (org.eclipse.rdf4j.model.Literal) objNode1;
            org.eclipse.rdf4j.model.IRI datatype = literal.getDatatype();
            if (literal.getLanguage().isPresent()) {
                obj = RefNodeFactory.toLiteral(literal.getLabel(), literal.getLanguage().get());
            } else if (datatype == null) {
                obj = RefNodeFactory.toLiteral(literal.getLabel());
            } else {
                IRI dataType = RefNodeFactory.toIRI(literal.getDatatype().stringValue());
                obj = RefNodeFactory.toLiteral(literal.getLabel(), dataType);
            }
        } else if (objNode1.isBNode()) {
            obj = RefNodeFactory.toBlank(objNode1.stringValue());
        } else {
            obj = RefNodeFactory.toIRI(objNode1.stringValue());
        }
        return obj;
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
