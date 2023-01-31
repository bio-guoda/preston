package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.apache.jena.riot.system.ErrorHandler;
import org.apache.jena.riot.system.ErrorHandlerFactory;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

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
        NxParser nxp = new NxParser();
        nxp.parse(inputStream, StandardCharsets.UTF_8);
        while (context.shouldKeepProcessing() && nxp.hasNext()) {
            Node[] statement = nxp.next();
            if (statement.length > 2) {
                copyOnEmit(asQuad(statement));
            }
        }
    }

    private Quad asQuad(Node[] statement) {
        BlankNodeOrIRI subj = parseSubj(statement[0]);

        IRI predicate = RefNodeFactory.toIRI(statement[1].getLabel());

        RDFTerm obj = parseObj(statement[2]);

        IRI graph = null;
        if (statement.length > 3) {
            graph = RefNodeFactory.toIRI(statement[3].getLabel());
        }

        return RefNodeFactory.toStatement(
                graph,
                subj,
                predicate,
                obj
        );
    }

    private BlankNodeOrIRI parseSubj(Node node1) {
        Node node = node1;
        BlankNodeOrIRI subj;
        if (node instanceof BNode) {
            subj = RefNodeFactory.toBlank(node.getLabel());
        } else {
            subj = RefNodeFactory.toIRI(node.getLabel());
        }
        return subj;
    }

    private RDFTerm parseObj(Node objNode1) {
        Node objNode = objNode1;
        RDFTerm obj;
        if (objNode instanceof Literal) {
            Resource datatype = ((Literal) objNode).getDatatype();
            if (datatype == null) {
                obj = RefNodeFactory.toLiteral(objNode.getLabel());
            } else {
                IRI dataType = RefNodeFactory.toIRI(datatype.getLabel());
                obj = RefNodeFactory.toLiteral(objNode.getLabel(), dataType);
            }
        } else if (objNode instanceof BNode) {
            obj = RefNodeFactory.toBlank(objNode.getLabel());
        } else {
            obj = RefNodeFactory.toIRI(objNode.getLabel());
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

    private static class ErrorHandlerLoggingFactory implements bio.guoda.preston.store.ErrorHandlerFactory {

        @Override
        public ErrorHandler createErrorHandler() {
            return ErrorHandlerFactory.errorHandlerStd;
        }
    }
}
