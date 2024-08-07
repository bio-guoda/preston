package bio.guoda.preston;

import bio.guoda.preston.process.StopProcessingException;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.helpers.NTriplesParserSettings;
import org.eclipse.rdf4j.rio.helpers.ParseErrorLogger;
import org.eclipse.rdf4j.rio.nquads.NQuadsParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class RDFUtil {

    private static final Logger LOG = LoggerFactory.getLogger(RDFUtil.class);

    public static String getValueFor(RDFTerm entity) {
        return RDFValueUtil.getValueFor(entity);
    }

    public static void parseQuads(InputStream inputStream, AbstractRDFHandler handler) {
        parseQuads(inputStream, handler, throwable -> {
            // blissfully optimistic parsing by default by ignoring StopProcessing Exception
            if (!(throwable instanceof StopProcessingException)) {
                LOG.warn("unexpected issue on processing rdf", throwable);
            }
        });
    }

    public static void parseQuads(InputStream inputStream,
                                  AbstractRDFHandler handler,
                                  ExceptionConsumer exceptionConsumer) {
        RDFParser rdfParser = new NQuadsParserFactory().getParser();
        rdfParser.setRDFHandler(handler);
        rdfParser.setParseErrorListener(new ParseErrorLogger());
        try {
            // try to be as lenient as possible
            rdfParser.getParserConfig()
                    .addNonFatalError(NTriplesParserSettings.FAIL_ON_INVALID_LINES)
                    .addNonFatalError(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES)
                    .addNonFatalError(BasicParserSettings.FAIL_ON_UNKNOWN_LANGUAGES)
                    .set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false)
                    .set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false)
                    .set(BasicParserSettings.VERIFY_URI_SYNTAX, false)
                    .set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);

            rdfParser.parse(inputStream);
        } catch (IOException | RDFParseException | RDFHandlerException | StopProcessingException e) {
            exceptionConsumer.accept(e);
        }
    }

    public static Quad asQuad(Statement statement) {
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

    private static BlankNodeOrIRI parseSubj(org.eclipse.rdf4j.model.Resource node1) {
        BlankNodeOrIRI subj;
        if (node1.isBNode()) {
            subj = RefNodeFactory.toBlank(node1.stringValue());
        } else {
            subj = RefNodeFactory.toIRI(node1.stringValue());
        }
        return subj;
    }

    private static RDFTerm parseObj(Value objNode1) {
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

    public static List<Quad> parseQuads(InputStream inputStream) {
        List<Quad> quads = new ArrayList<>();

        parseQuads(inputStream, new AbstractRDFHandler() {
            @Override
            public void handleStatement(Statement st) throws RDFHandlerException {
                quads.add(asQuad(st));
            }
        });
        return quads;
    }
}
