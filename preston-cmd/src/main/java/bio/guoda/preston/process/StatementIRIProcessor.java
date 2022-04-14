package bio.guoda.preston.process;

import bio.guoda.preston.IRIFixingProcessor;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.IRIProcessor;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.StatementProcessor;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;

import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class StatementIRIProcessor extends StatementProcessor {


    public static final Pattern UUID_PATTERN = Pattern.compile("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$");

    // URI validation regex as specified by RFC 3986: Appendix B.
    // altered to make scheme mandatory
    public static final Pattern VALID_URI_ORIG = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
    public static final Pattern ABSOLUTE_URI_PATTERN = Pattern.compile("^(([^:/?#]+):)(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
    public static final Predicate<String> IS_ABSOLUTE_URI = ABSOLUTE_URI_PATTERN.asPredicate();
    public static final Predicate<String> IS_UUID = UUID_PATTERN.asPredicate();

    public IRIProcessor iriProcessor = new IRIFixingProcessor();

    public void setIriProcessor(IRIProcessor iriProcessor) {
        this.iriProcessor = iriProcessor;
    }


    public StatementIRIProcessor(StatementsListener listeners) {
        super(listeners);
    }

    @Override
    public void on(Quad statement) {
        BlankNodeOrIRI subject = statement.getSubject();
        if (subject instanceof IRI) {
            subject = iriProcessor.process((IRI) subject);
        }
        RDFTerm object = statement.getObject();
        if (object instanceof IRI) {
            object = iriProcessor.process((IRI) object);
        }
        IRI verb = iriProcessor.process(statement.getPredicate());

        Optional<BlankNodeOrIRI> graphName = statement.getGraphName();
        if (graphName.isPresent() && graphName.get() instanceof IRI) {
            graphName = Optional.of(iriProcessor.process((IRI) graphName.get()));
        }

        Quad fixedQuad = RefNodeFactory.toStatement(
                graphName.orElse(null),
                subject,
                verb,
                object);

        emit(fixedQuad);
    }

}
