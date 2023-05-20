package bio.guoda.preston.cmd;

import bio.guoda.preston.IRIFixingProcessor;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.process.EmittingStreamOfAnyQuad;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.ProvenanceTracer;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.util.function.Predicate;

public final class AliasUtil {

    public static void findSelectedAlias(StatementsListener listener, Predicate<Quad> selector, Persisting persisting) {
        ReplayUtil.replay(
                new SelectiveListener(selector, listener),
                persisting,
                EmittingStreamOfAnyQuad::new
        );
    }

    public static void findSelectedAlias(StatementsListener listener, Predicate<Quad> selector, Persisting persisting, ProvenanceTracer tracer) {
        ReplayUtil.replay(
                new SelectiveListener(selector, listener),
                persisting,
                tracer,
                EmittingStreamOfAnyQuad::new
        );
    }

    public static Predicate<Quad> aliasSelectorFor(IRI alias) {
        Predicate<Quad> versionSelector = quad -> RefNodeConstants.HAS_VERSION.equals(quad.getPredicate());

        if (alias != null) {
            final IRI fixedIRI = new IRIFixingProcessor()
                    .process(alias);

            final Predicate<Quad> subjectMatches
                    = quad -> alias.equals(quad.getSubject()) || fixedIRI.equals(quad.getSubject());

            final Predicate<Quad> objectMatches
                    = quad -> alias.equals(quad.getObject()) || fixedIRI.equals(quad.getObject());

            versionSelector = versionSelector
                    .and(subjectMatches.or(objectMatches));
        }
        return versionSelector;
    }
}
