package bio.guoda.preston;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.TripleLike;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.junit.Test;

import static bio.guoda.preston.TripleMatcher.hasTriple;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class TripleMatcherTest {

    @Test
    public void matcherQuadToTriple() {
        IRI subj = toIRI("subj");
        IRI verb = toIRI("verb");
        IRI obj = toIRI("obj");
        TripleLike quad = new SimpleRDF().createQuad(null, subj, verb, obj);
        TripleLike otherQuad = new SimpleRDF().createTriple(subj, verb, obj);
        assertThat(quad, hasTriple(otherQuad));
    }

    @Test
    public void mismatchQuadToTriple() {
        IRI subj = toIRI("subj");
        IRI verb = toIRI("verb");
        IRI obj = toIRI("obj");
        TripleLike quad = new SimpleRDF().createQuad(null, subj, verb, obj);
        TripleLike otherQuad = new SimpleRDF().createTriple(toIRI("otherSubj"), verb, obj);
        assertThat(quad, not(hasTriple(otherQuad)));
    }

    @Test
    public void matchQuadWithGraphLabelToTriple() {
        IRI subj = toIRI("subj");
        IRI verb = toIRI("verb");
        IRI obj = toIRI("obj");
        TripleLike quad = new SimpleRDF().createQuad(toIRI("label"), subj, verb, obj);
        TripleLike triple = new SimpleRDF().createTriple(subj, verb, obj);
        assertThat(quad, hasTriple(triple));
    }

}