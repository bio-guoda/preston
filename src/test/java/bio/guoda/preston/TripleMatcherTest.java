package bio.guoda.preston;

import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.TripleLike;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.junit.Test;

import static bio.guoda.preston.TripleMatcher.hasTriple;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;

public class TripleMatcherTest {

    @Test
    public void matcherQuadToTriple() {
        IRI subj = toIRI("subj");
        IRI verb = toIRI("verb");
        IRI obj = toIRI("obj");
        TripleLike quad = new SimpleRDF().createQuad(null, subj, verb, obj);
        TripleLike triple = new SimpleRDF().createTriple(subj, verb, obj);
        assertThat(quad, hasTriple(triple));
    }

    @Test
    public void mismatchQuadToTriple() {
        IRI subj = toIRI("subj");
        IRI verb = toIRI("verb");
        IRI obj = toIRI("obj");
        TripleLike quad = new SimpleRDF().createQuad(null, subj, verb, obj);
        TripleLike triple = new SimpleRDF().createTriple(toIRI("otherSubj"), verb, obj);
        assertThat(quad, not(hasTriple(triple)));
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