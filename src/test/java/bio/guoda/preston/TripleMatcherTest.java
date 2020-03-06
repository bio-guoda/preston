package bio.guoda.preston;

import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.TripleLike;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.junit.Test;

import static bio.guoda.preston.TripleMatcher.hasTriple;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;

public class TripleMatcherTest {

    @Test
    public void matcherQuadToTriple() {
        IRI subj = RefNodeFactory.toIRI("subj");
        IRI verb = RefNodeFactory.toIRI("verb");
        IRI obj = RefNodeFactory.toIRI("obj");
        TripleLike quad = new SimpleRDF().createQuad(null, subj, verb, obj);
        TripleLike triple = new SimpleRDF().createTriple(subj, verb, obj);
        assertThat(quad, hasTriple(triple));
    }

    @Test
    public void matcherQuadToQuad() {
        IRI subj = RefNodeFactory.toIRI("subj");
        IRI verb = RefNodeFactory.toIRI("verb");
        IRI obj = RefNodeFactory.toIRI("obj");
        TripleLike quad = new SimpleRDF().createQuad(null, subj, verb, obj);
        TripleLike triple = new SimpleRDF().createTriple(RefNodeFactory.toIRI("otherSubj"), verb, obj);
        assertThat(quad, not(hasTriple(triple)));
    }

}