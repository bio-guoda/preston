package bio.guoda.preston;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.junit.Test;

import static bio.guoda.preston.QuadMatcher.hasQuad;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class QuadMatcherTest {

    @Test
    public void matcherQuadToTriple() {
        IRI subj = toIRI("subj");
        IRI verb = toIRI("verb");
        IRI obj = toIRI("obj");
        Quad quad = new SimpleRDF().createQuad(null, subj, verb, obj);
        Quad otherQuad = new SimpleRDF().createQuad(null, subj, verb, obj);
        assertThat(quad, hasQuad(otherQuad));
    }

    @Test
    public void mismatchQuadToTriple() {
        IRI subj = toIRI("subj");
        IRI verb = toIRI("verb");
        IRI obj = toIRI("obj");
        Quad quad = new SimpleRDF().createQuad(null, subj, verb, obj);
        Quad otherQuad = new SimpleRDF().createQuad(null, toIRI("otherSubj"), verb, obj);
        assertThat(quad, not(hasQuad(otherQuad)));
    }

    @Test
    public void graphLabelMismatch() {
        IRI subj = toIRI("subj");
        IRI verb = toIRI("verb");
        IRI obj = toIRI("obj");
        Quad quad = new SimpleRDF().createQuad(toIRI("label"), subj, verb, obj);
        Quad otherQuad = new SimpleRDF().createQuad(null, subj, verb, obj);
        assertThat(quad, not(hasQuad(otherQuad)));
    }

    @Test
    public void graphLabelMismatch2() {
        IRI subj = toIRI("subj");
        IRI verb = toIRI("verb");
        IRI obj = toIRI("obj");
        Quad quad = new SimpleRDF().createQuad(toIRI("label"), subj, verb, obj);
        Quad otherQuad = new SimpleRDF().createQuad(toIRI("otherLabel"), subj, verb, obj);
        assertThat(quad, not(hasQuad(otherQuad)));
    }

    @Test
    public void graphLabelMatch() {
        IRI subj = toIRI("subj");
        IRI verb = toIRI("verb");
        IRI obj = toIRI("obj");
        Quad quad = new SimpleRDF().createQuad(toIRI("label"), subj, verb, obj);
        Quad otherQuad = new SimpleRDF().createQuad(toIRI("label"), subj, verb, obj);
        assertThat(quad, hasQuad(otherQuad));
    }

}