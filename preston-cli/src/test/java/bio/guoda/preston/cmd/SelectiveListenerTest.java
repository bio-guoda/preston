package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.StatementsListenerAdapter;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;

public class SelectiveListenerTest {

    @Test
    public void shouldNotSelect() {
        Predicate<Quad> quadPredicate = quad -> false;
        List<Quad> quads = new ArrayList<>();
        onQuad(quadPredicate, quads);

        assertThat(quads, Is.is(empty()));
    }

    @Test
    public void shouldSelect() {
        Predicate<Quad> quadPredicate = quad -> true;
        List<Quad> quads = new ArrayList<>();
        onQuad(quadPredicate, quads);

        assertThat(quads, Is.is(CoreMatchers.not(empty())));
    }

    private void onQuad(Predicate<Quad> quadPredicate, List<Quad> quads) {
        IRI iri = RefNodeFactory.toIRI("https://example.org");
        new SelectiveListener(quadPredicate, new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                quads.add(statement);
            }
        }).on(RefNodeFactory.toStatement(iri, iri, iri));
    }

}