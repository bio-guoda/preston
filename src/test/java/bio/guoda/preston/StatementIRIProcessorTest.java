package bio.guoda.preston;

import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.StatementsListenerAdapter;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class StatementIRIProcessorTest {

    @Test
    public void relativeIRIsNon2005CompliantRDF() {
        final List<Quad> quads = new ArrayList<>();

        new StatementIRIProcessor(new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                quads.add(statement);
            }
        }).on(RefNodeFactory.toStatement(
                RefNodeFactory.toIRI("bdf0617c-b7e7-4f4b-a50c-6537784a9719"),
                RefNodeFactory.toIRI("foo"),
                RefNodeFactory.toIRI("eats"),
                RefNodeFactory.toIRI("bar")
        ));

        assertThat(quads.size(), is(1));
        assertThat(quads.get(0).getSubject().ntriplesString(), is("<urn:example:foo>"));
        assertThat(quads.get(0).getPredicate().ntriplesString(), is("<urn:example:eats>"));
        assertThat(quads.get(0).getObject().ntriplesString(), is("<urn:example:bar>"));
        assertThat(quads.get(0).getGraphName().get().ntriplesString(), is("<urn:uuid:bdf0617c-b7e7-4f4b-a50c-6537784a9719>"));
    }

    @Test
    public void absoluteIRIs() {
        final List<Quad> quads = new ArrayList<>();

        new StatementIRIProcessor(new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                quads.add(statement);
            }
        }).on(RefNodeFactory.toStatement(
                RefNodeFactory.toIRI("x:bdf0617c-b7e7-4f4b-a50c-6537784a9719"),
                RefNodeFactory.toIRI("x:foo"),
                RefNodeFactory.toIRI("x:eats"),
                RefNodeFactory.toIRI("x:bar")
        ));

        assertThat(quads.size(), is(1));
        assertThat(quads.get(0).getSubject().ntriplesString(), is("<x:foo>"));
        assertThat(quads.get(0).getPredicate().ntriplesString(), is("<x:eats>"));
        assertThat(quads.get(0).getObject().ntriplesString(), is("<x:bar>"));
        assertThat(quads.get(0).getGraphName().get().ntriplesString(), is("<x:bdf0617c-b7e7-4f4b-a50c-6537784a9719>"));
    }


}