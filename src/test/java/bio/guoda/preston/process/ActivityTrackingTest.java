package bio.guoda.preston.process;

import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static bio.guoda.preston.RefNodeConstants.ACTIVITY;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ActivityTrackingTest {

    @Test
    public void createActivity() {

        List<Quad> listQuads = new ArrayList<>();
        Optional<BlankNodeOrIRI> sourceActivity = Optional.of(toIRI("blahblah"));
        BlankNodeOrIRI blankNodeOrIRI = ActivityTracking.beginInformedActivity(new StatementEmitter() {
            @Override
            public void emit(Quad statement) {
                listQuads.add(statement);
            }
        }, sourceActivity);
        assertThat(blankNodeOrIRI, is(toIRI("blahblah")));
        assertThat(listQuads.size(), Matchers.greaterThan(0));
        assertThat(listQuads.get(0), is(toStatement(toIRI("blahblah"), IS_A, ACTIVITY)));
    }

    @Test
    public void startActivity()
}