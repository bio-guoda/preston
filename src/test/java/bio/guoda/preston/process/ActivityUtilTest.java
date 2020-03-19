package bio.guoda.preston.process;

import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.ACTIVITY;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

public class ActivityUtilTest {

    @Test
    public void createActivity() {
        List<Quad> listQuads = new ArrayList<>();
        Optional<BlankNodeOrIRI> sourceActivity = Optional.of(toIRI("activityIRI"));
        BlankNodeOrIRI newActivity = ActivityUtil.beginInformedActivity(new StatementEmitter() {
            @Override
            public void emit(Quad statement) {
                listQuads.add(statement);
            }
        }, sourceActivity);

        assertThat(newActivity, is(toIRI("activityIRI")));
        assertThat(listQuads.size(), greaterThan(0));
        assertThat(listQuads.get(0), is(toStatement(toIRI("activityIRI"), IS_A, ACTIVITY)));
    }

    @Test
    public void emitActivityStatement() {
        List<Quad> listQuads = new ArrayList<>();
        BlankNodeOrIRI activity = toIRI("activityIRI");
        Quad unlabeledStatement = toStatement(toIRI("cats"), toIRI("are"), toIRI("small"));
        ActivityUtil.emitWithActivityName(Stream.of(unlabeledStatement), new StatementEmitter() {
            @Override
            public void emit(Quad statement) {
                listQuads.add(statement);
            }
        }, activity);

        assertThat(listQuads.size(), is(1));
        assertThat(listQuads.get(0), is(toStatement(activity, unlabeledStatement.getSubject(), unlabeledStatement.getPredicate(), unlabeledStatement.getObject())));
    }

    @Test
    public void startActivity() {

    }

    @Test
    public void endActivity() {

    }
}