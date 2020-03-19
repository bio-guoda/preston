package bio.guoda.preston.process;

import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Quad;
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
import static org.junit.Assert.assertTrue;

public class ActivityUtilTest {

    @Test
    public void startActivity() {
        List<Quad> listQuads = new ArrayList<>();
        Optional<BlankNodeOrIRI> sourceActivity = Optional.of(toIRI("activityIRI"));

        BlankNodeOrIRI newActivity = ActivityUtil.beginInformedActivity(new StatementEmitter() {
            @Override
            public void emit(Quad statement) {
                listQuads.add(statement);
            }
        }, sourceActivity);

        assertThat(newActivity, is(toIRI("activityIRI")));
//        assertThat(listQuads.size(), greaterThan(0));
//        assertThat(listQuads.get(0), is(toStatement(toIRI("activityIRI"), IS_A, ACTIVITY)));
    }

    @Test
    public void endActivity() {
        List<Quad> listQuads = new ArrayList<>();
        BlankNodeOrIRI activity = toIRI("someActivity");

        ActivityUtil.endInformedActivity(new StatementEmitter() {
            @Override
            public void emit(Quad statement) {
                listQuads.add(statement);
            }
        }, activity);

        assertThat(listQuads.size(), is(0));
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
    public void emitActivity() {
        List<Quad> listQuads = new ArrayList<>();
        BlankNodeOrIRI parentActivity = toIRI("someActivity");
        Quad first = toStatement(toIRI("cats"), toIRI("are"), toIRI("small"));
        Stream<Quad> unlabeledStatements = Stream.of(first);

        BlankNodeOrIRI newActivity = ActivityUtil.emitAsNewActivity(unlabeledStatements, new StatementEmitter() {
            @Override
            public void emit(Quad statement) {
                listQuads.add(statement);
            }
        }, Optional.of(parentActivity));

        assertThat(listQuads.size(), greaterThan(0));
        assertTrue(listQuads.contains(toStatement(newActivity, first.getSubject(), first.getPredicate(), first.getObject())));
    }
}