package bio.guoda.preston.process;

import bio.guoda.preston.store.TestUtil;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.ACTIVITY;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.RefNodeConstants.WAS_INFORMED_BY;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ActivityUtilTest {

    @Test
    public void emitUninformedActivity() {
        List<Quad> nodes = new ArrayList<>();
        Quad first = toStatement(toIRI("cats"), toIRI("are"), toIRI("small"));
        Stream<Quad> unlabeledStatements = Stream.of(first);

        final BlankNodeOrIRI newActivity = ActivityUtil.emitAsNewActivity(unlabeledStatements, TestUtil.testEmitter(nodes), Optional.empty());

        assertTrue(isUUID(newActivity));

        assertThat(nodes.size(), is(2));
        assertTrue(nodes.contains(toStatement(newActivity, newActivity, IS_A, ACTIVITY)));
        assertTrue(nodes.contains(toStatement(newActivity, first.getSubject(), first.getPredicate(), first.getObject())));
    }

    @Test
    public void emitInformedActivity() {
        List<Quad> nodes = new ArrayList<>();
        final BlankNodeOrIRI sourceActivity = toIRI("someActivity");
        Quad first = toStatement(toIRI("cats"), toIRI("are"), toIRI("small"));
        Stream<Quad> unlabeledStatements = Stream.of(first);

        final BlankNodeOrIRI newActivity = ActivityUtil.emitAsNewActivity(unlabeledStatements, TestUtil.testEmitter(nodes), Optional.of(sourceActivity));

        assertTrue(isUUID(newActivity));

        assertThat(nodes.size(), is(3));
        assertTrue(nodes.contains(toStatement(newActivity, newActivity, IS_A, ACTIVITY)));
        assertTrue(nodes.contains(toStatement(newActivity, newActivity, WAS_INFORMED_BY, sourceActivity)));
        assertTrue(nodes.contains(toStatement(newActivity, first.getSubject(), first.getPredicate(), first.getObject())));
    }

    private boolean isUUID(BlankNodeOrIRI uuid) {
        String uuidString = uuid.toString();
        return uuidString.matches("^<urn:uuid:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}>$");
    }
}