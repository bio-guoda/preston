package bio.guoda.preston.zenodo;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import com.google.common.collect.Range;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class EmitAfterZenodoRecordUpdateTest {

    @Test
    public void shouldEmitOnRefreshStatement() {
        EmitAfterZenodoRecordUpdate emitAfterZenodoRecordUpdate = new EmitAfterZenodoRecordUpdate();

        IRI graphName = RefNodeFactory.toIRI("foo:bar");
        assertTrue(emitAfterZenodoRecordUpdate.shouldEmit(Arrays.asList(RefNodeFactory.toStatement(graphName, RefNodeConstants.LAST_REFRESHED_ON, graphName))));
    }

    @Test
    public void shouldEmitOnSize() {
        int numberOfMessages = 300;
        EmitAfterZenodoRecordUpdate emitAfterZenodoRecordUpdate = new EmitAfterZenodoRecordUpdate();

        List<Quad> nodes = addMessages(numberOfMessages);
        assertTrue(emitAfterZenodoRecordUpdate.shouldEmit(nodes));
    }

    @Test
    public void shouldNotEmitOnSize() {
        EmitAfterZenodoRecordUpdate emitAfterZenodoRecordUpdate = new EmitAfterZenodoRecordUpdate();

        List<Quad> nodes = addMessages(100);
        assertTrue(emitAfterZenodoRecordUpdate.shouldEmit(nodes));
    }

    private List<Quad> addMessages(int numberOfMessages) {
        IRI graphName = RefNodeFactory.toIRI("foo:bar");
        List<Quad> nodes = new ArrayList<Quad>();
        for (int i = 0; i < numberOfMessages; i++) {
            Quad quad = RefNodeFactory.toStatement(graphName, RefNodeConstants.SEE_ALSO, graphName);
            nodes.add(quad);
        }
        return nodes;
    }



}