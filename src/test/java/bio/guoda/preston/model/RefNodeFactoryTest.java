package bio.guoda.preston.model;

import bio.guoda.preston.RefNodeConstants;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.QuadLike;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.util.Optional;

import static bio.guoda.preston.model.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toDateTime;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RefNodeFactoryTest {

    @Test(expected = NullPointerException.class)
    public void createNullTriple() {
        toStatement(null, null, null);
    }

    @Test
    public void hasContent() {
        Quad statement = toStatement(toIRI("http://some"),
                RefNodeConstants.HAS_VERSION,
                toBlank());
        assertFalse(hasVersionAvailable(statement));
    }


    @Test
    public void recordGenerationTime() {
        Literal dateTime = toDateTime("2018-10-25");
        assertNotNull(dateTime);
        assertThat(dateTime.toString(), is("\"2018-10-25\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"));
    }

    @Test
    public void graphLabelForQuad() {
        IRI someLabel = toIRI("someLabel");
        Quad quad = RefNodeFactory.toStatementWithGraphName(someLabel, toIRI("subj"), toIRI("verb"), toIRI("obj"));

        assertNotNull(quad);
        Optional<BlankNodeOrIRI> graphName = quad.getGraphName();
        assertTrue(graphName.isPresent());
        assertThat(graphName.get(), is(toIRI("someLabel")));
    }

    @Test
    public void graphLabelForQuadNoGraphName() {
        Quad quad = RefNodeFactory.toStatement(toIRI("subj"), toIRI("verb"), toIRI("obj"));

        assertNotNull(quad);
        Optional<BlankNodeOrIRI> graphName = quad.getGraphName();
        assertFalse(graphName.isPresent());
    }


}