package bio.guoda.preston;

import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.util.Optional;

import static bio.guoda.preston.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toDateTime;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static junit.framework.TestCase.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
        Quad quad = RefNodeFactory.toStatement(someLabel, toIRI("subj"), toIRI("verb"), toIRI("obj"));

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

    @Test
    public void getVersion() {
        Quad quad = RefNodeFactory.toStatement(
                toIRI("subj"),
                RefNodeConstants.HAS_VERSION,
                toIRI("obj")
        );
        BlankNodeOrIRI version = RefNodeFactory.getVersion(quad);
        assertThat(version.ntriplesString(), is("<obj>"));
    }

    @Test
    public void getVersion2() {
        Quad quad = RefNodeFactory.toStatement(
                toIRI("https://doi.org/10.5281/zenodo.1410543"),
                RefNodeConstants.USED_BY,
                toIRI("urn:uuid:2ae013aa-9422-42c7-9afd-845dd0ad6112")
        );
        BlankNodeOrIRI version = RefNodeFactory.getVersion(quad);
        assertNull(version);
    }

    @Test
    public void getVersionSource() {
        Quad quad = RefNodeFactory.toStatement(
                toIRI("subj"),
                RefNodeConstants.HAS_VERSION,
                toIRI("obj")
        );
        BlankNodeOrIRI version = RefNodeFactory.getVersionSource(quad);
        assertThat(version.ntriplesString(), is("<subj>"));
    }




}