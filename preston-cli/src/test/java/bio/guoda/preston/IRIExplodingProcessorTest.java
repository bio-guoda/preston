package bio.guoda.preston;

import bio.guoda.preston.process.IRIExplodingProcessor;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;

public class IRIExplodingProcessorTest {

    @Test(expected = IllegalArgumentException.class)
    public void explodeOnRelativeUUID() {
        new IRIExplodingProcessor().process(RefNodeFactory.toIRI(UUID.randomUUID().toString()));
    }

    @Test
    public void doNotExplodeOnURN_UUID() {
        IRI process = new IRIExplodingProcessor().process(RefNodeFactory.toIRI("urn:uuid:e0e7dd66-1a73-4a5e-82e8-89f58bc57233"));
        assertThat(process.getIRIString(), Is.is("urn:uuid:e0e7dd66-1a73-4a5e-82e8-89f58bc57233"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void explodeOnRelativeURI() {
        new IRIExplodingProcessor().process(RefNodeFactory.toIRI("foo"));
    }


    @Test
    public void doNotExplodeOnAbsoluteURI() {
        IRI process = new IRIExplodingProcessor().process(RefNodeFactory.toIRI("foo:bar"));
        assertThat(process.getIRIString(), Is.is("foo:bar"));
    }

}