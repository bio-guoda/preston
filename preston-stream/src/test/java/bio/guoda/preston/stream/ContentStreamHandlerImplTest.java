package bio.guoda.preston.stream;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.stream.ContentStreamHandlerImpl;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.net.URISyntaxException;

import static org.hamcrest.MatcherAssert.assertThat;

public class ContentStreamHandlerImplTest {

    @Test
    public void wrapIRI() throws URISyntaxException {
        IRI iri = ContentStreamHandlerImpl.wrapIRI("zip", RefNodeFactory.toIRI("foo:bar"), "foo.txt");
        assertThat(iri.getIRIString(), Is.is("zip:foo:bar!/foo.txt"));
    }

    @Test
    public void wrapIRINoSuffix() throws URISyntaxException {
        IRI iri = ContentStreamHandlerImpl.wrapIRI("zip", RefNodeFactory.toIRI("foo:bar"));
        assertThat(iri.getIRIString(), Is.is("zip:foo:bar"));
    }

}