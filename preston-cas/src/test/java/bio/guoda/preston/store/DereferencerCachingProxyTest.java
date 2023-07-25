package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;

public class DereferencerCachingProxyTest {


    @Test
    public void ditchLeastAccessed() throws IOException {
        List<IRI> locations = new ArrayList<>();
        Dereferencer<IRI> deref = new DereferencerCachingProxy(
                new Dereferencer<IRI>() {
                    @Override
                    public IRI get(IRI uri) throws IOException {
                        locations.add(uri);
                        return RefNodeFactory.toIRI("foo:bar");
                    }
                },
                2
        );

        deref.get(RefNodeFactory.toIRI("https://example.org/1"));
        deref.get(RefNodeFactory.toIRI("https://example.org/2"));
        deref.get(RefNodeFactory.toIRI("https://example.org/3"));
        deref.get(RefNodeFactory.toIRI("https://example.org/2"));
        deref.get(RefNodeFactory.toIRI("https://example.org/2"));
        deref.get(RefNodeFactory.toIRI("https://example.org/2"));
        deref.get(RefNodeFactory.toIRI("https://example.org/1"));

        assertThat(locations
                        .stream()
                        .filter(iri -> StringUtils.equals(iri.getIRIString(), "https://example.org/1"))
                        .count(),
                Is.is(2L));
        assertThat(locations
                        .stream()
                        .filter(iri -> StringUtils.equals(iri.getIRIString(), "https://example.org/2"))
                        .count(),
                Is.is(1L));
        assertThat(locations
                        .stream()
                        .filter(iri -> StringUtils.equals(iri.getIRIString(), "https://example.org/3"))
                        .count(),
                Is.is(1L));
    }

}