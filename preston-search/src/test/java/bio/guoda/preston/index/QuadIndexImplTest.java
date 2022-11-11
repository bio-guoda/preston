package bio.guoda.preston.index;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

public class QuadIndexImplTest {

    @SuppressWarnings("resource")
    @Test
    public void retrieveQuad() throws IOException {
        try (QuadIndex index = new QuadIndexImpl(createTempFolder())) {
            Quad quad = RefNodeFactory.toStatement(
                    toIRI("https://example.com"),
                    RefNodeConstants.HAS_VERSION,
                    toIRI("hash://sha256/blah"));

            index.put(quad, toIRI("hash://sha256/bloop"));

            List<Quad> hits;
            hits = index.findQuadsWithSubject(toIRI("https://example.com"), 2).collect(Collectors.toList());
            assertThat(hits.size(), is(1));
            assertThat(hits.get(0), equalTo(quad));

            hits = index.findQuadsWithPredicate(RefNodeConstants.HAS_VERSION, 2).collect(Collectors.toList());
            assertThat(hits.size(), is(1));
            assertThat(hits.get(0), equalTo(quad));

            hits = index.findQuadsWithObject(toIRI("hash://sha256/blah"), 2).collect(Collectors.toList());
            assertThat(hits.size(), is(1));
            assertThat(hits.get(0), equalTo(quad));

            hits = index.findQuadsWithGraphName(toIRI("the:graph"), 2).collect(Collectors.toList());
            assertThat(hits.size(), is(0));
        }
    }

    @SuppressWarnings("resource")
    @Test
    public void noHits() throws IOException {
        QuadIndex index = new QuadIndexImpl(createTempFolder());

        List<Quad> hits = index.findQuadsWithSubject(toIRI("https://example.com"), 2).collect(Collectors.toList());

        assertThat(hits.size(), is(0));
    }
    private static File createTempFolder() throws IOException {
        TemporaryFolder tmp = new TemporaryFolder();
        tmp.create();
        return tmp.getRoot();
    }

}