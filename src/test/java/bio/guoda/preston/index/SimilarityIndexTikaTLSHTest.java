package bio.guoda.preston.index;

import org.apache.commons.collections4.OrderedMap;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.rdf.api.IRI;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.MatcherAssert.assertThat;

public class SimilarityIndexTikaTLSHTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void getSimilarContents() throws IOException {
        SimilarityIndexTikaTLSH index = new SimilarityIndexTikaTLSH(tmpDir.newFolder());

        index.indexHashPair(toIRI("hash://sha256/blah1"), toIRI("hash://tika-tlsh/123"));
        index.indexHashPair(toIRI("hash://sha256/blah2"), toIRI("hash://tika-tlsh/124"));
        index.indexHashPair(toIRI("hash://sha256/blah3"), toIRI("hash://tika-tlsh/789"));

        OrderedMap<IRI, Float> scores = new ListOrderedMap<>();
        index.getSimilarContents(toIRI("hash://tika-tlsh/123"), 3)
                .forEach(hit -> scores.put(hit.getSHA256(), hit.getScore()));

        assertThat(scores.size(), is(2));
        assertThat(scores.get(toIRI("hash://sha256/blah1")), greaterThan(scores.get(toIRI("hash://sha256/blah2"))));
    }
}
