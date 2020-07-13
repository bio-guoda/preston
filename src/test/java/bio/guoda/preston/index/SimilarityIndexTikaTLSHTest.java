package bio.guoda.preston.index;

import org.apache.commons.collections4.OrderedMap;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

public class SimilarityIndexTikaTLSHTest {

    @Test
    public void getSimilarContents() {
        SimilarityIndexTikaTLSH index = new SimilarityIndexTikaTLSH();

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
