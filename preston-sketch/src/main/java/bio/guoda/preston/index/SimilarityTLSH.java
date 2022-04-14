package bio.guoda.preston.index;

import org.apache.lucene.search.similarities.BM25Similarity;

public class SimilarityTLSH extends BM25Similarity {

    public SimilarityTLSH() {
        // From https://lucene.apache.org/core/8_2_0/core/org/apache/lucene/search/similarities/package-summary.html
        // "k1 ... A value of 0 makes term frequency completely ignored"
        // "b ... A value of 0 disables length normalization completely"
        super(0, 0);
    }
}
