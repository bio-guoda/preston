package bio.guoda.preston.index;

import org.apache.lucene.search.similarities.BasicStats;
import org.apache.lucene.search.similarities.SimilarityBase;

public class TLSHSimilarity extends SimilarityBase {
    @Override
    protected double score(BasicStats basicStats, double v, double v1) {
        return 1;
    }

    @Override
    public String toString() {
        return null;
    }
}
