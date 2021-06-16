package bio.guoda.preston.index;

import org.apache.commons.rdf.api.IRI;
import org.apache.jena.util.SplitIRI;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

import static bio.guoda.preston.model.RefNodeFactory.toIRI;

public class SimilarityIndexTikaTLSH implements Closeable {

    private static final String FIELD_SHA256 = "sha256";
    private static final String FIELD_TIKA_TLSH = "tika-tlsh";

    private SearchIndex index;

    @Override
    public void close() throws IOException {
        index.close();
    }

    public static class ScoredHit {
        private IRI sha256;
        private float score;

        public ScoredHit(IRI sha256, float score) {
            this.sha256 = sha256;
            this.score = score;
        }

        public IRI getSHA256() { return sha256; }
        public float getScore() { return score; }
    }

    public SimilarityIndexTikaTLSH(File indexDir) {
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer source = new TokenizerTLSH();
                return new TokenStreamComponents(source);
            }
        };

        try {
            Directory indexStore = FSDirectory.open(indexDir.toPath());
            index = new SearchIndexImpl(indexStore, analyzer, new TLSHSimilarity());
        } catch (IOException e) {
            throw new RuntimeException("Failed create an on-disk search index.", e);
        }
    }

    public Stream<ScoredHit> getSimilarContents(IRI tikaTlsh, int maxHits) {
        try {
            TopFieldDocs hits = index.find(FIELD_TIKA_TLSH, GetLocalName(tikaTlsh), maxHits);
            return Arrays.stream(hits.scoreDocs).map(scoreDoc -> {
                    try {
                        Document doc = index.get(scoreDoc.doc);
                        return new ScoredHit(
                                toIRI(doc.get(FIELD_SHA256)),
                                scoreDoc.score
                        );
                    } catch (IOException e) {
                        return null;
                    }
                }
            ).filter(Objects::nonNull);
        } catch (IOException e) {
            throw new RuntimeException("Failed read from search index.", e);
        }
    }

    private String GetLocalName(IRI iri) {
        return SplitIRI.localname(iri.getIRIString());
    }

    public void indexHashPair(IRI shaHash, IRI tlshHash) {
        try {
            Document doc = new Document();
            doc.add(new TextField(FIELD_SHA256, shaHash.getIRIString(), Field.Store.YES));
            doc.add(new TextField(FIELD_TIKA_TLSH, GetLocalName(tlshHash), Field.Store.NO));

            index.put(doc);

        } catch (IOException e) {
            throw new RuntimeException("Failed write to search index.", e);
        }
    }
}
