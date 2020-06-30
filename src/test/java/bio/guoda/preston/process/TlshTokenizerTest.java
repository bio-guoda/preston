package bio.guoda.preston.process;

import bio.guoda.preston.index.SearchIndexReadOnly;
import bio.guoda.preston.index.SearchIndexReadable;
import bio.guoda.preston.index.SearchIndexWritable;
import bio.guoda.preston.index.SearchIndexWriteOnly;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.QueryBuilder;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

public class TlshTokenizerTest {

    @Test
    public void tokenize() {

        Analyzer analyzer = getAnalyzer();

        QueryBuilder queryBuilder = new QueryBuilder(analyzer);
        Query query = queryBuilder.createBooleanQuery("", "deadbeef");

        assertThat(query.toString(), is("3 5 11 14 18 22 27 29 34 39 43 46 51 54 59 63"));
    }

    @Test
    public void putAndGet() throws IOException {

        Directory indexStore = SearchIndexTest.createIndexStore();
        Analyzer analyzer = getAnalyzer();

        String key = "tlsh";
        String tlsh = "2C51EFFADBC0960C19EA4691B371F920971291F353D045D0F856CAEBBF14C26F9979E0";
        Document doc = new Document();
        doc.add(new TextField(key, tlsh, Field.Store.YES));

        SearchIndexWritable writeOnlyIndex = new SearchIndexWriteOnly(indexStore, analyzer);
        writeOnlyIndex.put(doc);
        writeOnlyIndex.close();

        SearchIndexReadable readOnlyIndex = new SearchIndexReadOnlyFakeWords(indexStore, analyzer);
        TopFieldDocs hits = readOnlyIndex.find(key, tlsh, 2);

        assertThat(hits.totalHits.value, is(1L));

        Document firstHit = readOnlyIndex.get(hits.scoreDocs[0].doc);
        assertThat(firstHit.get(key), equalTo(tlsh));
    }

    @Test
    public void findSimilar() throws IOException {

        Directory indexStore = SearchIndexTest.createIndexStore();
        Analyzer analyzer = getAnalyzer();

        String key = "tlsh";
        String tlshQuery = "2C51EFFADBC0960C19EA4691B371F920971291F353D045D0F856CAEBBF14C26F9979E0";

        String tlshSimilar = "8451EFFADBC0960C19EA4681B371F920971291F353D045D0F856CAEBBF54C26F9A79E0";
        Document docSimilar = new Document();
        docSimilar.add(new TextField(key, tlshSimilar, Field.Store.YES));

        String tlshNotSimilar = "2A51FEE5E7628A1879C7A78472B0F8340A2291F717D041E4FC6BCBD67E18960FA279D1";
        Document docNotSimilar = new Document();
        docNotSimilar.add(new TextField(key, tlshNotSimilar, Field.Store.YES));

        SearchIndexWritable writeOnlyIndex = new SearchIndexWriteOnly(indexStore, analyzer);
        writeOnlyIndex.put(docNotSimilar);
        writeOnlyIndex.put(docSimilar);
        writeOnlyIndex.close();

        SearchIndexReadable readOnlyIndex = new SearchIndexReadOnlyFakeWords(indexStore, analyzer);
        TopFieldDocs hits = readOnlyIndex.find(key, tlshQuery, 3);

        assertThat(hits.totalHits.value, is(2L));

        ScoreDoc firstHit = hits.scoreDocs[0];
        ScoreDoc secondHit = hits.scoreDocs[1];

        assertThat(readOnlyIndex.get(firstHit.doc).get(key), equalTo(docSimilar.get(key)));
        assertThat(readOnlyIndex.get(secondHit.doc).get(key), equalTo(docNotSimilar.get(key)));
        assertThat(firstHit.score, greaterThan(secondHit.score));
    }

    private Analyzer getAnalyzer() {
        return new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer source = new TlshTokenizer();
                return new TokenStreamComponents(source);
            }
        };
    }

    private static class TlshTokenizer extends Tokenizer {
        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
        private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

        private static final int NUM_BINS_PER_CHAR = 2;

        private byte binPair; // Two 2-bit bins are packed into each TLSH string character
        private int binNumber;
        private int offset;

        @Override
        public boolean incrementToken() throws IOException {
            clearAttributes();
            String term;

            if (binNumber % NUM_BINS_PER_CHAR == 0) {
                final int readValue = input.read();
                if (readValue == -1) {
                    return false;
                }
                offset += 1;

                binPair = (byte)Character.digit((char)readValue, 16);
                term = GetTermForBin(GetFirstBinValue(binPair), binNumber);
            } else {
                term = GetTermForBin(GetSecondBinValue(binPair), binNumber);
            }

            char[] termBuffer = term.toCharArray();
            termAtt.copyBuffer(termBuffer, 0, termBuffer.length);

            offsetAtt.setOffset(correctOffset(offset - 1), correctOffset(offset));

            binNumber += 1;

            return true;
        }

        private static int GetFirstBinValue(int pair) {
            return (pair & 0b1100) >> 2;
        }

        private static int GetSecondBinValue(int pair) {
            return pair & 0b0011;
        }

        private static String GetTermForBin(int binValue, int binNumber) {
            int termValue = (binNumber << 2) | binValue;
            return Integer.toString(termValue);
        }

        public void reset() throws IOException {
            super.reset();
            binNumber = 0;
            offset = 0;
        }

        public final void end() throws IOException {
            super.end();
            int finalOffset = correctOffset(offset);
            this.offsetAtt.setOffset(finalOffset, finalOffset);
        }
    }

    private static class SearchIndexReadOnlyFakeWords extends SearchIndexReadOnly {

        public SearchIndexReadOnlyFakeWords(Directory directory, Analyzer analyzer) throws IOException {
            super(directory, analyzer);
            // From https://lucene.apache.org/core/8_2_0/core/org/apache/lucene/search/similarities/package-summary.html
            // "k1 ... A value of 0 makes term frequency completely ignored"
            // "b ... A value of 0 disables length normalization completely"
            indexSearcher.setSimilarity(new BM25Similarity(0, 0));
        }

    }

}