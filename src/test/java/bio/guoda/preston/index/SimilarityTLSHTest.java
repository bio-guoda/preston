package bio.guoda.preston.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopFieldDocs;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

public class SimilarityTLSHTest {

    @Test
    public void putAndGet() throws IOException {

        String key = "tlsh";
        String tlsh = "2C51EFFADBC0960C19EA4691B371F920971291F353D045D0F856CAEBBF14C26F9979E0";
        Document doc = new Document();
        doc.add(new TextField(key, tlsh, Field.Store.YES));

        SearchIndex index = new SearchIndexImpl(SearchIndexTest.createIndexStore(), TokenizerTLSHTest.getAnalyzer());
        index.put(doc);
        index.close();

        TopFieldDocs hits = index.find(key, tlsh, 2);

        assertThat(hits.totalHits.value, is(1L));

        Document firstHit = index.get(hits.scoreDocs[0].doc);
        assertThat(firstHit.get(key), equalTo(tlsh));
    }

    @Test
    public void findSimilar() throws IOException {
        String key = "tlsh";
        String tlshQuery = "2C51EFFADBC0960C19EA4691B371F920971291F353D045D0F856CAEBBF14C26F9979E0";

        String tlshSimilar = "8451EFFADBC0960C19EA4681B371F920971291F353D045D0F856CAEBBF54C26F9A79E0";
        Document docSimilar = new Document();
        docSimilar.add(new TextField(key, tlshSimilar, Field.Store.YES));

        String tlshNotSimilar = "2A51FEE5E7628A1879C7A78472B0F8340A2291F717D041E4FC6BCBD67E18960FA279D1";
        Document docNotSimilar = new Document();
        docNotSimilar.add(new TextField(key, tlshNotSimilar, Field.Store.YES));

        SearchIndex index = new SearchIndexImpl(SearchIndexTest.createIndexStore(), TokenizerTLSHTest.getAnalyzer());
        index.put(docNotSimilar);
        index.put(docSimilar);

        TopFieldDocs hits = index.find(key, tlshQuery, 3);

        assertThat(hits.totalHits.value, is(2L));

        ScoreDoc firstHit = hits.scoreDocs[0];
        ScoreDoc secondHit = hits.scoreDocs[1];

        assertThat(index.get(firstHit.doc).get(key), equalTo(docSimilar.get(key)));
        assertThat(index.get(secondHit.doc).get(key), equalTo(docNotSimilar.get(key)));
        assertThat(firstHit.score, greaterThan(secondHit.score));
    }
}