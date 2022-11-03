package bio.guoda.preston.index;

import bio.guoda.preston.store.TestUtil;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SearchIndexTest {

    @Test
    public void putAndGet() throws IOException {
        StandardAnalyzer analyzer = new StandardAnalyzer();
        Directory indexStore = TestUtil.getTestIndexStore();

        String key = "mango";
        String value = "papaya";
        Document doc = new Document();
        doc.add(new TextField(key, value, Field.Store.YES));

        SearchIndex index = new SearchIndexImpl(indexStore, analyzer);
        index.put(doc);

        TopFieldDocs hits = index.find(key, value, 2);
        assertThat(hits.totalHits.value, is(1L));

        Document firstHit = index.get(hits.scoreDocs[0].doc);
        assertThat(firstHit.get(key), is(value));
    }

    @Test
    public void putDuplicateDocuments() throws IOException {
        StandardAnalyzer analyzer = new StandardAnalyzer();
        Directory indexStore = TestUtil.getTestIndexStore();

        String key = "mango";
        String value = "papaya";
        Document doc = new Document();
        doc.add(new TextField(key, value, Field.Store.YES));

        SearchIndex index = new SearchIndexImpl(indexStore, analyzer);
        index.put(doc);
        index.put(doc);

        TopFieldDocs hits = index.find(key, value, 3);
        assertThat(hits.totalHits.value, is(2L));

        Document firstHit = index.get(hits.scoreDocs[0].doc);
        assertThat(firstHit.get(key), is(value));

        Document secondHit = index.get(hits.scoreDocs[0].doc);
        assertThat(secondHit.get(key), is(value));
    }

    @Test
    public void getMissingDocument() throws IOException {
        StandardAnalyzer analyzer = new StandardAnalyzer();
        Directory indexStore = TestUtil.getTestIndexStore();

        String key = "mango";
        String value = "papaya";
        Document doc = new Document();
        doc.add(new TextField(key, value, Field.Store.YES));

        SearchIndex index = new SearchIndexImpl(indexStore, analyzer);
        TopFieldDocs hits = index.find(key, value, 1);

        assertThat(hits.totalHits.value, is(0L));
    }

}