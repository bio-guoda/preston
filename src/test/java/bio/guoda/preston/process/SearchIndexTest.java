package bio.guoda.preston.process;

import bio.guoda.preston.index.SearchIndexReadOnly;
import bio.guoda.preston.index.SearchIndexReadable;
import bio.guoda.preston.index.SearchIndexWritable;
import bio.guoda.preston.index.SearchIndexWriteOnly;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Paths;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SearchIndexTest {

    @Test
    public void putAndGet() throws IOException {
        StandardAnalyzer analyzer = new StandardAnalyzer();
        Directory indexStore = createIndexStore();

        String key = "mango";
        String value = "papaya";
        Document doc = new Document();
        doc.add(new TextField(key, value, Field.Store.YES));

        SearchIndexWritable writeOnlyIndex = new SearchIndexWriteOnly(indexStore, analyzer);
        writeOnlyIndex.put(doc);
        writeOnlyIndex.close();

        SearchIndexReadable readOnlyIndex = new SearchIndexReadOnly(indexStore, analyzer);
        TopFieldDocs hits = readOnlyIndex.find(key, value, 2);

        assertThat(hits.totalHits.value, is(1L));

        Document firstHit = readOnlyIndex.get(hits.scoreDocs[0].doc);
        assertThat(firstHit.get(key), is(value));
    }

    @Test
    public void putDuplicateDocuments() throws IOException {
        StandardAnalyzer analyzer = new StandardAnalyzer();
        Directory indexStore = createIndexStore();

        String key = "mango";
        String value = "papaya";
        Document doc = new Document();
        doc.add(new TextField(key, value, Field.Store.YES));

        SearchIndexWritable writeOnlyIndex = new SearchIndexWriteOnly(indexStore, analyzer);
        writeOnlyIndex.put(doc);
        writeOnlyIndex.put(doc);
        writeOnlyIndex.close();

        SearchIndexReadable readOnlyIndex = new SearchIndexReadOnly(indexStore, analyzer);
        TopFieldDocs hits = readOnlyIndex.find(key, value, 3);

        assertThat(hits.totalHits.value, is(2L));

        Document firstHit = readOnlyIndex.get(hits.scoreDocs[0].doc);
        assertThat(firstHit.get(key), is(value));

        Document secondHit = readOnlyIndex.get(hits.scoreDocs[0].doc);
        assertThat(secondHit.get(key), is(value));
    }

    @Test
    public void getMissingDocument() throws IOException {
        StandardAnalyzer analyzer = new StandardAnalyzer();
        Directory indexStore = createIndexStore();

        String key = "mango";
        String value = "papaya";
        Document doc = new Document();
        doc.add(new TextField(key, value, Field.Store.YES));

        SearchIndexWritable writeOnlyIndex = new SearchIndexWriteOnly(indexStore, analyzer);
        writeOnlyIndex.close();

        SearchIndexReadable readOnlyIndex = new SearchIndexReadOnly(indexStore, analyzer);
        TopFieldDocs hits = readOnlyIndex.find(key, value, 1);

        assertThat(hits.totalHits.value, is(0L));
    }

    public static Directory createIndexStore() throws IOException {
        TemporaryFolder tmp = new TemporaryFolder();
        tmp.create();
        String tmpPath = tmp.getRoot().getPath();
        return FSDirectory.open(Paths.get(tmpPath));
    }
}