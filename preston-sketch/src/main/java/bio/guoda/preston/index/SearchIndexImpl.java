package bio.guoda.preston.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;

import java.io.IOException;

public class SearchIndexImpl implements SearchIndex {

    private IndexWriter indexWriter;
    private IndexWriterConfig indexWriterConfig;

    private SearchIndexReadOnlyImpl indexReadOnly;

    public SearchIndexImpl(Directory directory, Analyzer analyzer, Similarity similarity) throws IOException {
        indexWriterConfig = new IndexWriterConfig(analyzer);
        indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        indexWriterConfig.setSimilarity(similarity);
        indexWriter = new IndexWriter(directory, indexWriterConfig);

        indexWriter.commit(); // Must save the index to disk before creating a reader

        indexReadOnly = new SearchIndexReadOnlyImpl(directory, analyzer, similarity);
    }

    public SearchIndexImpl(Directory directory, Analyzer analyzer) throws IOException {
        this(directory, analyzer, IndexSearcher.getDefaultSimilarity());
    }

    @Override
    public void put(Document doc) throws IOException {
        indexWriter.addDocument(doc);
    }

    @Override
    public void close() throws IOException {
        indexWriter.close();
        indexReadOnly.close();
    }

    @Override
    public Document get(int documentId) throws IOException {
        indexReadOnly.refresh(indexWriter);
        return indexReadOnly.get(documentId);
    }

    @Override
    public TopFieldDocs find(String key, String value, int maxHits) throws IOException {
        indexReadOnly.refresh(indexWriter);
        return indexReadOnly.find(key, value, maxHits);
    }
}
