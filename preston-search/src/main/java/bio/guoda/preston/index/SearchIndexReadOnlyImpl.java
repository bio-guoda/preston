package bio.guoda.preston.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.QueryBuilder;

import java.io.IOException;


public class SearchIndexReadOnlyImpl implements SearchIndexReadOnly {

    protected DirectoryReader indexReader;
    protected IndexSearcher indexSearcher;
    private QueryBuilder queryBuilder;
    private Similarity similarity;

    public SearchIndexReadOnlyImpl(Directory directory, Analyzer analyzer, Similarity similarity) throws IOException {
        indexReader = DirectoryReader.open(directory);
        queryBuilder = new QueryBuilder(analyzer);

        this.similarity = similarity;
        refresh(null);
    }

    public SearchIndexReadOnlyImpl(Directory directory, Analyzer analyzer) throws IOException {
        this(directory, analyzer, IndexSearcher.getDefaultSimilarity());
    }

    @Override
    public final Document get(int documentId) throws IOException {
        return indexReader.document(documentId);
    }

    @Override
    public final TopFieldDocs find(String key, String value, int maxHits) throws IOException {
        Query query = queryBuilder.createBooleanQuery(key, value);
        return indexSearcher.search(query, maxHits, Sort.RELEVANCE, true);
    }

    @Override
    public final void close() throws IOException {
        indexReader.close();
    }

    public void refresh(IndexWriter indexWriter) throws IOException {
        DirectoryReader newReader = (indexWriter == null)
                ? DirectoryReader.openIfChanged(indexReader)
                : DirectoryReader.openIfChanged(indexReader, indexWriter);

        if (newReader != null && newReader != indexReader) {
            indexReader.close();
            indexReader = newReader;
            indexSearcher = new IndexSearcher(indexReader);
            indexSearcher.setSimilarity(similarity);
        }
    }

    public void refresh() throws IOException {
        refresh(null);
    }
}
