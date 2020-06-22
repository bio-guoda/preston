package bio.guoda.preston.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.QueryBuilder;

import java.io.IOException;


public class SearchIndexReadOnly implements SearchIndexReadable {

    private DirectoryReader indexReader;
    private IndexSearcher indexSearcher;
    private QueryBuilder queryBuilder;

    public SearchIndexReadOnly(Directory directory, Analyzer analyzer) throws IOException {
        indexReader = DirectoryReader.open(directory);
        indexSearcher = new IndexSearcher(indexReader);
        queryBuilder = new QueryBuilder(analyzer);
    }

    @Override
    public Document get(int documentId) throws IOException {
        return indexReader.document(documentId);
    }

    @Override
    public TopFieldDocs find(String key, String value, int maxHits) throws IOException {
        Query query = queryBuilder.createPhraseQuery(key, value);
        return indexSearcher.search(query, maxHits, Sort.RELEVANCE);
    }

    @Override
    public void close() throws IOException {
        indexReader.close();
    }
}
