package bio.guoda.preston.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.TopFieldDocs;

import java.io.Closeable;
import java.io.IOException;

public interface SearchIndexReadOnly extends Closeable {

    Document get(int documentId) throws IOException;

    TopFieldDocs find(String key, String value, int maxHits) throws IOException;

}
