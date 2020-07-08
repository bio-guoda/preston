package bio.guoda.preston.index;

import org.apache.lucene.document.Document;

import java.io.IOException;

public interface SearchIndex extends SearchIndexReadOnly {

    void put(Document doc) throws IOException;
}
