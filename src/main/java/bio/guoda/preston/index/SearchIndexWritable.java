package bio.guoda.preston.index;

import org.apache.lucene.document.Document;

import java.io.Closeable;
import java.io.IOException;

public interface SearchIndexWritable extends Closeable {

    void put(Document doc) throws IOException;

}
