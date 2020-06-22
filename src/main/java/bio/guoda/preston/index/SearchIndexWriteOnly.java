package bio.guoda.preston.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;

import java.io.IOException;

public class SearchIndexWriteOnly implements SearchIndexWritable {

    private IndexWriter indexWriter;
    private IndexWriterConfig indexWriterConfig;

    public SearchIndexWriteOnly(Directory directory, Analyzer analyzer) throws IOException {
        indexWriterConfig = new IndexWriterConfig(analyzer);
        indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        indexWriter = new IndexWriter(directory, indexWriterConfig);
    }

    @Override
    public void put(Document doc) throws IOException {
        indexWriter.addDocument(doc);
    }

    @Override
    public void close() throws IOException {
        indexWriter.commit();
        indexWriter.close();
    }
}
