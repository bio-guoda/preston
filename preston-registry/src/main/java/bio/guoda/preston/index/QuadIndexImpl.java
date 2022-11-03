package bio.guoda.preston.index;

import bio.guoda.preston.RDFUtil;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

public class QuadIndexImpl implements QuadIndex, Closeable {

    private static final String SUBJECT = "subject";
    private static final String PREDICATE = "predicate";
    private static final String OBJECT = "object";
    private static final String GRAPH = "graph";
    private static final String ORIGIN = "origin";

    private final SearchIndex index;

    public QuadIndexImpl(File indexDir, Analyzer analyzer, Similarity similarity) throws IOException {
        Directory indexStore = FSDirectory.open(indexDir.toPath());
        index = new SearchIndexImpl(indexStore, analyzer, similarity);
    }

    public QuadIndexImpl(File indexDir) throws IOException {
        this(indexDir, new WhitespaceAnalyzer(), IndexSearcher.getDefaultSimilarity());
    }

    @Override
    public Stream<Quad> findQuadsWithSubject(BlankNodeOrIRI subject, int maxHits) throws IOException {
        return findMatches(SUBJECT, subject, maxHits)
                .map(this::recoverQuadFromDocument);
    }

    @Override
    public Stream<Quad> findQuadsWithPredicate(IRI predicate, int maxHits) throws IOException {
        return findMatches(PREDICATE, predicate, maxHits)
                .map(this::recoverQuadFromDocument);
    }

    @Override
    public Stream<Quad> findQuadsWithObject(RDFTerm object, int maxHits) throws IOException {
        return findMatches(OBJECT, object, maxHits)
                .map(this::recoverQuadFromDocument);
    }

    @Override
    public Stream<Quad> findQuadsWithGraphName(BlankNodeOrIRI graphName, int maxHits) throws IOException {
        return findMatches(GRAPH, graphName, maxHits)
                .map(this::recoverQuadFromDocument);
    }

    private Stream<Document> findMatches(String key, RDFTerm value, int maxHits) throws IOException {
        TopFieldDocs hits = index.find(key, value.ntriplesString(), maxHits);
        return Arrays.stream(hits.scoreDocs).map(scoreDoc -> {
            try {
                return index.get(scoreDoc.doc);
            } catch (IOException e) {
                throw new RuntimeException("Failed to retrieve document from search index", e);
            }
        });
    }

    private Quad recoverQuadFromDocument(Document document) {
        return RDFUtil.asQuad(String.format("%s %s %s %s .",
                document.get(SUBJECT),
                document.get(PREDICATE),
                document.get(OBJECT),
                Optional.ofNullable(document.get(GRAPH)).orElse("")
        ));
    }

    @Override
    public void put(Quad quad, IRI origin) {
        try {
            Document doc = new Document();
            doc.add(new TextField(ORIGIN, origin.ntriplesString(), Field.Store.YES));
            doc.add(new TextField(SUBJECT, quad.getSubject().ntriplesString(), Field.Store.YES));
            doc.add(new TextField(PREDICATE, quad.getPredicate().ntriplesString(), Field.Store.YES));
            doc.add(new TextField(OBJECT, quad.getObject().ntriplesString(), Field.Store.YES));
            quad.getGraphName().ifPresent(
                    graphName -> doc.add(new TextField(GRAPH, graphName.ntriplesString(), Field.Store.YES)));

            index.put(doc);

        } catch (IOException e) {
            throw new RuntimeException("Failed write to search index", e);
        }
    }

    @Override
    public void close() throws IOException {
        index.close();
    }
}
