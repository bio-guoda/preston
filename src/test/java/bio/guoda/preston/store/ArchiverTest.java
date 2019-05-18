package bio.guoda.preston.store;

import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.RefNodeConstants.HAS_PREVIOUS_VERSION;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.model.RefNodeFactory.isBlankOrSkolemizedBlank;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toSkolemizedBlank;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

public class ArchiverTest {

    private static final IRI SOME_IRI = toIRI(URI.create("http://some"));

    @Test
    public void putContentThatFailsToDownload() throws IOException {
        BlankNode blank = toBlank();
        Triple statement
                = toStatement(toIRI(URI.create("http://some")),
                HAS_VERSION,
                blank);

        Dereferencer3<IRI> dereferencer = uri -> {
            throw new IOException("fails to dereference");
        };

        KeyValueStore testKeyValueStore = TestUtil.getTestPersistence();

        Archiver relationStore = new Archiver(
                dereferencer,
                new StatementStoreImpl(testKeyValueStore),
                TestUtil.getTestCrawlContext());

        relationStore.on(statement);

        // dereference subject

        IRI contentHash = relationStore
                .getStatementStore()
                .get(Pair.of(toIRI(URI.create("http://some")), HAS_VERSION));

        assertTrue(isBlankOrSkolemizedBlank(contentHash));
    }

    @Test
    public void doNotEmitSkolemizedBlanks() throws IOException {
        IRI skolemizedBlank = toSkolemizedBlank(toBlank());
        Triple statement = toStatement(toIRI(URI.create("http://some")),
                HAS_VERSION,
                skolemizedBlank);

        Dereferencer3<IRI> dereferencer = uri -> {
            throw new IOException("fails to dereference");
        };

        KeyValueStore testKeyValueStore = TestUtil.getTestPersistence();

        List<Triple> nodes = new ArrayList<>();

        Archiver relationStore = new Archiver(
                dereferencer,
                new StatementStoreImpl(testKeyValueStore),
                TestUtil.getTestCrawlContext(),
                nodes::add);

        relationStore.on(statement);

        assertThat(nodes.size(), Is.is(1));

        // dereference subject
        IRI contentHash = relationStore.getStatementStore().get(
                Pair.of(toIRI(URI.create("http://some")), HAS_VERSION));

        assertNull(contentHash);
    }

    @Test
    public void putContentThatNeedsDownload() throws IOException {
        BlankNode blank = toBlank();

        Triple statement
                = toStatement(toIRI(URI.create("http://some")),
                HAS_VERSION,
                blank);

        Dereferencer3<IRI> dereferencer = new DereferenceTest("#derefData");
        KeyValueStore testKeyValueStore = TestUtil.getTestPersistence();

        Archiver relationStore = getAppendOnlyRelationStore(testKeyValueStore, dereferencer);

        relationStore.on(statement);

        // dereference subject

        IRI contentHash = relationStore.getStatementStore().get(
                Pair.of(toIRI(URI.create("http://some")), HAS_VERSION));
        assertThat(contentHash, Is.is(RefNodeFactory.toIRI("http://some#derefData")));
    }

    private Archiver getAppendOnlyRelationStore(KeyValueStore testPersistencetence, Dereferencer3<IRI> dereferencer1) {
        return new Archiver(dereferencer1, new StatementStoreImpl(testPersistencetence), TestUtil.getTestCrawlContext());
    }

    private String toUTF8(InputStream content) throws IOException {
        return TestUtil.toUTF8(content);
    }

    @Test
    public void putNewVersionOfContent() throws IOException {
        Triple statement = toStatement(SOME_IRI, HAS_VERSION, toBlank());

        KeyValueStore keyValueStore = TestUtil.getTestPersistence();

        final DereferenceTest dereferencer = new DereferenceTest("#derefData");
        Archiver relationstore = getAppendOnlyRelationStore(
                keyValueStore, dereferencer);

        relationstore.on(statement);

        IRI contentHash = relationstore.getStatementStore().get(Pair.of(SOME_IRI, HAS_VERSION));
        assertThat(contentHash.getIRIString(), Is.is("http://some#derefData"));

        final DereferenceTest dereferencer1 = new DereferenceTest("#derefData2");
        relationstore = getAppendOnlyRelationStore(
                keyValueStore, dereferencer1);
        relationstore.on(statement);

        IRI contentHash2 = relationstore.getStatementStore().get(Pair.of(SOME_IRI, HAS_VERSION));


        assertThat(contentHash, Is.is(contentHash2));

        IRI newContentHash = relationstore.getStatementStore().get(Pair.of(HAS_PREVIOUS_VERSION, contentHash));

        assertThat(contentHash, not(Is.is(newContentHash)));
        assertThat(newContentHash.getIRIString(), Is.is("http://some#derefData2"));

        final DereferenceTest dereferencer2 = new DereferenceTest("#derefData3");
        relationstore = getAppendOnlyRelationStore(
                keyValueStore, dereferencer2);

        relationstore.on(statement);

        IRI newerContentHash = relationstore.getStatementStore().get(Pair.of(HAS_PREVIOUS_VERSION, newContentHash));
        assertThat(newerContentHash.getIRIString(), Is.is("http://some#derefData3"));
    }

    @Test
    public void archiveLastestTwo() throws IOException {
        Triple statement = toStatement(SOME_IRI, HAS_VERSION, toBlank());

        KeyValueStore keyValueStore = TestUtil.getTestPersistence();
        Archiver relationstore = getAppendOnlyRelationStore(
                keyValueStore, new DereferenceTest("#derefData"));

        relationstore.on(statement);

        IRI contentHash = relationstore.getStatementStore().get(Pair.of(SOME_IRI, HAS_VERSION));
        assertNotNull(contentHash);

        final DereferenceTest dereferencer1 = new DereferenceTest("#derefData2");
        relationstore = getAppendOnlyRelationStore(
                keyValueStore, dereferencer1);
        relationstore.on(statement);

        IRI contentHash2 = relationstore.getStatementStore().get(Pair.of(SOME_IRI, HAS_VERSION));
        assertThat(contentHash, Is.is(contentHash2));

        IRI newContentHash = relationstore.getStatementStore().get(Pair.of(HAS_PREVIOUS_VERSION, contentHash));

        assertThat(contentHash, not(Is.is(newContentHash)));
        assertThat(newContentHash.getIRIString(), Is.is("http://some#derefData2"));

        final DereferenceTest dereferencer2 = new DereferenceTest("#derefData3");
        relationstore = getAppendOnlyRelationStore(
                keyValueStore, dereferencer2);
        relationstore.on(statement);

        IRI newerContentHash = relationstore.getStatementStore().get(Pair.of(HAS_PREVIOUS_VERSION, newContentHash));

        assertThat(newerContentHash.getIRIString(), Is.is("http://some#derefData3"));
    }

    private class DereferenceTest implements Dereferencer3<IRI> {
        private final String prefix;

        public DereferenceTest(String s) {
            this.prefix = s;
        }

        @Override
        public IRI dereference(IRI uri) throws IOException {
            return RefNodeFactory.toIRI(uri.getIRIString() + prefix);
        }
    }
}
