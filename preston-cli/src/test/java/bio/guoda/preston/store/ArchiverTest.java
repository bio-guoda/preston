package bio.guoda.preston.store;

import bio.guoda.preston.cmd.ActivityContext;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.TripleMatcher.hasTriple;
import static bio.guoda.preston.model.RefNodeFactory.isBlankOrSkolemizedBlank;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toSkolemizedBlank;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class ArchiverTest {

    private static final IRI SOME_IRI = toIRI(URI.create("http://some"));

    @Test
    public void putContentThatFailsToDownload() throws IOException {
        BlankNode blank = toBlank();
        Quad statement
                = toStatement(toIRI(URI.create("http://some")),
                HAS_VERSION,
                blank);

        Dereferencer<IRI> dereferencer = uri -> {
            throw new IOException("fails to dereference");
        };

        final StatementStoreImpl versionStore = new StatementStoreImpl(TestUtil.getTestPersistence());
        StatementsListener versionLogger = createVersionLogger(versionStore);

        Archiver relationStore = new Archiver(
                dereferencer,
                TestUtilForProcessor.getTestCrawlContext(),
                versionLogger);

        relationStore.on(statement);

        // dereference subject
        IRI contentHash = versionStore
                .get(Pair.of(toIRI(URI.create("http://some")), HAS_VERSION));

        assertTrue(isBlankOrSkolemizedBlank(contentHash));
    }

    private StatementsListener createVersionLogger(final StatementStore versionStore) {
        return new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {

                if (HAS_VERSION.equals(statement.getPredicate())) {
                    try {
                        versionStore.put(Pair.of(statement.getSubject(), HAS_VERSION), statement.getObject());
                    } catch (IOException e) {
                        fail(e.getMessage());
                    }
                }
            }
        };
    }

    @Test
    public void doNotDereferenceSkolemizedBlank() throws IOException {
        IRI skolemizedBlank = toSkolemizedBlank(toBlank());
        Quad statement = toStatement(toIRI(URI.create("http://some")),
                HAS_VERSION,
                skolemizedBlank);

        AtomicBoolean triedDereferencing = new AtomicBoolean(false);

        Dereferencer<IRI> dereferencer = uri -> {
            triedDereferencing.set(true);
            throw new IOException("fails to dereference");
        };

        KeyValueStore testKeyValueStore = TestUtil.getTestPersistence();

        List<Quad> nodes = new ArrayList<>();

        StatementStoreImpl versionStore = new StatementStoreImpl(testKeyValueStore);

        Archiver relationStore = new Archiver(
                dereferencer,
                TestUtilForProcessor.getTestCrawlContext(),
                TestUtilForProcessor.testListener(nodes),
                createVersionLogger(versionStore));

        relationStore.on(statement);

        assertFalse(triedDereferencing.get());
        assertThat(nodes.size(), Is.is(1));
        assertThat(nodes.get(0).getObject().ntriplesString(), containsString("/.well-known/genid/"));

        // dereference subject
        IRI contentHash = versionStore.get(
                Pair.of(toIRI(URI.create("http://some")), HAS_VERSION));

        assertThat(contentHash.getIRIString(), containsString("/.well-known/genid/"));
    }

    @Test
    public void includeGenerationActivity() throws IOException {
        ActivityContext testCrawlContext = TestUtilForProcessor.getTestCrawlContext();
        Quad statement = toStatement(testCrawlContext.getActivity(),
                toIRI(URI.create("http://some")),
                HAS_VERSION,
                toBlank());

        AtomicBoolean triedDereferencing = new AtomicBoolean(false);

        Dereferencer<IRI> dereferencer = uri -> {
            triedDereferencing.set(true);
            return toIRI("hash://sha256/bla");
        };

        KeyValueStore testKeyValueStore = TestUtil.getTestPersistence();

        List<Quad> nodes = new ArrayList<>();

        StatementStoreImpl versionStore = new StatementStoreImpl(testKeyValueStore);

        Archiver relationStore = new Archiver(
                dereferencer,
                testCrawlContext,
                TestUtilForProcessor.testListener(nodes),
                createVersionLogger(versionStore));

        relationStore.on(statement);

        assertTrue(triedDereferencing.get());
        assertThat(nodes.size(), Is.is(7));

        Quad secondStatement = nodes.get(1);
        assertThat(secondStatement.getSubject(),
                is(RefNodeFactory.toIRI("hash://sha256/bla")));
        assertThat(secondStatement.getPredicate(),
                is(RefNodeFactory.toIRI("http://www.w3.org/ns/prov#qualifiedGeneration")));

        String s = nodes.get(3).getSubject().toString();
        String qualifiedGeneration = s.substring(1, s.length() - 1);
        assertThat(nodes.get(3), hasTriple(toStatement(toIRI(qualifiedGeneration), IS_A, toIRI("http://www.w3.org/ns/prov#Generation"))));
        assertThat(nodes.get(4), hasTriple(toStatement(toIRI(qualifiedGeneration), toIRI("http://www.w3.org/ns/prov#wasInformedBy"), testCrawlContext.getActivity())));
        assertThat(nodes.get(5), hasTriple(toStatement(toIRI(qualifiedGeneration), toIRI("http://www.w3.org/ns/prov#used"), toIRI("http://some"))));
        assertThat(nodes.get(6), hasTriple(toStatement(toIRI("http://some"), toIRI("http://purl.org/pav/hasVersion"), toIRI("hash://sha256/bla"))));
    }

    @Test
    public void putContentThatNeedsDownload() throws IOException {
        BlankNode blank = toBlank();

        Quad statement
                = toStatement(toIRI(URI.create("http://some")),
                HAS_VERSION,
                blank);

        Dereferencer<IRI> dereferencer = new DereferenceTest("#derefData");
        KeyValueStore testKeyValueStore = TestUtil.getTestPersistence();

        StatementStoreImpl versionStore = new StatementStoreImpl(testKeyValueStore);
        Archiver relationStore = getAppendOnlyRelationStore(dereferencer, versionStore);

        relationStore.on(statement);

        // dereference subject

        IRI contentHash = versionStore.get(
                Pair.of(toIRI(URI.create("http://some")), HAS_VERSION));
        assertNotNull(contentHash);
        assertThat(contentHash, Is.is(RefNodeFactory.toIRI("http://some#derefData")));
    }

    private Archiver getAppendOnlyRelationStore(Dereferencer<IRI> dereferencer1, StatementStore statementStore) {
        return new Archiver(dereferencer1, TestUtilForProcessor.getTestCrawlContext(), createVersionLogger(statementStore));
    }

    @Test
    public void putNewVersionOfContent() throws IOException {
        Quad statement = toStatement(SOME_IRI, HAS_VERSION, toBlank());

        KeyValueStore keyValueStore = TestUtil.getTestPersistence();
        StatementStore versionStore = new StatementStoreImpl(keyValueStore);

        final DereferenceTest dereferencer = new DereferenceTest("#derefData");
        Archiver relationstore = getAppendOnlyRelationStore(dereferencer, versionStore);

        relationstore.on(statement);

        IRI contentHash = versionStore.get(Pair.of(SOME_IRI, HAS_VERSION));
        assertThat(contentHash.getIRIString(), Is.is("http://some#derefData"));

        final DereferenceTest dereferencer1 = new DereferenceTest("#derefData2");
        relationstore = getAppendOnlyRelationStore(dereferencer1, versionStore);
        relationstore.on(statement);

        IRI contentHash2 = versionStore.get(Pair.of(SOME_IRI, HAS_VERSION));

        assertThat(contentHash, Is.is(contentHash2));
    }

    @Test
    public void archiveLastestTwo() throws IOException {
        Quad statement = toStatement(SOME_IRI, HAS_VERSION, toBlank());

        KeyValueStore keyValueStore = TestUtil.getTestPersistence();
        StatementStore versionStore = new StatementStoreImpl(keyValueStore);

        Archiver relationstore = getAppendOnlyRelationStore(
                new DereferenceTest("#derefData"), versionStore);

        relationstore.on(statement);

        IRI contentHash = versionStore.get(Pair.of(SOME_IRI, HAS_VERSION));
        assertNotNull(contentHash);

        final DereferenceTest dereferencer1 = new DereferenceTest("#derefData2");
        relationstore = getAppendOnlyRelationStore(
                dereferencer1, versionStore);
        relationstore.on(statement);

        IRI contentHash2 = versionStore.get(Pair.of(SOME_IRI, HAS_VERSION));
        assertThat(contentHash, Is.is(contentHash2));
    }

    private class DereferenceTest implements Dereferencer<IRI> {
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
