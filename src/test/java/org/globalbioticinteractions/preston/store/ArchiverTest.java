package org.globalbioticinteractions.preston.store;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.Hasher;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_PREVIOUS_VERSION;
import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_VERSION;
import static org.globalbioticinteractions.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static org.globalbioticinteractions.preston.RefNodeConstants.WAS_REVISION_OF;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.isBlankOrSkolemizedBlank;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toBlank;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toIRI;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toSkolemizedBlank;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toStatement;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
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

        Dereferencer dereferencer = uri -> {
            throw new IOException("fails to dereference");
        };

        Persistence testPersistence = TestUtil.getTestPersistence();

        Archiver relationStore = new Archiver(
                new AppendOnlyBlobStore(testPersistence),
                dereferencer, new StatementStoreImpl(testPersistence));

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

        Dereferencer dereferencer = uri -> {
            throw new IOException("fails to dereference");
        };

        Persistence testPersistence = TestUtil.getTestPersistence();

        List<Triple> nodes = new ArrayList<>();

        Archiver relationStore = new Archiver(
                new AppendOnlyBlobStore(testPersistence),
                dereferencer,
                new StatementStoreImpl(testPersistence),
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

        Dereferencer dereferencer = new DereferenceTest("derefData@");
        Persistence testPersistence = TestUtil.getTestPersistence();

        Archiver relationStore = new Archiver(
                new AppendOnlyBlobStore(testPersistence),
                dereferencer, new StatementStoreImpl(testPersistence));

        BlobStore blobStore = new AppendOnlyBlobStore(testPersistence);

        relationStore.on(statement);

        // dereference subject

        IRI contentHash = relationStore.getStatementStore().get(
                Pair.of(toIRI(URI.create("http://some")), HAS_VERSION));
        InputStream content = blobStore.get(contentHash);
        assertNotNull(contentHash);

        String expectedContent = "derefData@http://some";

        String actualContent = toUTF8(content);
        assertThat(actualContent, Is.is(expectedContent));
        assertThat(contentHash, Is.is(Hasher.calcSHA256(expectedContent)));
    }

    private Archiver getAppendOnlyRelationStore(Dereferencer dereferencer, BlobStore blobStore, Persistence testPersistencetence) {
        return new Archiver(blobStore, dereferencer, new StatementStoreImpl(testPersistencetence));
    }

    private String toUTF8(InputStream content) throws IOException {
        return TestUtil.toUTF8(content);
    }

    @Test
    public void putNewVersionOfContent() throws IOException {
        Triple statement = toStatement(SOME_IRI, HAS_VERSION, toBlank());


        String prefix = "derefData@";
        Dereferencer dereferencer1 = new DereferenceTest(prefix);

        BlobStore blogStore = new AppendOnlyBlobStore(TestUtil.getTestPersistence());

        Archiver relationstore = getAppendOnlyRelationStore(dereferencer1,
                blogStore,
                TestUtil.getTestPersistence());

        relationstore.on(statement);

        IRI contentHash = relationstore.getStatementStore().get(Pair.of(SOME_IRI, HAS_VERSION));
        assertNotNull(contentHash);

        Dereferencer dereferencer = new DereferenceTest("derefData2@");
        relationstore.setDereferencer(dereferencer);
        relationstore.on(statement);

        IRI contentHash2 = relationstore.getStatementStore().get(Pair.of(SOME_IRI, HAS_VERSION));


        assertThat(contentHash, Is.is(contentHash2));

        IRI newContentHash = relationstore.getStatementStore().get(Pair.of(HAS_PREVIOUS_VERSION, contentHash));
        InputStream newContent = blogStore.get(newContentHash);

        assertThat(contentHash, not(Is.is(newContentHash)));
        assertThat(newContentHash.getIRIString(), Is.is("hash://sha256/960d96611c4048e05303f6f532590968fd5eb23d0035141c4b02653b436f568c"));

        assertThat(toUTF8(newContent), Is.is("derefData2@http://some"));

        relationstore.setDereferencer(new DereferenceTest("derefData3@"));
        relationstore.on(statement);

        IRI newerContentHash = relationstore.getStatementStore().get(Pair.of(HAS_PREVIOUS_VERSION, newContentHash));
        InputStream newerContent = blogStore.get(newerContentHash);

        assertThat(newerContentHash.getIRIString(), Is.is("hash://sha256/7e66eac09d137afe06dd73614e966a417260a111208dabe7225b05f02ce380fd"));
        assertThat(toUTF8(newerContent), Is.is("derefData3@http://some"));
    }

    private class DereferenceTest implements Dereferencer {

        private final String prefix;

        DereferenceTest(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public InputStream dereference(IRI uri) {
            return IOUtils.toInputStream(prefix + uri.getIRIString(), StandardCharsets.UTF_8);
        }
    }

}
