package org.globalbioticinteractions.preston.store;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.Hasher;
import org.globalbioticinteractions.preston.model.RefNodeFactory;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

public class AppendOnlyStatementStoreTest {

    private static final IRI SOME_IRI = RefNodeFactory.toIRI(URI.create("http://some"));

    @Test
    public void putContentThatFailsToDownload() throws IOException {
        BlankNode blank = RefNodeFactory.toBlank();
        Triple statement
                = RefNodeFactory.toStatement(blank,
                Predicate.WAS_DERIVED_FROM,
                RefNodeFactory.toIRI(URI.create("http://some")));

        Dereferencer dereferencer = uri -> {
            throw new IOException("fails to dereference");
        };

        Persistence testPersistence = TestUtil.getTestPersistence();

        AppendOnlyStatementStore relationStore = new AppendOnlyStatementStore(
                new AppendOnlyBlobStore(testPersistence),
                testPersistence,
                dereferencer);

        relationStore.put(statement);

        // dereference subject

        IRI contentHash = relationStore.get(
                Pair.of(Predicate.WAS_DERIVED_FROM,
                        RefNodeFactory.toIRI(URI.create("http://some"))));

        assertTrue(RefNodeFactory.isBlankOrSkolemizedBlank(contentHash));
    }

    @Test
    public void putContentThatNeedsDownload() throws IOException {
        BlankNode blank = RefNodeFactory.toBlank();
        Triple statement
                = RefNodeFactory.toStatement(blank,
                Predicate.WAS_DERIVED_FROM,
                RefNodeFactory.toIRI(URI.create("http://some")));

        Dereferencer dereferencer = new DereferenceTest("derefData@");
        Persistence testPersistence = TestUtil.getTestPersistence();

        AppendOnlyStatementStore relationStore = new AppendOnlyStatementStore(
                new AppendOnlyBlobStore(testPersistence),
                testPersistence,
                dereferencer);

        BlobStore blobStore = new AppendOnlyBlobStore(testPersistence);

        relationStore.put(statement);

        // dereference subject

        IRI contentHash = relationStore.get(
                Pair.of(Predicate.WAS_DERIVED_FROM,
                        RefNodeFactory.toIRI(URI.create("http://some"))));
        InputStream content = blobStore.get(contentHash);
        assertNotNull(contentHash);

        String expectedContent = "derefData@http://some";

        String actualContent = toUTF8(content);
        assertThat(actualContent, Is.is(expectedContent));
        assertThat(contentHash, Is.is(Hasher.calcSHA256(expectedContent)));
    }

    private AppendOnlyStatementStore getAppendOnlyRelationStore(Dereferencer dereferencer, BlobStore blobStore, Persistence testPersistencetence) {
        return new AppendOnlyStatementStore(blobStore, testPersistencetence, dereferencer);
    }

    private String toUTF8(InputStream content) throws IOException {
        return TestUtil.toUTF8(content);
    }

    @Test
    public void putNewVersionOfContent() throws IOException {
        Triple statement
                = RefNodeFactory.toStatement(RefNodeFactory.toBlank(), Predicate.WAS_DERIVED_FROM, SOME_IRI);


        String prefix = "derefData@";
        Dereferencer dereferencer1 = new DereferenceTest(prefix);

        BlobStore blogStore = new AppendOnlyBlobStore(TestUtil.getTestPersistence());

        AppendOnlyStatementStore relationstore = getAppendOnlyRelationStore(dereferencer1,
                blogStore,
                TestUtil.getTestPersistence());

        relationstore.put(statement);

        IRI contentHash = relationstore.get(Pair.of(Predicate.WAS_DERIVED_FROM, SOME_IRI));
        assertNotNull(contentHash);

        Dereferencer dereferencer = new DereferenceTest("derefData2@");
        relationstore.setDereferencer(dereferencer);
        relationstore.put(statement);

        IRI contentHash2 = relationstore.get(Pair.of(Predicate.WAS_DERIVED_FROM, SOME_IRI));


        assertThat(contentHash, Is.is(contentHash2));

        IRI newContentHash = relationstore.get(Pair.of(Predicate.WAS_REVISION_OF, contentHash));
        InputStream newContent = blogStore.get(newContentHash);

        assertThat(contentHash, not(Is.is(newContentHash)));
        assertThat(newContentHash.getIRIString(), Is.is("hash://sha256/960d96611c4048e05303f6f532590968fd5eb23d0035141c4b02653b436f568c"));

        assertThat(toUTF8(newContent), Is.is("derefData2@http://some"));

        relationstore.setDereferencer(new DereferenceTest("derefData3@"));
        relationstore.put(statement);

        IRI newerContentHash = relationstore.get(Pair.of(Predicate.WAS_REVISION_OF, newContentHash));
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
