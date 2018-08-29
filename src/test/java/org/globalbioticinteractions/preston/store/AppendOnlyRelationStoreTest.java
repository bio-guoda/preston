package org.globalbioticinteractions.preston.store;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.globalbioticinteractions.preston.Hasher;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

public class AppendOnlyRelationStoreTest {

    @Test
    public void putImmutableStatement() throws IOException {
        URI GBIF = URI.create("http://gbif.org");
        URI GBIF_REGISTRY = URI.create("https://api.gbif.org/v1/registry");
        Triple<URI, URI, URI> statement
                = Triple.of(GBIF, Predicate.HAS_REGISTRY, GBIF_REGISTRY);

        Dereferencer dereferencer = new DereferenceTest("deref@");
        AppendOnlyBlobStore blobStore1 = new AppendOnlyBlobStore(TestUtil.getTestPersistence());
        RelationStore<URI> blobStore = getAppendOnlyRelationStore(dereferencer, blobStore1, TestUtil.getTestPersistence());

        blobStore.put(statement);

        URI key = blobStore.findKey(Pair.of(GBIF, Predicate.HAS_REGISTRY));

        assertThat(key.toString(), Is.is("hash://sha256/809f41e24585d47dd30008e11d3848aec67065135042a28847b357af3ccf84e4"));

        InputStream URIString = blobStore1.get(key);

        assertThat(toUTF8(URIString), Is.is("https://api.gbif.org/v1/registry"));
    }

    private AppendOnlyBlobStore createBlogStore() {
        return new AppendOnlyBlobStore(TestUtil.getTestPersistence());
    }

    @Test
    public void putContentThatNeedsDownload() throws IOException {
        Triple<URI, URI, URI> statement
                = Triple.of(URI.create("http://some"), Predicate.HAS_CONTENT, null);


        Dereferencer dereferencer = new DereferenceTest("derefData@");
        Persistence testPersistence = TestUtil.getTestPersistence();
        AppendOnlyRelationStore relationStore = getAppendOnlyRelationStore(dereferencer,
                new AppendOnlyBlobStore(testPersistence),
                testPersistence);

        AppendOnlyBlobStore blobStore = new AppendOnlyBlobStore(testPersistence);

        relationStore.put(statement);

        // dereference subject

        URI contentHash = relationStore.findKey(Pair.of(URI.create("http://some"), Predicate.HAS_CONTENT_HASH));
        InputStream content = blobStore.get(contentHash);

        assertNotNull(contentHash);
        InputStream otherContent = blobStore.get(contentHash);
        String actualOtherContent = toUTF8(otherContent);

        String expectedContent = "derefData@http://some";

        String actualContent = toUTF8(content);
        assertThat(actualContent, Is.is(expectedContent));
        assertThat(contentHash, Is.is(Hasher.calcSHA256(expectedContent)));
        assertThat(actualContent, Is.is(actualOtherContent));
    }

    private AppendOnlyRelationStore getAppendOnlyRelationStore(Dereferencer dereferencer, BlobStore blobStore, Persistence testPersistencetence) {
        return new AppendOnlyRelationStore(blobStore, testPersistencetence, dereferencer);
    }

    private String toUTF8(InputStream content) throws IOException {
        return TestUtil.toUTF8(content);
    }

    @Test
    public void putNewVersionOfContent() throws IOException {
        Triple<URI, URI, URI> statement
                = Triple.of(URI.create("http://some"), Predicate.HAS_CONTENT, null);


        String prefix = "derefData@";
        Dereferencer dereferencer1 = new DereferenceTest(prefix);
        BlobStore blogStore = new AppendOnlyBlobStore(TestUtil.getTestPersistence());
        AppendOnlyRelationStore relationstore = getAppendOnlyRelationStore(dereferencer1, blogStore, TestUtil.getTestPersistence());
        relationstore.put(statement);

        URI contentHash = relationstore.findKey(Pair.of(URI.create("http://some"), Predicate.HAS_CONTENT_HASH));
        URI content = relationstore.findKey(Pair.of(URI.create("http://some"), Predicate.HAS_CONTENT));

        Dereferencer dereferencer = new DereferenceTest("derefData2@");
        relationstore.setDereferencer(dereferencer);
        relationstore.put(statement);

        URI contentHash2 = relationstore.findKey(Pair.of(URI.create("http://some"), Predicate.HAS_CONTENT_HASH));
        URI content2 = relationstore.findKey(Pair.of(URI.create("http://some"), Predicate.HAS_CONTENT));


        assertThat(contentHash, Is.is(contentHash2));
        assertThat(content, Is.is(content2));

        URI newContentHash = relationstore.findKey(Pair.of(contentHash, Predicate.SUCCEEDED_BY));
        InputStream newContent = blogStore.get(newContentHash);

        assertThat(contentHash, not(Is.is(newContentHash)));
        assertThat(newContentHash.toString(), Is.is("hash://sha256/960d96611c4048e05303f6f532590968fd5eb23d0035141c4b02653b436f568c"));

        assertThat(content, not(Is.is(newContent)));
        assertThat(toUTF8(newContent), Is.is("derefData2@http://some"));

        relationstore.setDereferencer(new DereferenceTest("derefData3@"));
        relationstore.put(statement);

        URI newerContentHash = relationstore.findKey(Pair.of(newContentHash, Predicate.SUCCEEDED_BY));
        InputStream newerContent = blogStore.get(newerContentHash);

        assertThat(newerContentHash.toString(), Is.is("hash://sha256/7e66eac09d137afe06dd73614e966a417260a111208dabe7225b05f02ce380fd"));
        assertThat(toUTF8(newerContent), Is.is("derefData3@http://some"));
    }

    private class DereferenceTest implements Dereferencer {

        private final String prefix;

        DereferenceTest(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public InputStream dereference(URI uri) {
            return IOUtils.toInputStream(prefix + uri.toString(), StandardCharsets.UTF_8);
        }
    }

}
