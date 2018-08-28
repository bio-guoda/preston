package org.globalbioticinteractions.preston.store;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.globalbioticinteractions.preston.Hasher;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

public class AppendOnlyBlobStoreTest {

    @Test
    public void putImmutableStatement() throws IOException {
        URI GBIF = URI.create("http://gbif.org");
        URI GBIF_REGISTRY = URI.create("https://api.gbif.org/v1/registry");
        Triple<URI, URI, URI> statement
                = Triple.of(GBIF, Predicate.HAS_REGISTRY, GBIF_REGISTRY);

        Dereferencer dereferencer = new DereferenceTest("deref@");
        AppendOnlyBlobStore blobStore = createBlogStore(dereferencer);

        blobStore.put(statement);

        String key = blobStore.findKey(Pair.of(GBIF, Predicate.HAS_REGISTRY));

        assertThat(key, Is.is("809f41e24585d47dd30008e11d3848aec67065135042a28847b357af3ccf84e4"));

        InputStream URIString = blobStore.get(key);

        assertThat(toUTF8(URIString), Is.is("https://api.gbif.org/v1/registry"));
    }

    private AppendOnlyBlobStore createBlogStore(Dereferencer dereferencer) {
        return new AppendOnlyBlobStore(dereferencer, new Persistence() {
            private final Map<String, String> lookup = new TreeMap<>();

            @Override
            public void put(String key, String value) throws IOException {
                lookup.putIfAbsent(key, value);
            }

            @Override
            public String put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                String key = keyGeneratingStream.generateKeyWhileStreaming(is, os);
                lookup.putIfAbsent(key, toUTF8(new ByteArrayInputStream(os.toByteArray())));
                return key;
            }

            @Override
            public InputStream get(String key) throws IOException {
                String input = lookup.get(key);
                return input == null ? null : IOUtils.toInputStream(input, StandardCharsets.UTF_8);
            }
        });
    }

    @Test
    public void putContentThatNeedsDownload() throws IOException {
        Triple<URI, URI, URI> statement
                = Triple.of(URI.create("http://some"), Predicate.HAS_CONTENT, null);


        Dereferencer dereferencer = new DereferenceTest("derefData@");
        AppendOnlyBlobStore blobStore = createBlogStore(dereferencer);
        blobStore.put(statement);

        // dereference subject

        String contentHash = blobStore.findKey(Pair.of(URI.create("http://some"), Predicate.HAS_CONTENT_HASH));
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

    private String toUTF8(InputStream content) throws IOException {
        return IOUtils.toString(content, StandardCharsets.UTF_8);
    }

    @Test
    public void putNewVersionOfContent() throws IOException {
        Triple<URI, URI, URI> statement
                = Triple.of(URI.create("http://some"), Predicate.HAS_CONTENT, null);


        String prefix = "derefData@";
        Dereferencer dereferencer1 = new DereferenceTest(prefix);
        AppendOnlyBlobStore blobStore = createBlogStore(dereferencer1);
        blobStore.put(statement);

        String contentHash = blobStore.findKey(Pair.of(URI.create("http://some"), Predicate.HAS_CONTENT_HASH));
        String content = blobStore.findKey(Pair.of(URI.create("http://some"), Predicate.HAS_CONTENT));

        Dereferencer dereferencer = new DereferenceTest("derefData2@");
        blobStore.setDereferencer(dereferencer);
        blobStore.put(statement);

        String contentHash2 = blobStore.findKey(Pair.of(URI.create("http://some"), Predicate.HAS_CONTENT_HASH));
        String content2 = blobStore.findKey(Pair.of(URI.create("http://some"), Predicate.HAS_CONTENT));


        assertThat(contentHash, Is.is(contentHash2));
        assertThat(content, Is.is(content2));

        String newContentHash = blobStore.findKey(Pair.of(URI.create("preston:" + contentHash), Predicate.SUCCEEDED_BY));
        InputStream newContent = blobStore.get(newContentHash);

        assertThat(contentHash, not(Is.is(newContentHash)));
        assertThat(newContentHash, Is.is("960d96611c4048e05303f6f532590968fd5eb23d0035141c4b02653b436f568c"));

        assertThat(content, not(Is.is(newContent)));
        assertThat(toUTF8(newContent), Is.is("derefData2@http://some"));

        blobStore.setDereferencer(new DereferenceTest("derefData3@"));
        blobStore.put(statement);

        String newerContentHash = blobStore.findKey(Pair.of(URI.create("preston:" + newContentHash), Predicate.SUCCEEDED_BY));
        InputStream newerContent = blobStore.get(newerContentHash);

        assertThat(newerContentHash, Is.is("7e66eac09d137afe06dd73614e966a417260a111208dabe7225b05f02ce380fd"));
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
