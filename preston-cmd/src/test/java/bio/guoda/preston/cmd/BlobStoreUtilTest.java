package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;

public class BlobStoreUtilTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void blobStore() throws IOException, URISyntaxException {
        URL resource = getClass().getResource("index/data/27/f5/27f552c25bc733d05a5cc67e9ba63850");
        File root = new File(resource.toURI());
        File dataDir = root.getParentFile().getParentFile().getParentFile();

        Persisting persisting = new Persisting();
        persisting.setHashType(HashType.md5);

        persisting.setLocalDataDir(dataDir.getAbsolutePath());
        persisting.setLocalTmpDir(folder.newFolder("tmp").getAbsolutePath());
        persisting.setProvenanceArchor(RefNodeFactory.toIRI("hash://md5/ec998a9c63a64ac7bfef04c91ee84f16"));
        BlobStoreReadOnly blobStoreIndexed = BlobStoreUtil.createIndexedBlobStoreFor(new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                IRI iri = Hasher.calcHashIRI("foo\n", HashType.md5);
                if (!iri.equals(uri)) {
                    throw new IOException("kaboom!");
                }
                return IOUtils.toInputStream("foo", StandardCharsets.UTF_8);
            }
        }, persisting);

        InputStream inputStream = blobStoreIndexed.get(RefNodeFactory.toIRI("https://example.org"));

        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is("foo"));
    }

}