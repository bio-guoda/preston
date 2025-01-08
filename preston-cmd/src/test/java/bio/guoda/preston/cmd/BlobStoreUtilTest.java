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
    public void indexedBlobStore() throws IOException, URISyntaxException {
        File dataDir = getDataDir();

        Persisting persisting = getPersisting(dataDir);
        BlobStoreReadOnly blobStoreIndexed = BlobStoreUtil.createIndexedBlobStoreFor(getBlobStore(), persisting);

        InputStream inputStream = blobStoreIndexed.get(RefNodeFactory.toIRI("https://example.org"));

        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is("foo"));
    }

    @Test
    public void indexedBlobStoreWithoutProvenanceAnchor() throws IOException, URISyntaxException {
        File dataDir = getDataDir();

        Persisting persisting = getPersisting(dataDir);
        persisting.setProvenanceArchor(CmdWithProvenance.PROVENANCE_ANCHOR_DEFAULT);
        BlobStoreReadOnly blobStoreIndexed = BlobStoreUtil.createIndexedBlobStoreFor(getBlobStore(), persisting);

        InputStream inputStream = blobStoreIndexed.get(RefNodeFactory.toIRI("https://example.org"));

        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is("foo"));
    }

    @Test(expected = RuntimeException.class)
    public void indexedBlobStoreWithoutProvenanceAnchorNoProvenanceIndex() throws IOException, URISyntaxException {
        File dataDir = getDataDir("index-data-no-provenance/d3/b0/d3b07384d113edec49eaa6238ad5ff00");

        Persisting persisting = getPersisting(dataDir);
        persisting.setProvenanceArchor(CmdWithProvenance.PROVENANCE_ANCHOR_DEFAULT);

        try {
            BlobStoreUtil.createIndexedBlobStoreFor(getBlobStore(), persisting);
        } catch(RuntimeException ex) {
            assertThat(ex.getMessage(), Is.is("Cannot find most recent version: no provenance logs found."));
            throw ex;
        }
    }

    @Test
    public void resolvingBlobStoreWithProvenanceAnchor() throws IOException, URISyntaxException {
        File dataDir = getDataDir();

        Persisting persisting = getPersisting(dataDir);
        BlobStoreReadOnly blobStoreIndexed = BlobStoreUtil.createResolvingBlobStoreFor(getBlobStore(), persisting);

        InputStream inputStream = blobStoreIndexed.get(RefNodeFactory.toIRI("https://example.org"));

        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is("foo"));
    }

    @Test
    public void resolvingBlobStoreWithProvenanceAnchorDefault() throws IOException, URISyntaxException {
        File dataDir = getDataDir();

        Persisting persisting = getPersisting(dataDir);
        persisting.setProvenanceArchor(CmdWithProvenance.PROVENANCE_ANCHOR_DEFAULT);
        BlobStoreReadOnly blobStoreIndexed = BlobStoreUtil.createResolvingBlobStoreFor(getBlobStore(), persisting);

        InputStream inputStream = blobStoreIndexed.get(RefNodeFactory.toIRI("https://example.org"));

        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is("foo"));
    }

    private BlobStoreReadOnly getBlobStore() {
        return new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                IRI iri = Hasher.calcHashIRI("foo\n", HashType.md5);
                if (!iri.equals(uri)) {
                    throw new IOException("kaboom!");
                }
                return IOUtils.toInputStream("foo", StandardCharsets.UTF_8);
            }
        };
    }

    private Persisting getPersisting(File dataDir) throws IOException {
        Persisting persisting = new Persisting();
        persisting.setHashType(HashType.md5);

        persisting.setDataDir(dataDir.getAbsolutePath());
        persisting.setTmpDir(folder.newFolder("tmp").getAbsolutePath());
        persisting.setProvenanceArchor(RefNodeFactory.toIRI("hash://md5/ec998a9c63a64ac7bfef04c91ee84f16"));
        return persisting;
    }


    private File getDataDir() throws URISyntaxException {
        String s = "index-data/27/f5/27f552c25bc733d05a5cc67e9ba63850";
        return getDataDir(s);
    }

    private File getDataDir(String s) throws URISyntaxException {
        URL resource = getClass().getResource(s);
        File root = new File(resource.toURI());
        return root.getParentFile().getParentFile().getParentFile();
    }

}