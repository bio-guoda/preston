package bio.guoda.preston.process;

import bio.guoda.preston.store.KeyTo3LevelPath;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.Resources;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.Archiver;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.StatementStoreImpl;
import bio.guoda.preston.store.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ContentResolverTest {

    private BlobStoreAppendOnly blobStore;
    private Path tempDir;
    private Path datasetDir;
    private KeyValueStoreLocalFileSystem persistence;

    @Before
    public void init() throws IOException {
        tempDir = Files.createTempDirectory(Paths.get("target/"), "caching");
        datasetDir = Files.createTempDirectory(Paths.get("target/"), "datasets");
        this.persistence = new KeyValueStoreLocalFileSystem(tempDir.toFile(), new KeyTo3LevelPath(datasetDir.toFile().toURI()));
        this.blobStore = new BlobStoreAppendOnly(persistence);
    }

    private Archiver createStatementStore(StatementListener... listeners) {
        return new Archiver(blobStore, Resources::asInputStream, new StatementStoreImpl(persistence), TestUtil.getTestCrawlContext(), listeners);
    }

    @After
    public void destroy() {
        FileUtils.deleteQuietly(tempDir.toFile());
        FileUtils.deleteQuietly(datasetDir.toFile());
    }

    @Test
    public void cacheContent() throws IOException, URISyntaxException {
        ArrayList<Triple> refNodes = new ArrayList<>();

        StatementListener listener = createStatementStore(refNodes::add);


        URI testURI = getClass().getResource("test.txt").toURI();
        IRI providedNode = RefNodeFactory.toIRI(testURI);
        Triple relation = RefNodeFactory.toStatement(providedNode, RefNodeConstants.HAS_VERSION, RefNodeFactory.toBlank());

        listener.on(relation);

        assertTrue(tempDir.toFile().exists());
        assertFalse(refNodes.isEmpty());
        assertThat(refNodes.size(), is(3));

        assertThat(refNodes.get(1).getPredicate(), is(RefNodeConstants.WAS_GENERATED_BY));
        assertThat(refNodes.get(1).getObject(), is(TestUtil.getTestCrawlContext().getActivity()));

        String expectedHash = "50d7a905e3046b88638362cc34a31a1ae534766ca55e3aa397951efe653b062b";
        String expectedValue = "https://example.org";

        Triple cachedNode = refNodes.get(2);
        assertContentWith(cachedNode, expectedHash, expectedValue);
        InputStream inputStream = blobStore.get((IRI) cachedNode.getObject());
        assertNotNull(inputStream);
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), is("https://example.org"));

        String baseCacheDir = "/50/d7/" + expectedHash;
        String absCacheDir = datasetDir.toAbsolutePath().toString() + baseCacheDir;


        File data = new File(absCacheDir);
        assertTrue(data.exists());
        assertThat(IOUtils.toString(data.toURI(), StandardCharsets.UTF_8), is(expectedValue));

        assertContentWith(refNodes.get(2), expectedHash, expectedValue);

        FileUtils.deleteQuietly(tempDir.toFile());
    }

    private void assertContentWith(Triple cachedNode, String expectedHash, String expectedValue) throws IOException {
        String expectedSHA256 = "hash://sha256/" + expectedHash;
        assertThat(((IRI)cachedNode.getObject()).getIRIString(), is(expectedSHA256));

        String label = cachedNode.getObject().toString();
        assertThat(label, is("<hash://sha256/" + expectedHash + ">"));

        InputStream inputStream = blobStore.get(Hasher.toHashURI(expectedHash));

        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), is(expectedValue));
    }


}