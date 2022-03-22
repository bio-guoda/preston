package bio.guoda.preston.process;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.Archiver;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.DereferencerContentAddressed;
import bio.guoda.preston.store.KeyTo3LevelPath;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.TestUtilForProcessor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
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

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class ContentResolverTest {

    private BlobStoreAppendOnly blobStore;
    private Path tempDir;
    private Path datasetDir;

    @Before
    public void init() throws IOException {
        tempDir = Files.createTempDirectory(Paths.get("target/"), "caching");
        datasetDir = Files.createTempDirectory(Paths.get("target/"), "datasets");
        HashType type = HashType.sha256;
        KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory keyValueStreamFactory = new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(type);
        KeyValueStoreLocalFileSystem persistence = new KeyValueStoreLocalFileSystem(tempDir.toFile(), new KeyTo3LevelPath(datasetDir.toFile().toURI()), keyValueStreamFactory);
        this.blobStore = new BlobStoreAppendOnly(persistence, true, type);
    }

    private Archiver createStatementStore(StatementsListener... listeners) {
        return new Archiver(new DereferencerContentAddressed(ResourcesHTTP::asInputStream, blobStore), TestUtilForProcessor.getTestCrawlContext(), listeners);
    }

    @After
    public void destroy() {
        FileUtils.deleteQuietly(tempDir.toFile());
        FileUtils.deleteQuietly(datasetDir.toFile());
    }

    @Test
    public void cacheContent() throws IOException, URISyntaxException {
        ArrayList<Quad> refNodes = new ArrayList<>();

        StatementsListener listener = createStatementStore(new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                refNodes.add(statement);
            }
        });


        URI testURI = getClass().getResource("test.txt").toURI();
        IRI providedNode = toIRI(testURI);
        Quad relation = RefNodeFactory.toStatement(TestUtilForProcessor.getTestCrawlContext().getActivity(), providedNode, RefNodeConstants.HAS_VERSION, RefNodeFactory.toBlank());

        listener.on(relation);

        assertTrue(tempDir.toFile().exists());
        assertFalse(refNodes.isEmpty());
        assertThat(refNodes.size(), is(7));

        BlankNodeOrIRI activity = refNodes.get(0).getGraphName().get();

        assertThat(refNodes.get(0).getPredicate(), is(RefNodeConstants.WAS_GENERATED_BY));
        assertThat(refNodes.get(0).getObject(), is(activity));
        
        String expectedHash = "50d7a905e3046b88638362cc34a31a1ae534766ca55e3aa397951efe653b062b";
        String expectedValue = "https://example.org";

        Quad cachedNode = refNodes.get(6);
        assertContentWith(cachedNode, expectedHash, expectedValue);
        InputStream inputStream = blobStore.get((IRI) cachedNode.getObject());
        assertNotNull(inputStream);
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), is("https://example.org"));

        String baseCacheDir = "/50/d7/" + expectedHash;
        String absCacheDir = datasetDir.toAbsolutePath().toString() + baseCacheDir;


        File data = new File(absCacheDir);
        assertTrue(data.exists());
        assertThat(IOUtils.toString(data.toURI(), StandardCharsets.UTF_8), is(expectedValue));

        assertContentWith(refNodes.get(6), expectedHash, expectedValue);

        FileUtils.deleteQuietly(tempDir.toFile());
    }

    private void assertContentWith(Quad cachedNode, String expectedHash, String expectedValue) throws IOException {
        String expectedSHA256 = "hash://sha256/" + expectedHash;
        assertThat(((IRI)cachedNode.getObject()).getIRIString(), is(expectedSHA256));

        String label = cachedNode.getObject().toString();
        assertThat(label, is("<hash://sha256/" + expectedHash + ">"));

        InputStream inputStream = blobStore.get(Hasher.toHashIRI(HashType.sha256, expectedHash));

        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), is(expectedValue));
    }


}