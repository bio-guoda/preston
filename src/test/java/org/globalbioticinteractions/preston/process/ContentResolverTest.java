package org.globalbioticinteractions.preston.process;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.Hasher;
import org.globalbioticinteractions.preston.Resources;
import org.globalbioticinteractions.preston.model.RefNodeFactory;
import org.globalbioticinteractions.preston.store.AppendOnlyBlobStore;
import org.globalbioticinteractions.preston.store.AppendOnlyStatementStore;
import org.globalbioticinteractions.preston.store.FilePersistence;
import org.globalbioticinteractions.preston.store.Predicate;
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

    private AppendOnlyBlobStore blobStore;
    private Path tempDir;
    private Path datasetDir;
    private FilePersistence persistence;

    @Before
    public void init() throws IOException {
        tempDir = Files.createTempDirectory(Paths.get("target/"), "caching");
        datasetDir = Files.createTempDirectory(Paths.get("target/"), "datasets");
        this.persistence = new FilePersistence(tempDir.toFile(), datasetDir.toFile());
        this.blobStore = new AppendOnlyBlobStore(persistence);
    }

    private AppendOnlyStatementStore createStatementStore(RefStatementListener... listeners) {
        return new AppendOnlyStatementStore(blobStore, persistence, Resources::asInputStream, listeners);
    }

    @After
    public void destroy() {
        FileUtils.deleteQuietly(tempDir.toFile());
        FileUtils.deleteQuietly(datasetDir.toFile());
    }


    @Test
    public void generatePathFromUUID() {
        assertThat(FilePersistence.toPath("hash://sha256/3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"),
                is("3f/c9/b6/3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"));
    }

    @Test
    public void cacheContent() throws IOException, URISyntaxException {
        ArrayList<Triple> refNodes = new ArrayList<>();

        RefStatementListener listener = createStatementStore(refNodes::add);


        URI testURI = getClass().getResource("test.txt").toURI();
        IRI providedNode = RefNodeFactory.toIRI(testURI);
        Triple relation = RefNodeFactory.toStatement(RefNodeFactory.toBlank("test"), Predicate.WAS_DERIVED_FROM, providedNode);

        listener.on(relation);

        assertTrue(tempDir.toFile().exists());
        assertFalse(refNodes.isEmpty());
        assertThat(refNodes.size(), is(2));

        String expectedHash = "50d7a905e3046b88638362cc34a31a1ae534766ca55e3aa397951efe653b062b";
        String expectedValue = "https://example.org";

        Triple cachedNode = refNodes.get(1);
        assertContentWith(cachedNode, expectedHash, expectedValue);
        InputStream inputStream = blobStore.get((IRI) cachedNode.getSubject());
        assertNotNull(inputStream);
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), is("https://example.org"));

        String baseCacheDir = "/50/d7/a9/" + expectedHash + "/";
        String absCacheDir = datasetDir.toAbsolutePath().toString() + baseCacheDir;


        File data = new File(absCacheDir + "data");
        assertTrue(data.exists());
        assertThat(IOUtils.toString(data.toURI(), StandardCharsets.UTF_8), is(expectedValue));


        assertContentWith(refNodes.get(1), expectedHash, expectedValue);


        FileUtils.deleteQuietly(tempDir.toFile());
    }

    private void assertContentWith(Triple cachedNode, String expectedHash, String expectedValue) throws IOException {
        String expectedSHA256 = "hash://sha256/" + expectedHash;
        assertThat(((IRI)cachedNode.getSubject()).getIRIString(), is(expectedSHA256));

        String label = cachedNode.getSubject().toString();
        assertThat(label, is("<hash://sha256/" + expectedHash + ">"));

        InputStream inputStream = blobStore.get(Hasher.toHashURI(expectedHash));

        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), is(expectedValue));

    }


}