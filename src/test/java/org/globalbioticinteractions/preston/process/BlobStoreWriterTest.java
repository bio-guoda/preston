package org.globalbioticinteractions.preston.process;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.Resources;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeRelation;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.store.AppendOnlyBlobStore;
import org.globalbioticinteractions.preston.store.AppendOnlyRelationStore;
import org.globalbioticinteractions.preston.store.FilePersistence;
import org.globalbioticinteractions.preston.store.Persistence;
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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BlobStoreWriterTest {

    private AppendOnlyBlobStore blobStore;
    private AppendOnlyRelationStore relationStore;
    private Path tempDir;
    private Path datasetDir;

    @Before
    public void init() throws IOException {
        tempDir = Files.createTempDirectory(Paths.get("target/"), "caching");
        datasetDir = Files.createTempDirectory(Paths.get("target/"), "datasets");
        Persistence persistence = new FilePersistence(tempDir.toFile(), datasetDir.toFile());
        this.blobStore = new AppendOnlyBlobStore(persistence);
        this.relationStore = new AppendOnlyRelationStore(blobStore, persistence, Resources::asInputStream);
    }

    @After
    public void destroy() {
        FileUtils.deleteQuietly(tempDir.toFile());
        FileUtils.deleteQuietly(datasetDir.toFile());
    }


    @Test
    public void generatePathFromUUID() {
        assertThat(BlobStoreWriter.toPath("3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"),
                is("3f/c9/b6/3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"));
    }

    @Test
    public void cacheString() throws IOException {
        ArrayList<RefNodeRelation> refNodes = new ArrayList<>();

        BlobStoreWriter blobStoreWriter = new BlobStoreWriter(this.blobStore, this.relationStore, refNodes::add);
        RefNodeString providedNode = new RefNodeString("https://example.org");
        blobStoreWriter.on(new RefNodeRelation(providedNode, RefNodeConstants.HAS_PART, providedNode));
        assertTrue(tempDir.toFile().exists());
        assertFalse(refNodes.isEmpty());

        RefNodeRelation cachedNode = refNodes.get(0);
        assertThat(cachedNode.getSource().getId(), is("50d7a905e3046b88638362cc34a31a1ae534766ca55e3aa397951efe653b062b"));
        assertThat(cachedNode.getSource().getLabel(), is("https://example.org"));
        assertTrue(cachedNode.getSource().equivalentTo(providedNode));

        FileUtils.deleteQuietly(tempDir.toFile());
    }

    @Test
    public void cacheContent() throws IOException, URISyntaxException {
        ArrayList<RefNodeRelation> refNodes = new ArrayList<>();

        BlobStoreWriter blobStoreWriter = new BlobStoreWriter(this.blobStore, this.relationStore, refNodes::add);


        URI testURI = getClass().getResource("test.txt").toURI();
        RefNode providedNode = new RefNodeString(testURI.toString());
        RefNodeRelation relation = new RefNodeRelation(providedNode, RefNodeConstants.HAS_CONTENT, null);

        blobStoreWriter.on(relation);
        assertTrue(tempDir.toFile().exists());
        assertFalse(refNodes.isEmpty());
        assertThat(refNodes.size(), is(1));

        RefNodeRelation cachedNode = (RefNodeRelation)refNodes.get(0);
        String expectedSHA256 = "50d7a905e3046b88638362cc34a31a1ae534766ca55e3aa397951efe653b062b";
        assertThat(cachedNode.getTarget().getId(), is(expectedSHA256));

        String label = cachedNode.getTarget().getLabel();
        assertThat(label, is("preston:50d7a905e3046b88638362cc34a31a1ae534766ca55e3aa397951efe653b062b"));

        InputStream inputStream = blobStore.get("preston:50d7a905e3046b88638362cc34a31a1ae534766ca55e3aa397951efe653b062b");

        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), is("https://example.org"));

        inputStream = cachedNode.getTarget().getData();
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), is("https://example.org"));

        String baseCacheDir = "/50/d7/a9/50d7a905e3046b88638362cc34a31a1ae534766ca55e3aa397951efe653b062b/";
        String absCacheDir = datasetDir.toAbsolutePath().toString() + baseCacheDir;


        File data = new File(absCacheDir + "data");
        assertTrue(data.exists());
        assertThat(IOUtils.toString(data.toURI(), StandardCharsets.UTF_8), is("https://example.org"));

        FileUtils.deleteQuietly(tempDir.toFile());
    }


}