package org.globalbioticinteractions.preston.process;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.globalbioticinteractions.preston.DateUtil;
import org.globalbioticinteractions.preston.Hasher;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.Resources;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefStatement;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.store.AppendOnlyBlobStore;
import org.globalbioticinteractions.preston.store.AppendOnlyStatementStore;
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

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ContentResolverTest {

    private AppendOnlyBlobStore blobStore;
    private AppendOnlyStatementStore relationStore;
    private Path tempDir;
    private Path datasetDir;

    @Before
    public void init() throws IOException {
        tempDir = Files.createTempDirectory(Paths.get("target/"), "caching");
        datasetDir = Files.createTempDirectory(Paths.get("target/"), "datasets");
        Persistence persistence = new FilePersistence(tempDir.toFile(), datasetDir.toFile());
        this.blobStore = new AppendOnlyBlobStore(persistence);
        this.relationStore = new AppendOnlyStatementStore(blobStore, persistence, Resources::asInputStream);
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
    public void cacheString() throws IOException {
        ArrayList<RefStatement> refNodes = new ArrayList<>();

        ContentResolver contentResolver = new ContentResolver(this.blobStore, this.relationStore, refNodes::add);
        RefNodeString providedNode = new RefNodeString("https://example.org");
        contentResolver.on(new RefStatement(providedNode, RefNodeConstants.HAD_MEMBER, providedNode));
        assertTrue(tempDir.toFile().exists());
        assertFalse(refNodes.isEmpty());

        RefStatement cachedNode = refNodes.get(0);
        assertThat(cachedNode.getSubject().getContentHash().toString(), is("hash://sha256/50d7a905e3046b88638362cc34a31a1ae534766ca55e3aa397951efe653b062b"));
        assertThat(cachedNode.getSubject().getLabel(), is("https://example.org"));
        assertTrue(cachedNode.getSubject().equivalentTo(providedNode));

        FileUtils.deleteQuietly(tempDir.toFile());
    }

    @Test
    public void cacheContent() throws IOException, URISyntaxException {
        ArrayList<RefStatement> refNodes = new ArrayList<>();

        ContentResolver contentResolver = new ContentResolver(this.blobStore, this.relationStore, refNodes::add);


        URI testURI = getClass().getResource("test.txt").toURI();
        RefNode providedNode = new RefNodeString(testURI.toString());
        RefStatement relation = new RefStatement(null, RefNodeConstants.WAS_DERIVED_FROM, providedNode);

        contentResolver.on(relation);

        assertTrue(tempDir.toFile().exists());
        assertFalse(refNodes.isEmpty());
        assertThat(refNodes.size(), is(2));

        String expectedHash = "50d7a905e3046b88638362cc34a31a1ae534766ca55e3aa397951efe653b062b";
        String expectedValue = "https://example.org";

        RefStatement refStatement = refNodes.get(0);
        assertThat(refStatement.getSubject().getLabel(), is("hash://sha256/" + expectedHash));
        assertThat(refStatement.getPredicate().getLabel(), is(RefNodeConstants.GENERATED_AT_TIME.getLabel()));

        InputStream datetimeContent = refStatement.getObject().getContent();
        assertThat(datetimeContent, is(notNullValue()));
        String s = IOUtils.toString(datetimeContent, StandardCharsets.UTF_8);
        String dateTimeSuffix = "^^xsd:dateTime";
        assertThat(s, endsWith(dateTimeSuffix));
        assertNotNull(DateUtil.parse(s.replace(dateTimeSuffix, "")));

        RefStatement cachedNode = refNodes.get(1);
        assertContentWith(cachedNode, expectedHash, expectedValue);
        InputStream inputStream = cachedNode.getObject().getContent();
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), is(testURI.toString()));

        String baseCacheDir = "/50/d7/a9/" + expectedHash + "/";
        String absCacheDir = datasetDir.toAbsolutePath().toString() + baseCacheDir;


        File data = new File(absCacheDir + "data");
        assertTrue(data.exists());
        assertThat(IOUtils.toString(data.toURI(), StandardCharsets.UTF_8), is(expectedValue));


        assertContentWith(refNodes.get(1), expectedHash, expectedValue);


        FileUtils.deleteQuietly(tempDir.toFile());
    }

    private void assertContentWith(RefStatement cachedNode, String expectedHash, String expectedValue) throws IOException {
        String expectedSHA256 = "hash://sha256/" + expectedHash;
        assertThat(cachedNode.getSubject().getContentHash().toString(), is(expectedSHA256));

        String label = cachedNode.getSubject().getLabel();
        assertThat(label, is("hash://sha256/" + expectedHash));

        InputStream inputStream = blobStore.get(Hasher.toHashURI(expectedHash));

        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), is(expectedValue));

    }


}