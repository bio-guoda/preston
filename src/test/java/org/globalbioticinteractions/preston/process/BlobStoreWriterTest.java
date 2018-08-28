package org.globalbioticinteractions.preston.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeProxyData;
import org.globalbioticinteractions.preston.model.RefNodeRelation;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefNodeType;
import org.globalbioticinteractions.preston.model.RefNodeURI;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BlobStoreWriterTest {


    @Test
    public void generatePathFromUUID() {
        assertThat(BlobStoreWriter.toPath("3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"),
                is("3f/c9/b6/3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"));
    }

    @Test
    public void cacheString() throws IOException {
        Path tempDir = Files.createTempDirectory(Paths.get("target/"), "caching");

        ArrayList<RefNode> refNodes = new ArrayList<>();

        BlobStoreWriter blobStoreWriter = new BlobStoreWriter(refNodes::add);
        blobStoreWriter.setTmpDir(new File("target/"));
        RefNodeString providedNode = new RefNodeString(RefNodeType.URI, "https://example.org");
        blobStoreWriter.on(providedNode);
        assertTrue(tempDir.toFile().exists());
        assertFalse(refNodes.isEmpty());

        RefNode cachedNode = refNodes.get(0);
        assertThat(cachedNode.getId(), is("50d7a905e3046b88638362cc34a31a1ae534766ca55e3aa397951efe653b062b"));
        assertThat(cachedNode.getLabel(), is("https://example.org"));
        assertThat(cachedNode.getType(), is(RefNodeType.URI));
        assertThat(cachedNode, is(instanceOf(RefNodeProxyData.class)));
        assertTrue(cachedNode.equivalentTo(providedNode));

        FileUtils.deleteQuietly(tempDir.toFile());
    }

    @Test
    public void cacheContent() throws IOException, URISyntaxException {
        Path tempDir = Files.createTempDirectory(Paths.get("target/"), "caching");
        Path datasetDir = Files.createTempDirectory(Paths.get("target/"), "datasets");

        ArrayList<RefNode> refNodes = new ArrayList<>();

        BlobStoreWriter blobStoreWriter = new BlobStoreWriter(refNodes::add);
        blobStoreWriter.setTmpDir(tempDir.toFile());
        blobStoreWriter.setDatasetDir(datasetDir.toFile());
        URI testURI = getClass().getResource("test.txt").toURI();
        RefNode providedNode = new RefNodeString(RefNodeType.URI, testURI.toString());
        RefNode providedNodeBlob = new RefNodeURI(RefNodeType.URI, testURI);
        RefNode relation = new RefNodeRelation(providedNode, RefNodeConstants.DEREFERENCE_OF, providedNodeBlob);

        blobStoreWriter.on(relation);
        assertTrue(tempDir.toFile().exists());
        assertFalse(refNodes.isEmpty());
        assertThat(refNodes.size(), is(4));

        RefNode linkNode = refNodes.get(3);
        assertThat(linkNode, is(instanceOf(RefNodeRelation.class)));
        assertThat(((RefNodeRelation)linkNode).getTarget().getLabel(), is(providedNodeBlob.getLabel()));
        assertThat(((RefNodeRelation)linkNode).getRelationType().getLabel(), is(RefNodeConstants.DEREFERENCE_OF.getLabel()));

        RefNode cachedNode = refNodes.get(0);
        String expectedSHA256 = "4be693a8d624f6905339ece8d714bd81a62136a4f738b24580462f6f5e85a5fb";
        assertThat(cachedNode.getId(), is(expectedSHA256));
        assertThat(cachedNode.getSize(), is(109L));
        assertThat(cachedNode.getType(), is(RefNodeType.URI));
        assertThat(cachedNode, is(instanceOf(RefNodeProxyData.class)));
        assertTrue(cachedNode.equivalentTo(providedNode));

        String baseCacheDir = "/50/d7/a9/50d7a905e3046b88638362cc34a31a1ae534766ca55e3aa397951efe653b062b/";
        String absCacheDir = datasetDir.toAbsolutePath().toString() + baseCacheDir;
        File meta = new File(absCacheDir + "meta.json");
        assertTrue("expected [" + meta.toString() + "] to exist", meta.exists());

        JsonNode jsonNode = new ObjectMapper().readTree(meta);
        Iterator<String> fields = jsonNode.fieldNames();
        int count = 0;
        while (fields.hasNext()) {
            fields.next();
            count++;
        }
        assertThat(count, is(4));
        assertThat(jsonNode.get("id").asText(), is("50d7a905e3046b88638362cc34a31a1ae534766ca55e3aa397951efe653b062b"));
        assertThat(jsonNode.get("size").asLong(), is(19L));
        assertTrue(jsonNode.has("created"));
        DateTime created = ISODateTimeFormat.dateTime().withZoneUTC().parseDateTime(jsonNode.get("created").asText());
        assertThat(created, is(notNullValue()));


        File data = new File(absCacheDir + "data");
        assertTrue(data.exists());
        assertThat(IOUtils.toString(data.toURI(), StandardCharsets.UTF_8), is("https://example.org"));

        System.out.println(absCacheDir);

        FileUtils.deleteQuietly(tempDir.toFile());
    }


}