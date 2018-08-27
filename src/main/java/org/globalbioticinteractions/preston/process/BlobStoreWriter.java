package org.globalbioticinteractions.preston.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globalbioticinteractions.preston.Hasher;
import org.globalbioticinteractions.preston.cmd.CmdList;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeProxyData;
import org.globalbioticinteractions.preston.model.RefNodeProxyParent;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefNodeType;
import org.joda.time.format.ISODateTimeFormat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;

public class BlobStoreWriter extends RefNodeProcessor {
    public static final RefNodeString DEREFERENCE_OF = new RefNodeString(null, RefNodeType.URI, "http://example.org/dereferenceOf");
    private static Log LOG = LogFactory.getLog(CmdList.class);
    private File tmpDir = new File("tmp");
    private File datasetDir = new File("datasets");

    public BlobStoreWriter(RefNodeListener... listeners) {
        super(listeners);
    }

    @Override
    public void on(RefNode refNode) {
        try {
            RefNode parentCached = refNode.getParent() == null
                    ? null
                    : cache(refNode.getParent(), getTmpDir(), getDatasetDir());
            RefNode refNodeWithCachedParent = new RefNodeProxyParent(parentCached, refNode);
            RefNode dereferencedNode = cache(refNodeWithCachedParent, getTmpDir(), getDatasetDir());
            emit(dereferencedNode);
            emit(new RefNodeRelation(parentCached, DEREFERENCE_OF, dereferencedNode));
        } catch (IOException e) {
            LOG.warn("failed to handle [" + refNode.getLabel() + "]", e);
        }
    }

    public static RefNode cache(RefNode refNode, File tmpDir, File dataDir) throws IOException {
        FileUtils.forceMkdir(tmpDir);
        File tmpFile = File.createTempFile("cacheFile", ".tmp", tmpDir);
        try {
            final String id = Hasher.calcSHA256(refNode.getData(), new FileOutputStream(tmpFile));
            if (!getDataFile(id, dataDir).exists()) {
                cacheFile(tmpFile, new RefNodeProxyData(refNode, id), dataDir);
            }
            return new RefNodeProxyData(refNode, id, getDataFile(id, dataDir));
        } finally {
            FileUtils.deleteQuietly(tmpFile);
        }
    }

    private static void cacheFile(File tmpFile, RefNode refNode, File dataDir) throws IOException {
        File datasetPath = getDatasetDir(refNode.getId(), dataDir);
        FileUtils.forceMkdir(datasetPath);
        File destFile = getDataFile(refNode.getId(), dataDir);
        FileUtils.moveFile(tmpFile, destFile);
        writeMeta(refNode, datasetPath, destFile);
    }

    private static void writeMeta(RefNode refNode, File datasetPath, File destFile) throws IOException {
        ObjectNode node = new ObjectMapper().createObjectNode();
        node.put("id", refNode.getId());
        if (null != refNode.getParent()) {
            node.put("parentId", refNode.getParent().getId());
        }
        node.put("type", refNode.getType().toString());
        if (destFile.exists()) {
            node.put("size", destFile.length());
        }
        node.put("created", ISODateTimeFormat.dateTime().withZoneUTC().print(new Date().getTime()));
        FileUtils.copyToFile(IOUtils.toInputStream(node.toString(), StandardCharsets.UTF_8), new File(datasetPath, "meta.json"));
    }

    public static File getDataFile(String id, File dataDir) {
        return new File(getDatasetDir(id, dataDir), "data");
    }

    private static File getDatasetDir(String id, File dataDir) {
        return new File(dataDir, toPath(id));
    }

    public static String toPath(String id) {
        if (StringUtils.length(id) < 8) {
            throw new IllegalArgumentException("expected id [" + id + "] of at least 8 characters");
        }
        String u0 = id.substring(0, 2);
        String u1 = id.substring(2, 4);
        String u2 = id.substring(4, 6);
        return StringUtils.join(Arrays.asList(u0, u1, u2, id), "/");
    }


    public File getTmpDir() {
        return tmpDir;
    }

    public void setTmpDir(File tmpDir) {
        this.tmpDir = tmpDir;
    }

    public File getDatasetDir() {
        return datasetDir;
    }

    public void setDatasetDir(File datasetDir) {
        this.datasetDir = datasetDir;
    }
}
