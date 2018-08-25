package org.globalbioticinteractions.preston.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globalbioticinteractions.preston.cmd.CmdList;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeCached;
import org.joda.time.format.ISODateTimeFormat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Date;

public class Caching extends RefNodeProcessor {
    private static Log LOG = LogFactory.getLog(CmdList.class);

    public Caching(RefNodeListener... listeners) {
        super(listeners);
    }

    @Override
    public void on(RefNode refNode) {
        try {
            emit(cache(refNode));
        } catch (IOException e) {
            LOG.warn("failed to handle [" + refNode.getLabel() + "]", e);
        }
    }

    public static RefNode cache(RefNode refNode) throws IOException {
        File cacheDir = new File("cacheDir");
        FileUtils.forceMkdir(cacheDir);
        File cache = File.createTempFile("cacheFile", ".tmp", cacheDir);
        final String id = calcSHA256(refNode.getData(), new FileOutputStream(cache));
        RefNodeCached datasetCached = new RefNodeCached(refNode, id);
        if (!getDataFile(id).exists()) {
            cacheFile(cache, datasetCached);
        }
        return new RefNodeCached(refNode, id, getDataFile(id));
    }

    private static void cacheFile(File dataFile, RefNode refNode) throws IOException {
        File datasetPath = getDatasetDir(refNode.getId());
        FileUtils.forceMkdir(datasetPath);
        File destFile = getDataFile(refNode.getId());
        FileUtils.moveFile(dataFile, destFile);
        FileUtils.copyToFile(IOUtils.toInputStream(refNode.getId(), StandardCharsets.UTF_8), new File(datasetPath, "data.sha256"));
        ObjectNode node = new ObjectMapper().createObjectNode();
        node.put("id", refNode.getId());
        if (null != refNode.getParent()) {
            node.put("parentId", refNode.getParent().getId());
        }
        node.put("type", refNode.getType().toString());
        node.put("created", ISODateTimeFormat.dateTime().withZoneUTC().print(new Date().getTime()));
        FileUtils.copyToFile(IOUtils.toInputStream(node.toString(), StandardCharsets.UTF_8), new File(datasetPath, "meta.json"));
    }

    public static File getDataFile(String id) {
        return new File(getDatasetDir(id), "data");
    }

    private static File getDatasetDir(String id) {
        return new File("datasets/" + toPath(id));
    }

    public static String calcSHA256(InputStream is, OutputStream os) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            DigestInputStream digestInputStream = new DigestInputStream(is, md);
            IOUtils.copy(digestInputStream, os);
            digestInputStream.close();
            os.flush();
            os.close();
            return String.format("%064x", new BigInteger(1, md.digest()));
        } catch (IOException | NoSuchAlgorithmException var9) {
            throw new IOException("failed to cache dataset", var9);
        }
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


}
