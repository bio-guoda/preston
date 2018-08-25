package org.globalbioticinteractions.preston.cmd;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globalbioticinteractions.preston.Dataset;
import org.globalbioticinteractions.preston.DatasetListener;
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

public class DatasetListenerCaching implements DatasetListener {
    private static Log LOG = LogFactory.getLog(CmdList.class);
    private final DatasetListener[] next;

    public DatasetListenerCaching(DatasetListener... next) {
        this.next = next;
    }


    @Override
    public void onDataset(Dataset dataset) {
        try {
            Dataset cache = cache(dataset);
            for (DatasetListener datasetListener : next) {
                datasetListener.onDataset(cache);
            }
        } catch (IOException e) {
            LOG.warn("failed to handle [" + dataset.getLabel() + "]", e);
        }
    }

    public static Dataset cache(Dataset dataset) throws IOException {
        File cacheDir = new File("cacheDir");
        FileUtils.forceMkdir(cacheDir);
        File cache = File.createTempFile("cacheFile", ".tmp", cacheDir);
        final String id = calcSHA256(dataset.getData(), new FileOutputStream(cache));
        DatasetCached datasetCached = new DatasetCached(dataset, id);
        if (!getDataFile(id).exists()) {
            cacheFile(cache, datasetCached);
        }
        return new DatasetCached(dataset, id, getDataFile(id));
    }

    private static void cacheFile(File dataFile, Dataset dataset) throws IOException {
        File datasetPath = getDatasetDir(dataset.getId());
        FileUtils.forceMkdir(datasetPath);
        File destFile = getDataFile(dataset.getId());
        FileUtils.moveFile(dataFile, destFile);
        FileUtils.copyToFile(IOUtils.toInputStream(dataset.getId(), StandardCharsets.UTF_8), new File(datasetPath, "data.sha256"));
        ObjectNode node = new ObjectMapper().createObjectNode();
        node.put("id", dataset.getId());
        if (null != dataset.getParent()) {
            node.put("parentId", dataset.getParent().getId());
        }
        node.put("type", dataset.getType().toString());
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
