package org.globalbioticinteractions.preston;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CrawlerGBIF implements Crawler {

    private final Log LOG = LogFactory.getLog(CrawlerGBIF.class);

    public static final Map<String, DatasetType> TYPE_MAP = new HashMap<String, DatasetType>() {{
        put("DWC_ARCHIVE", DatasetType.DWCA);
        put("EML", DatasetType.EML);
    }};

    public static boolean parse(InputStream resourceAsStream, DatasetListener listener, Dataset parent) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(resourceAsStream);
        if (jsonNode != null && jsonNode.has("results")) {
            for (JsonNode result : jsonNode.get("results")) {
                if (result.has("key") && result.has("endpoints")) {
                    String uuid = result.get("key").asText();
                    listener.onDataset(new DatasetString(parent, DatasetType.UUID, uuid));
                    for (JsonNode endpoint : result.get("endpoints")) {
                        if (endpoint.has("url") && endpoint.has("type")) {
                            String urlString = endpoint.get("url").asText();
                            URI url = URI.create(urlString);
                            String type = endpoint.get("type").asText();
                            DatasetType datasetType = TYPE_MAP.get(type);
                            if (datasetType != null) {
                                listener.onDataset(new DatasetString(parent, DatasetType.URI, urlString));
                                listener.onDataset(new DatasetURI(parent, datasetType, url));
                            }
                        }
                    }
                }
            }
        }
        return !jsonNode.has("endOfRecords") || jsonNode.get("endOfRecords").asBoolean(true);
    }


    public static String calcSHA256(String str) throws IOException {
        return calcSHA256(IOUtils.toInputStream(str, StandardCharsets.UTF_8), new NullOutputStream());
    }

    public static String calcSHA256(InputStream is, OutputStream os) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            DigestInputStream digestInputStream = new DigestInputStream(is, md);
            IOUtils.copy(digestInputStream, os);
            digestInputStream.close();
            return String.format("%064x", new BigInteger(1, md.digest()));
        } catch (IOException | NoSuchAlgorithmException var9) {
            throw new IOException("failed to cache dataset", var9);
        }
    }

    public static String toPath(String hash) {
        String u0 = hash.substring(0, 2);
        String u1 = hash.substring(2, 4);
        String u2 = hash.substring(4, 6);
        return StringUtils.join(Arrays.asList(u0, u1, u2, hash), "/");
    }

    @Override
    public void crawl(DatasetListener listener) throws IOException {
        int offset = 0;
        int limit = 50;
        boolean endOfRecords = false;
        Dataset previousPage = null;

        while (!endOfRecords) {
            Dataset currentPageURI = nextPage(previousPage, offset, limit, listener);
            endOfRecords = crawlPage(listener, currentPageURI);
            offset = offset + limit;
            previousPage = currentPageURI;
        }
    }

    private Dataset nextPage(Dataset previousPage, int offset, int limit, DatasetListener listener) {
        String uri = "https://api.gbif.org/v1/dataset?offset=" + offset + "&limit=" + limit;
        Dataset currentPageURI = new DatasetString(previousPage, DatasetType.URI, uri);
        listener.onDataset(currentPageURI);
        DatasetURI datasetURI = new DatasetURI(currentPageURI, DatasetType.GBIF_DATASETS_JSON, URI.create(uri));
        listener.onDataset(datasetURI);
        return datasetURI;
    }

    protected boolean crawlPage(DatasetListener listener, Dataset dataset) throws IOException {
        boolean endOfRecords = true;
        if (dataset.getType() == DatasetType.GBIF_DATASETS_JSON) {
            ParserGBIF parser = new ParserGBIF(listener, dataset);
            parser.handle(dataset.getData());
            endOfRecords = parser.reachedEndOfRecords();
        }
        return endOfRecords;
    }

}