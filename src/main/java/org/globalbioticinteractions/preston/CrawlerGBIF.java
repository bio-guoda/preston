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

    public static final Map<String, DatasetType> TYPE_MAP = new HashMap<String, DatasetType>() {{
        put("DWC_ARCHIVE", DatasetType.DWCA);
        put("EML", DatasetType.EML);
    }};

    @Override
    public void crawl(DatasetListener listener) throws IOException {
        nextPage(null, 0, 2, listener);
    }

    private static Dataset nextPage(Dataset previousPage, int offset, int limit, DatasetListener listener) {
        String uri = "https://api.gbif.org/v1/dataset?offset=" + offset + "&limit=" + limit;
        Dataset currentPageURI = new DatasetString(previousPage, DatasetType.URI, uri);
        listener.onDataset(currentPageURI);
        DatasetURI datasetURI = new DatasetURI(currentPageURI, DatasetType.GBIF_DATASETS_JSON, URI.create(uri));
        listener.onDataset(datasetURI);
        return datasetURI;
    }

    public static void parse(InputStream resourceAsStream, DatasetListener listener, Dataset parent) throws IOException {
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

        boolean endOfRecords = !jsonNode.has("endOfRecords") || jsonNode.get("endOfRecords").asBoolean(true);
        if (!endOfRecords && jsonNode.has("offset") && jsonNode.has("limit")) {
            int offset = jsonNode.get("offset").asInt();
            int limit = jsonNode.get("limit").asInt();
            nextPage(parent, offset + limit, limit, listener);
        }

    }


}