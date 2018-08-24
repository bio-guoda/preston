package org.globalbioticinteractions.preston;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CrawlerGBIF implements Crawler {

    public static final Map<String, DatasetType> TYPE_MAP = new HashMap<String, DatasetType>() {{
        put("DWC_ARCHIVE", DatasetType.DARWIN_CORE_ARCHIVE);
        put("EML", DatasetType.EML);
    }};

    public static boolean parse(InputStream resourceAsStream, DatasetListener listener) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(resourceAsStream);
        if (jsonNode != null && jsonNode.has("results")) {
            for (JsonNode result : jsonNode.get("results")) {
                if (result.has("key") && result.has("endpoints")) {

                    String uuid = result.get("key").asText();
                    for (JsonNode endpoint : result.get("endpoints")) {
                        if (endpoint.has("url") && endpoint.has("type")) {
                            String urlString = endpoint.get("url").asText();
                            URI url = URI.create(urlString);
                            String type = endpoint.get("type").asText();
                            DatasetType datasetType = TYPE_MAP.get(type);
                            if (datasetType != null) {
                                listener.onDataset(new Dataset(UUID.fromString(uuid), url, datasetType));
                            }
                        }
                    }
                }
            }
        }
        return !jsonNode.has("endOfRecords") || jsonNode.get("endOfRecords").asBoolean(true);
    }

    @Override
    public void crawl(DatasetListener listener)
            throws IOException {
        int offset = 0;
        int limit = 50;
        boolean endOfRecords = false;

        while (!endOfRecords) {
            endOfRecords = crawlPage(listener, offset, limit);
            offset = offset + limit;

        }
    }

    protected boolean crawlPage(DatasetListener listener, int offset, int limit) throws IOException {
        boolean endOfRecords;
        int soTimeoutMs = 300000;
        RequestConfig config = RequestConfig.custom().setSocketTimeout(soTimeoutMs).setConnectTimeout(soTimeoutMs).build();
        CloseableHttpClient client = HttpClientBuilder.create().setRetryHandler(new DefaultHttpRequestRetryHandler(3, true)).setUserAgent("globalbioticinteractions/" + Preston.getVersion() + " (https://globalbioticinteractions.org; mailto:info@globalbioticinteractions.org)").setDefaultRequestConfig(config).build();
        try {
            HttpGet get = new HttpGet("https://api.gbif.org/v1/dataset?offset=" + offset + "&limit=" + limit);
            get.setHeader("Accept", "application/json;charset=UTF-8");
            get.setHeader("Content-Type", "application/json;charset=UTF-8");
            get.setHeader(HttpHeaders.ACCEPT_ENCODING, "gzip");

            CloseableHttpResponse response = client.execute(get);

            StatusLine statusLine = response.getStatusLine();
            HttpEntity entity = response.getEntity();
            if (statusLine.getStatusCode() >= 300) {
                EntityUtils.consume(entity);
                throw new HttpResponseException(statusLine.getStatusCode(), statusLine.getReasonPhrase());
            } else {
                endOfRecords = parse(entity.getContent(), listener);
            }
        } finally {
            client.close();
        }
        return endOfRecords;
    }
}