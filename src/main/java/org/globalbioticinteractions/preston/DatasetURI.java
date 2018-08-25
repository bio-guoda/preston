package org.globalbioticinteractions.preston;

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
import java.net.URI;

public class DatasetURI extends DatasetImpl {
    private static CloseableHttpClient httpClient = null;
    private final URI dataURI;

    public DatasetURI(Dataset parent, DatasetType type, URI uri) {
        super(parent, type);
        this.dataURI = uri;
    }

    @Override
    public InputStream getData() throws IOException {
        HttpGet get = new HttpGet(dataURI);
        get.setHeader("Accept", "application/json;charset=UTF-8");
        get.setHeader("Content-Type", "application/json;charset=UTF-8");
        get.setHeader(HttpHeaders.ACCEPT_ENCODING, "gzip");

        CloseableHttpResponse response = getHttpClient().execute(get);

        StatusLine statusLine = response.getStatusLine();
        HttpEntity entity = response.getEntity();
        if (statusLine.getStatusCode() >= 300) {
            EntityUtils.consume(entity);
            throw new HttpResponseException(statusLine.getStatusCode(), statusLine.getReasonPhrase());
        }
        return entity.getContent();
    }

    private CloseableHttpClient getHttpClient() {
        return httpClient == null ? initClient() : httpClient;
    }

    private static CloseableHttpClient initClient() {
        int soTimeoutMs = 300000;
        RequestConfig config = RequestConfig.custom().setSocketTimeout(soTimeoutMs).setConnectTimeout(soTimeoutMs).build();
        return HttpClientBuilder.create().setRetryHandler(new DefaultHttpRequestRetryHandler(3, true)).setUserAgent("globalbioticinteractions/" + Preston.getVersion() + " (https://globalbioticinteractions.org; mailto:info@globalbioticinteractions.org)").setDefaultRequestConfig(config).build();
    }

    @Override
    public String getLabel() {
        return "data@" + dataURI.toString();
    }

}
