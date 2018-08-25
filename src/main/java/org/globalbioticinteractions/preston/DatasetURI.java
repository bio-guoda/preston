package org.globalbioticinteractions.preston;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class DatasetURI extends Dataset {
    private final URI dataURI;

    public DatasetURI(Dataset parent, DatasetType type, URI uri) {
        super(parent, type);
        this.dataURI = uri;
    }

    @Override
    public InputStream getData() throws IOException {
        return StringUtils.isNotBlank(getId()) && getDataFile().exists()
                ? FileUtils.openInputStream(getDataFile())
                : cacheAndStream();
    }

    private InputStream cacheAndStream() throws IOException {
        int soTimeoutMs = 300000;
        RequestConfig config = RequestConfig.custom().setSocketTimeout(soTimeoutMs).setConnectTimeout(soTimeoutMs).build();
        CloseableHttpClient client = HttpClientBuilder.create().setRetryHandler(new DefaultHttpRequestRetryHandler(3, true)).setUserAgent("globalbioticinteractions/" + Preston.getVersion() + " (https://globalbioticinteractions.org; mailto:info@globalbioticinteractions.org)").setDefaultRequestConfig(config).build();
        try {
            HttpGet get = new HttpGet(dataURI);
            get.setHeader("Accept", "application/json;charset=UTF-8");
            get.setHeader("Content-Type", "application/json;charset=UTF-8");
            get.setHeader(HttpHeaders.ACCEPT_ENCODING, "gzip");

            CloseableHttpResponse response = client.execute(get);

            StatusLine statusLine = response.getStatusLine();
            HttpEntity entity = response.getEntity();
            if (statusLine.getStatusCode() >= 300) {
                EntityUtils.consume(entity);
                throw new HttpResponseException(statusLine.getStatusCode(), statusLine.getReasonPhrase());
            }
            File cacheDir = new File("cacheDir");
            FileUtils.forceMkdir(cacheDir);
            File cache = File.createTempFile("cacheFile", ".tmp", cacheDir);
            setId(CrawlerGBIF.calcSHA256(entity.getContent(), new FileOutputStream(cache)));
            return FileUtils.openInputStream(getDataFile().exists() ? getDataFile() : cacheFile(cache));
        } finally {
            client.close();
        }
    }

    private File cacheFile(File dataFile) throws IOException {
        File datasetPath = getDatasetDir();
        FileUtils.forceMkdir(datasetPath);
        File destFile = getDataFile();
        FileUtils.moveFile(dataFile, destFile);
        FileUtils.copyToFile(IOUtils.toInputStream(getId(), StandardCharsets.UTF_8), new File(datasetPath, "data.sha256"));
        return destFile;
    }

    private File getDataFile() {
        return new File(getDatasetDir(), "data");
    }

    private File getDatasetDir() {
        return new File("datasets/" + CrawlerGBIF.toPath(getId()));
    }

    @Override
    public String getLabel() {
        return "data@" + dataURI.toString();
    }

}
