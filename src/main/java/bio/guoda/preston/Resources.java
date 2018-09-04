package bio.guoda.preston;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
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

public class Resources {
    private static CloseableHttpClient httpClient = null;

    public static InputStream asInputStreamOfflineOnly(IRI dataIRI) throws IOException {
        InputStream is = null;
        URI uri = URI.create(dataIRI.getIRIString());
        if (StringUtils.equals("file", uri.getScheme())) {
            is = uri.toURL().openStream();
        }
        return is;
    }

    public static InputStream asInputStream(IRI dataURI) throws IOException {
        InputStream is = asInputStreamOfflineOnly(dataURI);
        if (is == null) {
            HttpGet get = new HttpGet(URI.create(dataURI.getIRIString()));
            get.setHeader(HttpHeaders.ACCEPT_ENCODING, "gzip");

            CloseableHttpResponse response = getHttpClient().execute(get);

            StatusLine statusLine = response.getStatusLine();
            HttpEntity entity = response.getEntity();
            if (statusLine.getStatusCode() >= 300) {
                EntityUtils.consume(entity);
                throw new HttpResponseException(statusLine.getStatusCode(), statusLine.getReasonPhrase());
            }
            is = entity.getContent();
        }
        return is;
    }

    private static CloseableHttpClient getHttpClient() {
        return httpClient == null ? initClient() : httpClient;
    }

    private static CloseableHttpClient initClient() {
        int soTimeoutMs = 30 * 1000;
        RequestConfig config = RequestConfig.custom().setSocketTimeout(soTimeoutMs).setConnectTimeout(soTimeoutMs).build();
        return HttpClientBuilder.create().setRetryHandler(new DefaultHttpRequestRetryHandler(3, true)).setUserAgent("globalbioticinteractions/" + Preston.getVersion() + " (https://globalbioticinteractions.org; mailto:info@globalbioticinteractions.org)").setDefaultRequestConfig(config).build();
    }
}
