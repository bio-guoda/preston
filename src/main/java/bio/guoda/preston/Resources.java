package bio.guoda.preston;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
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
import java.util.Arrays;
import java.util.List;

public class Resources {
    public static final List<Integer> REDIRECT_CODES = Arrays.asList(301, 302, 303);
    private static CloseableHttpClient httpClient = null;
    private static CloseableHttpClient redirectingHttpClient = null;

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

            CloseableHttpClient client = shouldRedirect(dataURI)
                    ? getRedirectingHttpClient()
                    : getHttpClient();

            CloseableHttpResponse response = client.execute(get);

            StatusLine statusLine = response.getStatusLine();
            HttpEntity entity = response.getEntity();
            if (statusLine.getStatusCode() >= 300) {
                if (shouldRedirect(dataURI) || !REDIRECT_CODES.contains(statusLine.getStatusCode())) {
                    EntityUtils.consume(entity);
                    throw new HttpResponseException(statusLine.getStatusCode(), statusLine.getReasonPhrase());
                }
            }
            is = entity.getContent();
        }
        return is;
    }

    private static boolean shouldRedirect(IRI dataURI) {
        return !dataURI.toString().contains("https://cn.dataone.org/cn/");
    }

    private static CloseableHttpClient getRedirectingHttpClient() {
        return redirectingHttpClient == null ? initRedirectingClient() : redirectingHttpClient;
    }

    private static CloseableHttpClient getHttpClient() {
        return httpClient == null ? initClient() : httpClient;
    }

    private static CloseableHttpClient initRedirectingClient() {
        RequestConfig.Builder builder = defaultConfig();
        return initClient(builder);
    }

    private static CloseableHttpClient initClient() {
        RequestConfig.Builder builder = defaultConfig();
        builder.setRedirectsEnabled(false);
        return initClient(builder);
    }

    private static CloseableHttpClient initClient(RequestConfig.Builder builder) {
        RequestConfig config = builder
                .build();
        return HttpClientBuilder.create().setRetryHandler(new DefaultHttpRequestRetryHandler(3, true)).setUserAgent("globalbioticinteractions/" + Preston.getVersion() + " (https://globalbioticinteractions.org; mailto:info@globalbioticinteractions.org)").setDefaultRequestConfig(config).build();
    }

    private static RequestConfig.Builder defaultConfig() {
        int soTimeoutMs = 30 * 1000;
        return RequestConfig.custom().setSocketTimeout(soTimeoutMs).setConnectTimeout(soTimeoutMs);
    }
}
