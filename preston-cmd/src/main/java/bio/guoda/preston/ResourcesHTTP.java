package bio.guoda.preston;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.Version;
import bio.guoda.preston.stream.ContentStreamUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ResourcesHTTP {
    private static final List<Integer> REDIRECT_CODES = Arrays.asList(301, 302, 303);
    private static CloseableHttpClient httpClient = null;
    private static CloseableHttpClient redirectingHttpClient = null;

    public static InputStream asInputStreamOfflineOnly(IRI dataIRI) throws IOException {
        InputStream is = null;
        URI uri = URI.create(dataIRI.getIRIString());
        if (StringUtils.equals("file", uri.getScheme())
                && new File(uri).exists()) {
            is = uri.toURL().openStream();
        }
        return is;
    }

    public static InputStream asInputStreamIgnore404(IRI dataURI, DerefProgressListener derefProgressListener) throws IOException {
        return asInputStream(dataURI, Arrays.asList(400, 404), derefProgressListener);
    }

    public static InputStream asInputStream(IRI dataURI, DerefProgressListener nullDerefProgressListener) throws IOException {
        return asInputStream(dataURI, Collections.emptyList(), nullDerefProgressListener);
    }

    public static InputStream asInputStream(IRI dataURI) throws IOException {
        return asInputStream(dataURI, ContentStreamUtil.getNOOPDerefProgressListener());
    }


    public static InputStream asInputStream(IRI dataURI,
                                            List<Integer> ignoreCodes,
                                            DerefProgressListener listener) throws IOException {
        HttpGet get = new HttpGet(URI.create(dataURI.getIRIString()));
        get.setHeader(HttpHeaders.ACCEPT_ENCODING, "gzip");
        return asInputStream(dataURI, get, ignoreCodes, listener);
    }

    public static InputStream asInputStream(IRI dataURI,
                                            HttpUriRequest request,
                                            List<Integer> ignoreCodes,
                                            DerefProgressListener listener) throws IOException {
        InputStream is = asInputStreamOfflineOnly(dataURI);
        if (is == null) {

            CloseableHttpClient client = shouldRedirect(dataURI)
                    ? getRedirectingHttpClient()
                    : getHttpClient();

            CloseableHttpResponse response = client.execute(request);

            StatusLine statusLine = response.getStatusLine();
            HttpEntity entity = response.getEntity();
            if (ignoreCodes.contains(statusLine.getStatusCode())) {
                EntityUtils.consume(entity);
            } else {
                if (statusLine.getStatusCode() >= 300) {
                    if (shouldRedirect(dataURI) || !REDIRECT_CODES.contains(statusLine.getStatusCode())) {
                        EntityUtils.consume(entity);
                        throw new HttpResponseException(statusLine.getStatusCode(), "[" + dataURI + "]" + statusLine.getReasonPhrase());
                    }
                }

                final long contentLength = entity.getContentLength();

                InputStream contentStream = entity.getContent();

                return ContentStreamUtil.getInputStreamWithProgressLogger(dataURI, listener, contentLength, contentStream);
            }
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

        try {
            SSLContext build = new SSLContextBuilder()
                    .loadTrustMaterial(null, (TrustStrategy) (arg0, arg1) -> true)
                    .build();

            return HttpClientBuilder
                    .create()
                    .setSSLHostnameVerifier(new NoopHostnameVerifier()) // see https://github.com/bio-guoda/preston/issues/25
                    .setSSLContext(build)
                    .setRetryHandler(new DefaultHttpRequestRetryHandler(3, true))
                    .setUserAgent("globalbioticinteractions/" + Version.getVersion() + " (https://globalbioticinteractions.org; mailto:info@globalbioticinteractions.org)")
                    .setDefaultRequestConfig(config)
                    .build();
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            throw new IllegalStateException("unexpected ssl exception", e);
        }
    }

    private static RequestConfig.Builder defaultConfig() {
        int soTimeoutMs = 30 * 1000;
        return RequestConfig
                .custom()
                .setSocketTimeout(soTimeoutMs)
                .setConnectTimeout(soTimeoutMs);
    }
}
