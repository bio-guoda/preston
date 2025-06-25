package bio.guoda.preston;

import bio.guoda.preston.stream.ContentStreamUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.http.HttpEntity;
import org.apache.http.HttpMessage;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.config.CookieSpecs;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class ResourcesHTTP {
    private static final Logger LOG = LoggerFactory.getLogger(ResourcesHTTP.class);

    public static final String ZENODO_AUTH_TOKEN = "ZENODO_TOKEN";
    public static final String ZOTERO_AUTH_TOKEN = "ZOTERO_TOKEN";
    public static final String GITHUB_AUTH_TOKEN = "GITHUB_TOKEN";

    private static final List<Integer> REDIRECT_CODES = Arrays.asList(
            HttpStatus.SC_MOVED_PERMANENTLY,
            HttpStatus.SC_MOVED_TEMPORARILY,
            HttpStatus.SC_SEE_OTHER
    );

    public static final String MIMETYPE_GITHUB_JSON = "application/vnd.github+json";

    public static final Predicate<Integer> SHOULD_IGNORE_40x_50x
            = statusCode -> statusCode >= 400;

    public static final Predicate<Integer> NEVER_IGNORE = new Predicate<Integer>() {
        @Override
        public boolean test(Integer integer) {
            return false;
        }
    };

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

    public static InputStream asInputStreamIgnore40x50x(IRI dataURI, DerefProgressListener derefProgressListener) throws IOException {
        return asInputStream(dataURI, derefProgressListener, SHOULD_IGNORE_40x_50x);
    }

    public static InputStream asInputStream(IRI dataURI, DerefProgressListener progressListener) throws IOException {
        return asInputStream(dataURI, progressListener, NEVER_IGNORE);
    }

    public static InputStream asInputStream(IRI dataURI) throws IOException {
        return asInputStream(dataURI, ContentStreamUtil.getNOOPDerefProgressListener());
    }


    public static InputStream asInputStream(IRI dataURI,
                                            DerefProgressListener listener,
                                            Predicate<Integer> shouldIgnore) throws IOException {
        HttpGet get = new HttpGet(URI.create(dataURI.getIRIString()));
        get.addHeader("Accept", "*/*");
        return asInputStream(dataURI, get, listener, shouldIgnore);
    }

    private static void injectAuthorizationIfPossible(IRI dataURI, HttpMessage msg) {
        if (StringUtils.startsWith(dataURI.getIRIString(), "https://ghcr.io")) {
            msg.addHeader("Authorization", "Bearer QQ==");
        } else if (StringUtils.startsWith(dataURI.getIRIString(), "https://api.github.com/") || StringUtils.startsWith(dataURI.getIRIString(), "https://github.com/")) {
            msg.addHeader("Accept", MIMETYPE_GITHUB_JSON);
            appendGitHubAuthTokenIfAvailable(msg);
        } else if (StringUtils.startsWith(dataURI.getIRIString(), "https://api.zotero.org/")) {
            appendAuthBearerUsingEnvironmentVariableIfAvailable(msg, ZOTERO_AUTH_TOKEN);
        } else if (StringUtils.startsWith(dataURI.getIRIString(), "https://zenodo.org/api")
                || StringUtils.startsWith(dataURI.getIRIString(), "https://sandbox.zenodo.org/api")) {
            appendAuthBearerUsingEnvironmentVariableIfAvailable(msg, ZENODO_AUTH_TOKEN);
        }
    }

    private static void appendGitHubAuthTokenIfAvailable(HttpMessage msg) {
        String githubToken = EnvUtil.getEnvironmentVariable(GITHUB_AUTH_TOKEN);
        if (StringUtils.isNotBlank(githubToken)) {
            msg.addHeader("Authorization", "token " + githubToken);
        }
    }

    private static void appendAuthBearerUsingEnvironmentVariableIfAvailable(HttpMessage msg, String authTokenEnvironmentVariableName) {
        String authToken = EnvUtil.getEnvironmentVariable(authTokenEnvironmentVariableName);
        setAuthBearerIfAvailable(msg, authToken);
    }

    public static void setAuthBearerIfAvailable(HttpMessage msg, String authToken) {
        if (StringUtils.isNotBlank(authToken)) {
            msg.removeHeaders("Authorization");
            msg.addHeader("Authorization", "Bearer " + authToken);
        }
    }

    public static InputStream asInputStream(IRI dataURI,
                                            HttpUriRequest request,
                                            DerefProgressListener listener,
                                            Predicate<Integer> shouldIgnore) throws IOException {
        InputStream is = asInputStreamOfflineOnly(dataURI);
        if (is == null) {

            CloseableHttpClient client = shouldRedirect(dataURI)
                    ? getRedirectingHttpClient()
                    : getHttpClient();

            injectAuthorizationIfPossible(dataURI, request);

            CloseableHttpResponse response = client.execute(request);
            StatusLine statusLine = response.getStatusLine();
            HttpEntity entity = response.getEntity();
            if (shouldIgnore.test(statusLine.getStatusCode())) {
                EntityUtils.consume(entity);
            } else {
                if (new ShouldThrowOn(dataURI).test(statusLine.getStatusCode())) {
                    StringBuilder builder = new StringBuilder();
                    builder.append("[").append(dataURI).append("] with reason [").append(statusLine.getReasonPhrase()).append("]");
                    if (entity != null && entity.getContentLength() < 1024 * 1024) {
                        builder.append(" and possible error message [").append(IOUtils.toString(entity.getContent(), StandardCharsets.UTF_8)).append("]");
                    }
                    EntityUtils.consumeQuietly(entity);
                    throw new HttpResponseException(statusLine.getStatusCode(), builder.toString());
                }

                return entity == null
                        ? new ByteArrayInputStream("".getBytes())
                        : getInputStream(dataURI, listener, entity);
            }
        }
        return is;
    }

    private static InputStream getInputStream(IRI dataURI, DerefProgressListener listener, HttpEntity entity) throws IOException {
        final long contentLength = entity.getContentLength();

        InputStream contentStream = entity.getContent();

        return ContentStreamUtil.getInputStreamWithProgressLogger(dataURI, listener, contentLength, contentStream);
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
                .setCookieSpec(CookieSpecs.STANDARD)
                .build();

        try {
            SSLContext build = new SSLContextBuilder()
                    .loadTrustMaterial(null, (TrustStrategy) (arg0, arg1) -> true)
                    .build();

            return HttpClientBuilder
                    .create()
                    // see https://github.com/bio-guoda/preston/issues/249
                    .disableContentCompression()
                    // see https://github.com/bio-guoda/preston/issues/25
                    .setSSLHostnameVerifier(new NoopHostnameVerifier())
                    .setSSLContext(build)
                    .setServiceUnavailableRetryStrategy(new RateLimitedRetryStrategy())
                    .setRetryHandler(new DefaultHttpRequestRetryHandler(3, true))
                    .setUserAgent(getUserAgent(VersionUtil.getVersionString()))
                    .setDefaultRequestConfig(config)
                    // for loading proxy config see https://github.com/globalbioticinteractions/nomer/issues/121
                    .useSystemProperties()
                    .build();
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            throw new IllegalStateException("unexpected ssl exception", e);
        }
    }

    static String getUserAgent(String versionString) {
        return "globalbioticinteractions/" + versionString + " (https://globalbioticinteractions.org; mailto:info@globalbioticinteractions.org)";
    }

    private static RequestConfig.Builder defaultConfig() {
        int soTimeoutMs = 300 * 1000; // 5 minutes
        return RequestConfig
                .custom()
                .setNormalizeUri(false)
                .setSocketTimeout(soTimeoutMs)
                .setConnectTimeout(soTimeoutMs);
    }

    public static class ShouldThrowOn implements Predicate<Integer> {

        public static final Pattern SCIELO_403_SOFT_REDIRECT = Pattern.compile("http[s]?://(www.)?scielo.cl/scielo.php\\?script=sci_pdf.*");
        private final IRI dataURI;

        public ShouldThrowOn(IRI dataURI) {
            this.dataURI = dataURI;
        }

        @Override
        public boolean test(Integer statusCode) {
            boolean shouldThrow;
            if (statusCode == HttpStatus.SC_FORBIDDEN
                    && SCIELO_403_SOFT_REDIRECT.matcher(dataURI.getIRIString()).matches()) {
                shouldThrow = false;
            } else {
                shouldThrow = statusCode >= 300 && (shouldRedirect(dataURI) || !REDIRECT_CODES.contains(statusCode));
            }
            return shouldThrow;
        }
    }
}
