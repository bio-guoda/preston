package bio.guoda.preston;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;


public class RateLimitedRetryStrategyIT {

    @Test
    public void testZenodoRateLimitingNoClientSideThrottling() throws IOException, InterruptedException {
        CloseableHttpClient client = HttpClientBuilder.create().build();

        CloseableHttpResponse resp;
        Map<String, Long> rateLimits = null;

        exhaustRateLimit(client, rateLimits, zenodoUrl());

        resp = makeRequest(client, zenodoUrl());
        EntityUtils.consume(resp.getEntity());
        assertThat(resp.getStatusLine().getStatusCode(), is(429));
    }

    @Test
    public void testPensoftRateLimitingNoClientSideThrottling() throws IOException, InterruptedException {
        CloseableHttpClient client = HttpClientBuilder.create().build();

        CloseableHttpResponse resp;
        Map<String, Long> rateLimits = null;

        exhaustRateLimit(client, rateLimits, zenodoUrl());

        resp = makeRequest(client, pensoftUrl());
        EntityUtils.consume(resp.getEntity());
        assertThat(resp.getStatusLine().getStatusCode(), is(429));
    }

    private String pensoftUrl() {
        return "https://bdj.pensoft.net/lib/ajax_srv/archive_download.php?archive_type=2&document_id=6313";
    }

    private CloseableHttpResponse makeRequest(CloseableHttpClient client, String uri) throws IOException {
        return client.execute(new HttpGet(uri));
    }


    @Test
    public void withClientSideThrottling() throws IOException {
        CloseableHttpClient client = HttpClientBuilder
                .create()
                .setServiceUnavailableRetryStrategy(new RateLimitedRetryStrategy())
                .build();

        CloseableHttpResponse resp;
        Map<String, Long> rateLimits = null;

        exhaustRateLimit(client, rateLimits, zenodoUrl());

        resp = makeRequest(client, zenodoUrl());
        EntityUtils.consume(resp.getEntity());
        assertThat(resp.getStatusLine().getStatusCode(), is(not(RateLimitUtils.HTTP_STATUS_CODE_TOO_MANY_REQUESTS)));
    }

    private String zenodoUrl() {
        return "https://zenodo.org/api/records/?q=_files.checksum:/eb5e8f37583644943b86d1d9ebd4ded5";
    }

    @Test
    public void pensoftWithClientSideThrottling() throws IOException {
        CloseableHttpClient client = HttpClientBuilder
                .create()
                .setServiceUnavailableRetryStrategy(new RateLimitedRetryStrategy())
                .build();

        CloseableHttpResponse resp;
        Map<String, Long> rateLimits = null;

        repeatRequestUntilNon200Code(client, rateLimits, pensoftUrl());

        resp = makeRequest(client, pensoftUrl());
        EntityUtils.consume(resp.getEntity());
        assertThat(resp.getStatusLine().getStatusCode(), is(RateLimitUtils.HTTP_STATUS_CODE_TOO_MANY_REQUESTS));
    }

    private void exhaustRateLimit(CloseableHttpClient client, Map<String, Long> rateLimits, String uri) throws IOException {
        CloseableHttpResponse resp;
        while (rateLimits == null || (rateLimits.containsKey(RateLimitUtils.X_RATE_LIMIT_REMAINING) && rateLimits.get(RateLimitUtils.X_RATE_LIMIT_REMAINING) > 0)) {
            resp = makeRequest(client, uri);
            EntityUtils.consume(resp.getEntity());
            rateLimits = RateLimitUtils.parseRateLimits(resp);
            System.out.println(rateLimits.get(RateLimitUtils.X_RATE_LIMIT_REMAINING));
        }

        assertThat(rateLimits.get(RateLimitUtils.X_RATE_LIMIT_REMAINING), is(0L));
    }


    private void repeatRequestUntilNon200Code(CloseableHttpClient client, Map<String, Long> rateLimits, String uri) throws IOException {
        CloseableHttpResponse resp;
        int statusCode = 200;
        int retryCount = 0;
        while (statusCode == 200) {
            resp = makeRequest(client, uri);
            statusCode = resp.getStatusLine().getStatusCode();
            EntityUtils.consume(resp.getEntity());
            System.out.println("retry [" + retryCount + "] for [" + uri + "] with code [" + statusCode + "]");
            retryCount++;
        }

        assertThat(statusCode, is(429));
    }

}