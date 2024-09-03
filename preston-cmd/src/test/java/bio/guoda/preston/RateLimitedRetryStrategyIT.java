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

        exhaustRateLimit(client, rateLimits);

        resp = makeRequest(client);
        EntityUtils.consume(resp.getEntity());
        assertThat(resp.getStatusLine().getStatusCode(), is(429));
    }

    private CloseableHttpResponse makeRequest(CloseableHttpClient client) throws IOException {
        return client.execute(new HttpGet("https://zenodo.org/api/records/?q=_files.checksum:/eb5e8f37583644943b86d1d9ebd4ded5"));
    }


    @Test
    public void withClientSideThrottling() throws IOException {
        CloseableHttpClient client = HttpClientBuilder
                .create()
                .setServiceUnavailableRetryStrategy(new RateLimitedRetryStrategy())
                .build();

        CloseableHttpResponse resp;
        Map<String, Long> rateLimits = null;

        exhaustRateLimit(client, rateLimits);

        resp = makeRequest(client);
        EntityUtils.consume(resp.getEntity());
        assertThat(resp.getStatusLine().getStatusCode(), is(not(RateLimitUtils.HTTP_STATUS_CODE_TOO_MANY_REQUESTS)));
    }

    private void exhaustRateLimit(CloseableHttpClient client, Map<String, Long> rateLimits) throws IOException {
        CloseableHttpResponse resp;
        while (rateLimits == null || rateLimits.get(RateLimitUtils.X_RATE_LIMIT_REMAINING) > 0) {
            resp = makeRequest(client);
            EntityUtils.consume(resp.getEntity());
            rateLimits = RateLimitUtils.parseRateLimits(resp);
            System.out.println(rateLimits.get(RateLimitUtils.X_RATE_LIMIT_REMAINING));
        }

        assertThat(rateLimits.get(RateLimitUtils.X_RATE_LIMIT_REMAINING), is(0L));
    }

}