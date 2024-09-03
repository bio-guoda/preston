package bio.guoda.preston;

import org.apache.http.HttpResponse;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.impl.client.DefaultServiceUnavailableRetryStrategy;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

public class RateLimitedRetryStrategy implements ServiceUnavailableRetryStrategy {
    private final static Logger LOG = LoggerFactory.getLogger(RateLimitedRetryStrategy.class);

    private AtomicReference<Duration> retryDurationSeconds = new AtomicReference<>(Duration.ofSeconds(1L));

    @Override
    public boolean retryRequest(HttpResponse response, int executionCount, HttpContext context) {
        boolean tooManyRequests = response.getStatusLine().getStatusCode() == RateLimitUtils.HTTP_STATUS_CODE_TOO_MANY_REQUESTS;

        boolean shouldRetry = tooManyRequests
                || new DefaultServiceUnavailableRetryStrategy().retryRequest(response, executionCount, context);
        if (shouldRetry) {
            Duration duration = RateLimitUtils.retryAfter(response, Duration.ofSeconds(1));
            retryDurationSeconds.set(duration);
            if (tooManyRequests) {
                LOG.warn("server signalled [429: too many requests]: retrying request after waiting for [" + duration.getSeconds() + "]s");
            }
        }
        return shouldRetry;
    }

    @Override
    public long getRetryInterval() {
        return retryDurationSeconds.get().getSeconds() * 1000;
    }

}
