package bio.guoda.preston;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.stream.IntStreams;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class RateLimitedRetryStrategy implements ServiceUnavailableRetryStrategy {
    private final static Logger LOG = LoggerFactory.getLogger(RateLimitedRetryStrategy.class);
    public static final int RETRY_INTERVAL_NO_HINT_SECONDS = 30;
    public static final List<Integer> SERVER_ERRORS = Arrays.asList(HttpStatus.SC_BAD_GATEWAY, HttpStatus.SC_GATEWAY_TIMEOUT, HttpStatus.SC_SERVICE_UNAVAILABLE);

    private AtomicReference<Duration> retryDurationSeconds = new AtomicReference<>(Duration.ofSeconds(RETRY_INTERVAL_NO_HINT_SECONDS));
    private int maxRetries = 3;

    @Override
    public boolean retryRequest(HttpResponse response, int executionCount, HttpContext context) {
        StatusLine statusLine = response.getStatusLine();

        boolean shouldRetry = false;
        if (replyIndicatesTooManyRequests(statusLine)) {
            shouldRetry = retryForTooManyRequests(response, executionCount, context);
        } else if (replyIndicatesServerError(statusLine)
                && (executionCount < maxRetries)) {
            int exponentialDelay = calculateExponentialDelayFor(executionCount);
            Duration duration = RateLimitUtils.retryAfter(response, Duration.ofSeconds(exponentialDelay));
            retryDurationSeconds.set(duration);
            LOG.warn("server error [" + statusLine.getStatusCode() + ": " + statusLine.getReasonPhrase() + "]: retrying request for [" + context.getAttribute("http.request") + "] after waiting for [" + duration.getSeconds() + "]s following server provided rate limits [" + RateLimitUtils.parseRateLimits(response) + "]");
            shouldRetry = true;
        }

        return shouldRetry;
    }

    static int calculateExponentialDelayFor(int executionCount) {
        return IntStreams.range(executionCount)
                .map(x -> 10)
                .reduce(RETRY_INTERVAL_NO_HINT_SECONDS, (acc, i) -> acc * i);
    }

    private boolean replyIndicatesServerError(StatusLine statusLine) {
        return SERVER_ERRORS.contains(statusLine.getStatusCode());
    }

    private static boolean replyIndicatesTooManyRequests(StatusLine statusLine) {
        boolean tooManyRequests = statusLine.getStatusCode() == RateLimitUtils.HTTP_STATUS_CODE_TOO_MANY_REQUESTS;

        if (!tooManyRequests) {
            tooManyRequests =
                    statusLine.getStatusCode() == RateLimitUtils.HTTP_STATUS_CODE_UNAUTHORIZED
                            && StringUtils.contains(statusLine.getReasonPhrase(), "rate limit exceeded");
        }
        return tooManyRequests;
    }

    private boolean retryForTooManyRequests(HttpResponse response, int executionCount, HttpContext context) {
        boolean shouldRetry;
        if (RateLimitUtils.hasRetryAfterHint(response)) {
            Duration duration = RateLimitUtils.retryAfter(response, Duration.ofSeconds(1));
            retryDurationSeconds.set(duration);
            LOG.warn("server signalled [429: too many requests]: retrying request for [" + context.getAttribute("http.request") + "] after waiting for [" + duration.getSeconds() + "]s following server provided rate limits [" + RateLimitUtils.parseRateLimits(response) + "]");
            shouldRetry = true;
        } else if (RateLimitUtils.hasRateResetHint(response)) {
            Duration durationUntilReset = RateLimitUtils.durationUntilReset(response);
            retryDurationSeconds.set(durationUntilReset);
            LOG.warn("server signalled [403: too many requests]: retrying request for [" + context.getAttribute("http.request") + "] after waiting for [" + durationUntilReset.getSeconds() + "]s following server provided rate limits [" + RateLimitUtils.parseRateLimits(response) + "]");
            shouldRetry = true;
        } else {
            retryDurationSeconds.set(Duration.ofSeconds(RETRY_INTERVAL_NO_HINT_SECONDS));
            shouldRetry = executionCount < maxRetries;
            if (shouldRetry) {
                LOG.warn("server signalled [429: too many requests] with no retry interval provided: retry attempt [" + executionCount + "] in " + RETRY_INTERVAL_NO_HINT_SECONDS + "s");
            } else {
                LOG.warn("server signalled [429: too many requests] with no retry interval provided and max retries [" + maxRetries + "] reached: stop trying");

            }
        }
        return shouldRetry;
    }

    @Override
    public long getRetryInterval() {
        return retryDurationSeconds.get().getSeconds() * 1000;
    }

}
