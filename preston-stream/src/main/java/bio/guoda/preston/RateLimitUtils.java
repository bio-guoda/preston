package bio.guoda.preston;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpMessage;
import org.joda.time.DateTimeUtils;

import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;

import static bio.guoda.preston.RateLimitedRetryStrategy.RETRY_INTERVAL_NO_HINT_SECONDS;

public class RateLimitUtils {

    public static final String X_RATE_LIMIT_PREFIX = "X-RateLimit-";
    public static final String X_RATE_LIMIT_LIMIT = X_RATE_LIMIT_PREFIX + "Limit";
    public static final String X_RATE_LIMIT_REMAINING = X_RATE_LIMIT_PREFIX + "Remaining";
    public static final String X_RATE_LIMIT_RESET = X_RATE_LIMIT_PREFIX + "Reset";
    public static final int HTTP_STATUS_CODE_TOO_MANY_REQUESTS = 429;
    public static final int HTTP_STATUS_CODE_UNAUTHORIZED = 403;

    public static boolean hasRetryAfterHint(HttpMessage msg) {
        Map<String, Long> rateLimits = parseRateLimits(msg);
        return rateLimits.containsKey(HttpHeaders.RETRY_AFTER);
    }

    public static boolean hasRateResetHint(HttpMessage msg) {
        Map<String, Long> rateLimits = parseRateLimits(msg);
        return rateLimits.containsKey(X_RATE_LIMIT_RESET);
    }

    public static Map<String, Long> parseRateLimits(HttpMessage msg) {
        Map<String, Long> rateLimits = new TreeMap<>();

        appendNameValuePair(msg, rateLimits, X_RATE_LIMIT_RESET);
        appendNameValuePair(msg, rateLimits, X_RATE_LIMIT_REMAINING);
        appendNameValuePair(msg, rateLimits, X_RATE_LIMIT_LIMIT);
        appendNameValuePair(msg, rateLimits, HttpHeaders.RETRY_AFTER);
        return rateLimits;
    }

    private static void appendNameValuePair(HttpMessage msg, Map<String, Long> rateLimits, String name) {
        Header[] headers = msg.getHeaders(name);
        for (Header header1 : headers) {
            if (NumberUtils.isDigits(header1.getValue())) {
                rateLimits.put(name, Long.parseLong(header1.getValue()));
            }
        }
    }

    public static Duration retryAfter(HttpMessage msg, Duration defaultRetryDuration) {
        Map<String, Long> rateLimits = parseRateLimits(msg);
        Long retryAfterInSeconds = rateLimits.get(HttpHeaders.RETRY_AFTER);
        return retryAfterInSeconds == null ? defaultRetryDuration : Duration.ofSeconds(retryAfterInSeconds + 1L);
    }

    public static Duration durationUntilReset(HttpMessage msg) {
        Map<String, Long> rateLimits = parseRateLimits(msg);
        Long resetDateInUnixEpochSeconds = rateLimits.get(X_RATE_LIMIT_RESET);
        Duration untilReset = Duration.ofSeconds(1 + resetDateInUnixEpochSeconds - (DateTimeUtils.currentTimeMillis() / 1000));
        return untilReset.isNegative()
                ? Duration.ofSeconds(RETRY_INTERVAL_NO_HINT_SECONDS)
                : untilReset;
    }
}
