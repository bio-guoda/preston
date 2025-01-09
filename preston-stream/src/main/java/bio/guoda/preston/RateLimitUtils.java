package bio.guoda.preston;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpMessage;

import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;

public class RateLimitUtils {

    public static final String X_RATE_LIMIT_PREFIX = "X-RateLimit-";
    public static final String X_RATE_LIMIT_LIMIT = X_RATE_LIMIT_PREFIX + "Limit";
    public static final String X_RATE_LIMIT_REMAINING = X_RATE_LIMIT_PREFIX + "Remaining";
    public static final String X_RATE_LIMIT_RESET = X_RATE_LIMIT_PREFIX + "Reset";
    public static final int HTTP_STATUS_CODE_TOO_MANY_REQUESTS = 429;

    public static boolean hasRetryAfterHint(HttpMessage msg) {
        Map<String, Long> rateLimits = parseRateLimits(msg);
        return rateLimits.containsKey(HttpHeaders.RETRY_AFTER);
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
}
