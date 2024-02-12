package bio.guoda.preston.cmd;

import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;

public final class URLEncodingUtil {
    public static String urlEncode(String str) {
        try {
            URI uri = new URI("https", null, "example.org", -1, "/" + str, null, null);
            return StringUtils.substring(uri.getRawPath(), 1);
        } catch (URISyntaxException e) {
            return str;
            //throw new ContentStreamException("unexpected failure of URL Encoding filename [" + filename + "]", e);
        }

    }
}
