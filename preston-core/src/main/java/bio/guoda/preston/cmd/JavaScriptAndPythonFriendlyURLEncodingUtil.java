package bio.guoda.preston.cmd;

import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public final class JavaScriptAndPythonFriendlyURLEncodingUtil {


    public static String urlEncode(String str) {
        String result;

        try {
            String quoteEscaped = StringUtils.replace(str, "'", "&quot;");
            result = URLEncoder.encode(quoteEscaped, "UTF-8")
                    .replaceAll("\\+", "%20")
                    .replaceAll("\\%21", "!")
                    .replaceAll("\\%27", "'")
                    .replaceAll("\\%28", "(")
                    .replaceAll("\\%29", ")")
                    .replaceAll("\\%7E", "~");
        } catch (UnsupportedEncodingException e) {
            result = str;
        }

        return result;
    }
}
