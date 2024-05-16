package bio.guoda.preston.zenodo;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ZenodoTestUtil {
    public static String getAccessToken() throws IOException {
        return IOUtils.toString(ZenodoUtilsTaxoDrosIT.class.getResourceAsStream("zenodo-token.hidden"), StandardCharsets.UTF_8);
    }
}
