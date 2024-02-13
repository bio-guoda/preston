package bio.guoda.preston.zenodo;

import bio.guoda.preston.zenodo.ZenodoUtilsIT;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ZenodoTestUtil {
    public static String getAccessToken() throws IOException {
        return IOUtils.toString(ZenodoUtilsIT.class.getResourceAsStream("zenodo-token.hidden"), StandardCharsets.UTF_8);
    }
}
