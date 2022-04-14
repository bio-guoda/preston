package bio.guoda.preston.store;

/*
    Preston - a commandline tool to help discover, access and archive the biodiversity data archives, identifiers and registries.
 */

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Version {
    public static String getVersion(String defaultVersion) {
        String version = null;
        try (InputStream file = Version.class.getResourceAsStream("/preston.properties")) {
            Properties prop = new Properties();
            prop.load(file);
            version = prop.getProperty("version");
        } catch (IOException e) {
            //
        }

        return StringUtils.isBlank(version) ? defaultVersion : version;
    }

    public static String getVersion() {
        return getVersion("dev");
    }
}
