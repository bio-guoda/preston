package bio.guoda.preston;

/*
    Preston - a commandline tool to help discover, access and archive the biodiversity data archives, identifiers and registries.
 */

import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class Version implements CommandLine.IVersionProvider {
    public static String getVersionString(String defaultVersion) {
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

    public static String getVersionString() {
        return getVersionString("dev");
    }

    @Override
    public String[] getVersion() throws Exception {
        return new String[] { getVersionString("dev") };
    }
}
