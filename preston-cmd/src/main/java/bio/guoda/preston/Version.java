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
        String propertyName = "version";
        return getPropertyValue(propertyName, defaultVersion);
    }

    private static String getPropertyValue(String propertyName, String defaultVersion) {
        String propertyValue = null;
        try (InputStream file = Version.class.getResourceAsStream("/preston.properties")) {
            Properties prop = new Properties();
            prop.load(file);
            propertyValue = prop.getProperty(propertyName);
        } catch (IOException e) {
            //
        }

        return StringUtils.isBlank(propertyValue) ? defaultVersion : propertyValue;
    }

    public static String getVersionString() {
        return getVersionString("dev");
    }

    public static String getGitCommitHash() {
        return getPropertyValue("git.commit.hash", "dev");
    }

    public static String getGitCommitDate() {
        return getPropertyValue("git.commit.date", "dev");
    }

    @Override
    public String[] getVersion() throws Exception {
        return new String[] { getVersionString("dev") };
    }
}
