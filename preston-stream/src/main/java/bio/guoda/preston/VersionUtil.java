package bio.guoda.preston;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class VersionUtil {
    public static String getVersionString(String defaultVersion) {
        String propertyName = "version";
        return getPropertyValue(propertyName, defaultVersion);
    }

    private static String getPropertyValue(String propertyName, String defaultVersion) {
        String propertyValue = null;
        try (InputStream file = VersionUtil.class.getResourceAsStream("/preston.properties")) {
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
}
