package bio.guoda.preston;

import org.apache.commons.lang3.StringUtils;

public class VersionUtil {
    public static String getVersionString(String defaultVersion) {
        String propertyValue = VersionUtil.class.getPackage().getImplementationVersion();
        return StringUtils.isBlank(propertyValue) ? defaultVersion : propertyValue;
    }

    public static String getVersionString() {
        return getVersionString("dev");
    }
}
