package bio.guoda.preston;

import org.apache.commons.lang3.StringUtils;

public class EnvUtil {

    public static String getEnvironmentVariable(String environmentVariableName) {
        return StringUtils.defaultIfBlank(
                StringUtils.defaultIfBlank(
                        System.getenv("secrets." + environmentVariableName),
                        System.getenv(environmentVariableName)
                ),
                System.getProperty(environmentVariableName));
    }

    public static String getEnvironmentVariable(String environmentVariableName, String defaultValue) {
        return StringUtils.defaultIfBlank(
                getEnvironmentVariable(environmentVariableName),
                defaultValue);
    }
}
