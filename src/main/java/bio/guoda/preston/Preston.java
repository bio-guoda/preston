package bio.guoda.preston;

/*
    Preston - a commandline tool to help discover, access and archive the biodiversity data archives, identifiers and registries.
 */

import org.apache.commons.lang3.StringUtils;
import bio.guoda.preston.cmd.CmdLine;

import java.io.InputStream;
import java.io.IOException;
import java.util.Properties;

import static java.lang.System.exit;

public class Preston {
    public static void main(String[] args) {
        try {
            CmdLine.run(args);
            exit(0);
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            exit(1);
        }
    }

    public static String getVersion() {
        String version = "";
        try (InputStream file = Preston.class.getClassLoader().getResourceAsStream("preston.properties")) {
            Properties prop = new Properties();
            prop.load(file);
            version = prop.getProperty("version");
        } catch (IOException e) {
        }

        return StringUtils.isBlank(version) ? "dev" : version;
    }
}
