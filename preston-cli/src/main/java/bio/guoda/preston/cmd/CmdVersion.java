package bio.guoda.preston.cmd;

import bio.guoda.preston.Version;
import picocli.CommandLine;

@CommandLine.Command(
        name = "version",
        description = CmdVersion.SHOW_VERSION
)
public class CmdVersion extends Cmd implements Runnable {

    public static final String SHOW_VERSION = "Show version";

    @Override
    public void run() {
        print(Version.getVersionString() + "\n", () -> {

        });
    }

}
