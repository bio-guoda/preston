package bio.guoda.preston.cmd;

import bio.guoda.preston.Version;
import com.beust.jcommander.Parameters;
import picocli.CommandLine;

@Parameters(separators = "= ", commandDescription = CmdVersion.SHOW_VERSION)
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
