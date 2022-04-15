package bio.guoda.preston.cmd;

import bio.guoda.preston.Version;
import com.beust.jcommander.Parameters;
import picocli.CommandLine;

@Parameters(separators = "= ", commandDescription = "show version")
@CommandLine.Command(
        name = "version"
)
public class CmdVersion extends Cmd implements Runnable {

    @Override
    public void run() {
        print(Version.getVersionString() + "\n", () -> {

        });
    }

}
