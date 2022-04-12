package bio.guoda.preston.cmd;

import bio.guoda.preston.Version;
import bio.guoda.preston.process.LogErrorHandler;
import com.beust.jcommander.Parameters;

@Parameters(separators = "= ", commandDescription = "show version")
public class CmdVersion extends Cmd implements Runnable {

    @Override
    public void run() {
        print(Version.getVersion() + "\n", () -> {

        });
    }

}
