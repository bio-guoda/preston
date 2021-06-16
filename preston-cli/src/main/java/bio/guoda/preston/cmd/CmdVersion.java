package bio.guoda.preston.cmd;

import bio.guoda.preston.Preston;
import com.beust.jcommander.Parameters;

@Parameters(separators = "= ", commandDescription = "show version")
public class CmdVersion implements Runnable {

    @Override
    public void run() {
        System.out.print(Preston.getVersion() + "\n");
    }

}
