package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.Parameters;
import org.globalbioticinteractions.preston.Preston;

@Parameters(separators = "= ", commandDescription = "Show Version")
public class CmdVersion implements Runnable {

    @Override
    public void run() {
        System.out.println(Preston.getVersion());
    }

}
