package bio.guoda.preston.cmd;

import org.junit.Test;

public class CmdLineIT {

    @Test
    public void history() throws Throwable {
        CmdLine.run(new String[]{"history","--remote","https://deeplinker.bio/"});
    }

}