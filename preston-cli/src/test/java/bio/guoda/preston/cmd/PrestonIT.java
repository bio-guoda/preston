package bio.guoda.preston.cmd;

import bio.guoda.preston.Preston;
import org.junit.Test;

public class PrestonIT {

    @Test
    public void history() {
        Preston.run(new String[]{"history","--remote","https://deeplinker.bio/"});
    }

}