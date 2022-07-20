package bio.guoda.preston.cmd;

import bio.guoda.preston.Preston;
import org.junit.Test;

public class PrestonIT {

    @Test
    public void history() {
        Preston.run(new String[]{
                "history",
                "--remote",
                "https://deeplinker.bio/"
        });
    }

    @Test
    public void catDataOne() {
        Preston.run(new String[]{
                "cat",
                "--algo",
                "md5",
                "--remote",
                "https://dataone.org",
                "hash://md5/e27c99a7f701dab97b7d09c467acf468"
        });
    }

}