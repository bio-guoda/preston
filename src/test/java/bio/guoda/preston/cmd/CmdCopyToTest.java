package bio.guoda.preston.cmd;

import org.hamcrest.core.Is;
import org.junit.Test;

import static org.junit.Assert.*;

public class CmdCopyToTest {

    @Test
    public void formatProgress() {
        assertThat(CmdCopyTo.formatProgress(1, 99), Is.is("\rcopying [1.0%]..."));
    }

}