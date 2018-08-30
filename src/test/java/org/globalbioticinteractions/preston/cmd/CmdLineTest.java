package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.MissingCommandException;
import org.junit.Test;

public class CmdLineTest {

    @Test
    public void check() throws Throwable {
        CmdLine.run(new String[]{"version"});
    }

    @Test(expected = MissingCommandException.class)
    public void invalidCommand() throws Throwable {
        CmdLine.run(new String[]{"bla"});
    }

    @Test(expected = MissingCommandException.class)
    public void noCommand() throws Throwable {
        CmdLine.run(new String[]{});
    }
}