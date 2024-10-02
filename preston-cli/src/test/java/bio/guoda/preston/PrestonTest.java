package bio.guoda.preston;

import bio.guoda.preston.cmd.CmdHistory;
import bio.guoda.preston.cmd.CmdTrack;
import org.junit.Test;
import picocli.CommandLine;

import java.net.URI;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class PrestonTest {

    @Test
    public void parseRemotesWithCommas() {
        CommandLine commandLine = Preston.getCommandLine();
        CommandLine.ParseResult parseResult = commandLine.parseArgs("history",
                "--remote",
                "https://deeplinker.bio/,https://linker.bio/");

        assertTwoRemotes(parseResult);
    }

    @Test
    public void parseMessagePhrase() {
        CommandLine commandLine = Preston.getCommandLine();
        CommandLine.ParseResult parseResult = commandLine.parseArgs("track",
                "--message",
                "hello world");
        assertThat(parseResult, is(notNullValue()));

        CommandLine.ParseResult subcommand = parseResult.subcommand();
        Object o = subcommand.commandSpec().userObject();
        assertTrue(o instanceof CmdTrack);
        CmdTrack cmd = (CmdTrack) o;

        assertThat(cmd.getActivityDescription(), is("hello world"));
    }

    @Test
    public void parseRemotes() {
        CommandLine commandLine = Preston.getCommandLine();
        CommandLine.ParseResult parseResult = commandLine.parseArgs("history",
                "--remote",
                "https://deeplinker.bio/",
                "--remote",
                "https://linker.bio/");

        assertTwoRemotes(parseResult);
    }

    private void assertTwoRemotes(CommandLine.ParseResult parseResult) {
        assertThat(parseResult, is(notNullValue()));

        CommandLine.ParseResult subcommand = parseResult.subcommand();
        Object o = subcommand.commandSpec().userObject();
        assertTrue(o instanceof CmdHistory);
        CmdHistory cmd = (CmdHistory) o;

        List<URI> remotes = cmd.getRemotes();
        assertThat(remotes.size(), is(2));

        assertThat(remotes.get(0).toString(), is("https://deeplinker.bio/"));
        assertThat(remotes.get(1).toString(), is("https://linker.bio/"));
    }

}