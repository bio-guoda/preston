package bio.guoda.preston.cmd;

import bio.guoda.preston.Preston;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

@CommandLine.Command(
        name = "config-man",
        aliases = {"config-manpage", "install-manpage"},
        description = "Installs/configures Preston man page, so you can type [man preston] on unix-like system to learn more about Preston. "
)
public class CmdInstallManual extends Cmd implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CmdInstallManual.class);


    @Override
    public void run() {
        File manPageDir = new File("/usr/local/share/man/man1/");
        Collection<String> strings = listManpageFilenames();
        for (String manpageFilename : strings) {
            installManPage(manPageDir, manpageFilename);
        }
    }

    private void installManPage(File manPageDir, String manpageFilename) {
        File file = new File(manPageDir, manpageFilename);
        try (InputStream resourceAsStream = getClass().getResourceAsStream("/bio/guoda/preston/docs/manpage/" + manpageFilename)) {
            if (resourceAsStream != null) {
                if (!manPageDir.exists()) {
                    FileUtils.forceMkdir(manPageDir);
                }
                IOUtils.copy(resourceAsStream, Files.newOutputStream(file.toPath()));
                LOG.info("installed man page at [" + file.getAbsolutePath() + "]");
            }
        } catch (IOException e) {
            LOG.error("failed to install man page at [" + file.getAbsolutePath() + "]", e);
        }
    }

    static Collection<String> listManpageFilenames() {
        Map<String, CommandLine> subcommands = Preston.getCommandLine().getSubcommands();
        Set<String> names = new TreeSet<>();
        names.add("preston.1");
        for (String s : subcommands.keySet()) {
            names.add("preston-" + subcommands.get(s).getCommandName() + ".1");
        }
        return names;
    }


}
