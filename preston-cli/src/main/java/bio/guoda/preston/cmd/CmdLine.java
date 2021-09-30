package bio.guoda.preston.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.MissingCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CmdLine {
    private final static Logger LOG = LoggerFactory.getLogger(CmdLine.class);

    public static void run(JCommander actual) {
        if (actual == null) {
            throw new MissingCommandException("no command provided");
        } else if (!(actual.getObjects().get(0) instanceof Runnable)) {
            throw new MissingCommandException("invalid command provided");
        } else {
            Object cmdObject = actual.getObjects().get(0);
            ((Runnable) cmdObject).run();
        }
    }

    public static void run(String[] args) throws Throwable {
        JCommander jc = new CmdLine().buildCommander();
        try {
            jc.parse(args);
            CmdLine.run(jc.getCommands().get(jc.getParsedCommand()));
        } catch (MissingCommandException ex) {
            printUsage(jc);
            throw ex;
        } catch (Throwable ex) {
            LOG.error("unexpected exception", ex);
            throw ex;
        }
    }

    private static void printUsage(JCommander jc) {
        StringBuilder out = new StringBuilder();
        jc.usage(out);
        System.err.append(out.toString());
    }

    public class CommandMain implements Runnable {

        @Override
        public void run() {

        }
    }

    private JCommander buildCommander() {
        return JCommander.newBuilder()
                .addObject(new CommandMain())
                // core operations
                .addCommand("ls", new CmdList(), "log", "logs")
                .addCommand("get", new CmdGet(), "cat")
                .addCommand("cp", new CmdCopyTo(), "copyTo", "export")
                .addCommand("clone", new CmdClone(), "pull")
                .addCommand("update", new CmdUpdate(), "track")
                .addCommand("append", new CmdAppend(), "handle", "process", "add")
                .addCommand("history", new CmdHistory())

                // sketch generation (e.g, bloom, theta, tika-tlsh)
                .addCommand("tika", new CmdTika(), "tika-tlsh")
                .addCommand("sketch", new CmdCreateSketch(), "bloom")

                // sketch operations
                .addCommand("similar", new CmdSimilar(), "similarity")
                .addCommand("diff", new CmdSketchDiff(), "intersect")
                .addCommand("union", new CmdSketchUnion())

                // utilities
                .addCommand("grep", new CmdMatch(), "match", "findURLs")
                .addCommand("test", new CmdVerify(), "verify", "check", "validate")
                .addCommand("version", new CmdVersion())
                .addCommand("seeds", new CmdSeeds())
                .addCommand("text", new CmdText(), "extract-text")
                .addCommand("hash", new CmdHash())
                .addCommand("alias", new CmdAlias(), "aliases", "nickname", "nick")

                .build();
    }


}