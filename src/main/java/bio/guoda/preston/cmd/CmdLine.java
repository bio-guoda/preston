package bio.guoda.preston.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.MissingCommandException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CmdLine {
    private final static Log LOG = LogFactory.getLog(CmdLine.class);

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
            printUsage(jc);
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

    JCommander buildCommander() {
        return JCommander.newBuilder()
                .addObject(new CommandMain())
                .addCommand("ls", new CmdList(), "log", "logs")
                .addCommand("hash", new CmdHash())
                .addCommand("get", new CmdGet(), "cat")
                .addCommand("cp", new CmdCopyTo(), "copyTo", "export")
                .addCommand("clone", new CmdClone(), "pull")
                .addCommand("update", new CmdUpdate(), "track")
                .addCommand("process", new CmdProcess(), "handle")
                .addCommand("tika", new CmdTika(), "tika-tlsh")
                .addCommand("similar", new CmdSimilar(), "similarity")
                .addCommand("history", new CmdHistory())
                .addCommand("version", new CmdVersion())
                .addCommand("test", new CmdVerify(), "verify", "check", "validate")
                .addCommand("seeds", new CmdSeeds())
                .build();
    }


}