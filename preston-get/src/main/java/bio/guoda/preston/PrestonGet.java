package bio.guoda.preston;

import bio.guoda.preston.cmd.CmdGet;
import com.beust.jcommander.JCommander;

import java.util.List;

import static java.lang.System.exit;

public class PrestonGet {
    public static void main(String[] args) {
        try {
            run(args);
            exit(0);
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            exit(1);
        }
    }

    static void run(String[] args) {
        JCommander jc = JCommander.newBuilder()
                .addObject(new CmdGet())
                .build();
        jc.parse(args);
        List<Object> objects = jc.getObjects();
        if (objects.size() > 0) {
            ((Runnable) objects.get(0)).run();
        }
    }

}
