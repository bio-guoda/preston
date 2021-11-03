package bio.guoda.preston;

/*
    Preston - a commandline tool to help discover, access and archive the biodiversity data archives, identifiers and registries.
 */

import bio.guoda.preston.cmd.CmdLine;

import static java.lang.System.exit;

public class Preston {
    public static void main(String[] args) {
        try {
            CmdLine.run(args);
            exit(0);
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            exit(1);
        }
    }

}
