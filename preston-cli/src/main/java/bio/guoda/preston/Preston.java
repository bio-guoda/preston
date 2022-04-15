package bio.guoda.preston;

/*
    Preston - a commandline tool to help discover, access and archive the biodiversity data archives, identifiers and registries.
 */

import bio.guoda.preston.cmd.CmdAlias;
import bio.guoda.preston.cmd.CmdAppend;
import bio.guoda.preston.cmd.CmdClone;
import bio.guoda.preston.cmd.CmdCopyTo;
import bio.guoda.preston.cmd.CmdGrep;
import bio.guoda.preston.cmd.CmdHash;
import bio.guoda.preston.cmd.CmdSeeds;
import bio.guoda.preston.cmd.CmdSketchCreate;
import bio.guoda.preston.cmd.CmdGet;
import bio.guoda.preston.cmd.CmdHistory;
import bio.guoda.preston.cmd.CmdList;
import bio.guoda.preston.cmd.CmdMerge;
import bio.guoda.preston.cmd.CmdOrigins;
import bio.guoda.preston.cmd.CmdSketchDiff;
import bio.guoda.preston.cmd.CmdUpdate;
import bio.guoda.preston.cmd.CmdVerify;
import bio.guoda.preston.cmd.CmdVersion;
import bio.guoda.preston.cmd.TypeConverterIRI;
import org.apache.commons.rdf.api.IRI;
import picocli.CommandLine;

import static java.lang.System.exit;

@CommandLine.Command(name = "preston",
        versionProvider = Version.class,
        subcommands = {
                CmdGet.class,
                CmdList.class,
                CmdCopyTo.class,
                CmdClone.class,
                CmdUpdate.class,
                CmdAppend.class,
                CmdHistory.class,
                CmdOrigins.class,
                CmdMerge.class,
                CmdSketchCreate.class,
                CmdSketchDiff.class,
                CmdGrep.class,
                CmdVerify.class,
                CmdVersion.class,
                CmdSeeds.class,
                CmdHash.class,
                CmdAlias.class,
                CommandLine.HelpCommand.class
        },
        description = "Preston - a biodiversity dataset tracker",
        mixinStandardHelpOptions = true)
public class Preston {
    public static void main(String[] args) {
        try {
            //CmdLine.run(args);

            CommandLine commandLine = new CommandLine(new Preston());
            commandLine.registerConverter(IRI.class, new TypeConverterIRI());
            int exitCode = commandLine.execute(args);
            System.exit(exitCode);
            exit(0);
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            exit(1);
        }
    }

}
