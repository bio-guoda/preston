package bio.guoda.preston;

/*
    Preston - a commandline tool to help discover, access and archive the biodiversity data archives, identifiers and registries.
 */

import bio.guoda.preston.cmd.CmdAlias;
import bio.guoda.preston.cmd.CmdAppend;
import bio.guoda.preston.cmd.CmdCite;
import bio.guoda.preston.cmd.CmdClone;
import bio.guoda.preston.cmd.CmdCopyTo;
import bio.guoda.preston.cmd.CmdDwcRecordStream;
import bio.guoda.preston.cmd.CmdGenerateQRCode;
import bio.guoda.preston.cmd.CmdGet;
import bio.guoda.preston.cmd.CmdGrep;
import bio.guoda.preston.cmd.CmdHash;
import bio.guoda.preston.cmd.CmdHead;
import bio.guoda.preston.cmd.CmdHistory;
import bio.guoda.preston.cmd.CmdInstallManual;
import bio.guoda.preston.cmd.CmdList;
import bio.guoda.preston.cmd.CmdMerge;
import bio.guoda.preston.cmd.CmdPlazi;
import bio.guoda.preston.cmd.CmdSeeds;
import bio.guoda.preston.cmd.CmdUpdate;
import bio.guoda.preston.cmd.CmdVerify;
import bio.guoda.preston.cmd.CmdVersion;
import bio.guoda.preston.cmd.TypeConverterIRI;
import bio.guoda.preston.dbase.CmdDBaseRecordStream;
import bio.guoda.preston.excel.CmdExcelRecordStream;
import bio.guoda.preston.server.CmdServe;
import org.apache.commons.rdf.api.IRI;
import picocli.CommandLine;
import picocli.codegen.docgen.manpage.ManPageGenerator;

import static java.lang.System.exit;

@CommandLine.Command(name = "preston",
        versionProvider = Version.class,
        description = "a biodiversity dataset tracker",
        mixinStandardHelpOptions = true,
        subcommands = {
                CmdGet.class,
                CmdList.class,
                CmdCopyTo.class,
                CmdClone.class,
                CmdUpdate.class,
                CmdAppend.class,
                CmdHistory.class,
                CmdMerge.class,
                CmdGrep.class,
                CmdVerify.class,
                CmdVersion.class,
                CmdSeeds.class,
                CmdHash.class,
                CmdAlias.class,
                CmdDwcRecordStream.class,
                CmdDBaseRecordStream.class,
                CmdExcelRecordStream.class,
                CmdPlazi.class,
                CmdServe.class,
                CmdCite.class,
                CmdGenerateQRCode.class,
                CmdHead.class,
                CmdInstallManual.class,
                ManPageGenerator.class,
                CommandLine.HelpCommand.class
        })

public class Preston {
    public static void main(String[] args) {
        try {
            int exitCode = run(args);
            System.exit(exitCode);
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            exit(1);
        }
    }

    public static int run(String[] args) {
        CommandLine commandLine = getCommandLine();
        return commandLine.execute(args);
    }

    public static CommandLine getCommandLine() {
        CommandLine commandLine = new CommandLine(new Preston());
        commandLine.registerConverter(IRI.class, new TypeConverterIRI());
        return commandLine;
    }

}
