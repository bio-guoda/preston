package bio.guoda.preston;

import bio.guoda.preston.cmd.CmdGet;
import bio.guoda.preston.cmd.TypeConverterIRI;
import org.apache.commons.rdf.api.IRI;
import picocli.CommandLine;

import static java.lang.System.exit;

public class PrestonGet {
    public static void main(String[] args) {
        try {
            int exitCode = run(args);
            System.exit(exitCode);
            exit(0);
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            exit(1);
        }
    }

    static int run(String[] args) {
        CommandLine commandLine = new CommandLine(new CmdGet());
        commandLine.registerConverter(IRI.class, new TypeConverterIRI());
        return commandLine.execute(args);
    }

}
