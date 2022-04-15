package bio.guoda.preston.cmd;

import bio.guoda.preston.Seeds;
import org.apache.commons.rdf.api.IRI;
import picocli.CommandLine;

@CommandLine.Command(name = "seeds", description = CmdSeeds.LISTS_SUPPORTED_BIODIVERSITY_NETWORKS)
public class CmdSeeds extends Cmd implements Runnable {

    public static final String LISTS_SUPPORTED_BIODIVERSITY_NETWORKS = "Lists supported biodiversity networks";

    @Override
    public void run() {
        Seeds.AVAILABLE
                .stream()
                .map(IRI::getIRIString)
                .forEach(x -> {
                    print(x + "\n", LogErrorHandlerExitOnError.EXIT_ON_ERROR);
                });
    }

}
