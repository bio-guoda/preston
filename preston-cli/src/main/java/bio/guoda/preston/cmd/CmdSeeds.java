package bio.guoda.preston.cmd;

import bio.guoda.preston.Seeds;
import com.beust.jcommander.Parameters;
import org.apache.commons.rdf.api.IRI;

import java.io.PrintStream;

@Parameters(separators = "= ", commandDescription = "lists supported biodiversity networks")
public class CmdSeeds extends Cmd implements Runnable {

    @Override
    public void run() {
        Seeds.AVAILABLE
                .stream()
                .map(IRI::getIRIString)
                .forEach(x -> new PrintStream(getOutputStream()).print(x + "\n"));
    }

}
