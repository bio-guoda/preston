package bio.guoda.preston.cmd;

import bio.guoda.preston.HashGenerator;
import bio.guoda.preston.HashGeneratorFactory;
import bio.guoda.preston.HashType;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

/**
 * Hashes bytes from stdin using some hash algorithm
 */

@Parameters(separators = "= ", commandDescription = "offline (re-)processing of tracked biodiversity dataset graph using stdin")
public class CmdHash extends Cmd implements Runnable {

    @Parameter(names = {"--hash-algorithm", "--algo", "-a"}, description = "hash algorithm used content identifier")
    private HashType hashType = HashType.sha256;

    @Override
    public void run() {
        HashGenerator<List<IRI>> iriHashGenerator
                = new HashGeneratorFactory()
                .create(Collections.singletonList(hashType));
        try {
            List<IRI> hashes = iriHashGenerator.hash(getInputStream());
            for (IRI hash : hashes) {
                InputStream hashIs = IOUtils.toInputStream(
                        hash.getIRIString() + "\n",
                        StandardCharsets.UTF_8);
                IOUtils.copy(hashIs, getOutputStream());
            }
        } catch (IOException e) {
            throw new RuntimeException("failed to generate hash", e);
        }
    }

}
