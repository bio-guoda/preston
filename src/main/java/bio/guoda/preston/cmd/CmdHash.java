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
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Hashes bytes from stdin using some hash algorithm
 */

@Parameters(separators = "= ", commandDescription = "offline (re-)processing of tracked biodiversity dataset graph using stdin")
public class CmdHash implements Runnable {

    private InputStream is = System.in;
    private OutputStream os = System.out;

    @Parameter(names = {"--hash-algorithm", "--algo", "-a"}, description = "hash algorithm used for verification")
    private HashType hashType = HashType.SHA256;

    public void setInputStream(InputStream inputStream) {
        this.is = inputStream;
    }
    private InputStream getInputStream() {
        return(this.is);
    }

    @Override
    public void run() {
        HashGenerator<IRI> iriHashGenerator = new HashGeneratorFactory().create(hashType);
        try {
            InputStream hashIs = IOUtils.toInputStream(
                    iriHashGenerator.hash(getInputStream()).getIRIString() + "\n",
                    StandardCharsets.UTF_8);
            IOUtils.copy(hashIs, os);
        } catch (IOException e) {
            throw new RuntimeException("failed to generate hash", e);
        }
    }

    void setOutputStream(OutputStream outputStream) {
        this.os = outputStream;
    }

    void setHashAlgorithm(HashType hashType) {
        this.hashType = hashType;
    }
}
