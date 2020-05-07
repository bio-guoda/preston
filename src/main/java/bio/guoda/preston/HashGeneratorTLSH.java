package bio.guoda.preston;

import org.apache.commons.io.output.NullOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HashGeneratorTLSH extends HashGeneratorAbstract<String> {

    @Override
    public String hash(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
        return HashGeneratorTLSHashIRI.calculateLTSH(is, os, shouldCloseInputStream);
    }

}
