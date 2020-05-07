package bio.guoda.preston;

import org.apache.commons.io.output.NullOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HashGeneratorLTSH implements HashGenerator<String> {
    @Override
    public String hash(InputStream is) throws IOException {
        return hash(is, new NullOutputStream());
    }

    @Override
    public String hash(InputStream is, OutputStream os) throws IOException {
        return hash(is, os, true);
    }

    @Override
    public String hash(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
        return HashGeneratorTLSHashIRI.calculateLTSH(is, os, shouldCloseInputStream);
    }

}
