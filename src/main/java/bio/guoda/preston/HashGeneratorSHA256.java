package bio.guoda.preston;

import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HashGeneratorSHA256 implements HashGenerator<IRI> {

    @Override
    public IRI hash(InputStream is) throws IOException {
        return hash(is, new NullOutputStream());
    }

    @Override
    public IRI hash(InputStream is, OutputStream os) throws IOException {
        return hash(is, os, true);
    }

    @Override
    public IRI hash(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
        return Hasher.toSHA256IRI(Hasher.calcSHA256String(is, os, shouldCloseInputStream));
    }
}
