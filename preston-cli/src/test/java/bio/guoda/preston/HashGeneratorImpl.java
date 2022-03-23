package bio.guoda.preston;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HashGeneratorImpl extends HashGeneratorAbstract<IRI> {

    private final HashType type;

    public HashGeneratorImpl(HashType type) {
        this.type = type;
    }

    @Override
    public IRI hash(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
        return Hasher.calcHashIRI(is, os, shouldCloseInputStream, type);
    }
}
