package bio.guoda.preston;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HashGeneratorSHA256 extends HashGeneratorAbstract<IRI> {

    @Override
    public IRI hash(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
        return Hasher.calcHashIRI(is, os, shouldCloseInputStream, HashType.sha256.getAlgorithm());
    }
}
