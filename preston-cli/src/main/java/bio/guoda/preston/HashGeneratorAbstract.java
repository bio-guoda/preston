package bio.guoda.preston;

import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class HashGeneratorAbstract<T> implements HashGenerator<T> {

    @Override
    public T hash(InputStream is) throws IOException {
        return hash(is, new NullOutputStream());
    }

    @Override
    public T hash(InputStream is, OutputStream os) throws IOException {
        return hash(is, os, true);
    }

}
