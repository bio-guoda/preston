package bio.guoda.preston;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class HashGeneratorFactory {

    public HashGenerator<IRI> create(HashType type) {
        return new HashGeneratorSingle(type);
    }

    public HashGenerator<List<IRI>> create(final List<HashType> types) {
        return new HashGeneratorCascading(types);
    }

    private static class HashGeneratorSingle extends HashGeneratorAbstract<IRI> {
        private final HashGenerator<List<IRI>> proxy;

        public HashGeneratorSingle(HashType type) {
            proxy = new HashGeneratorCascading(Collections.singletonList(type));
        }

        @Override
        public IRI hash(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
            List<IRI> hashes = proxy.hash(is, os, shouldCloseInputStream);
            if (hashes.size() != 1) {
                throw new IOException("expected one hash but got [" + hashes.size() + "]");
            }
            return hashes.get(0);
        }
    }
}


