package bio.guoda.preston;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

class HashGeneratorCascading extends HashGeneratorAbstract<List<IRI>> {

    private final List<HashType> types;

    public HashGeneratorCascading(List<HashType> types) {
        this.types = types;
    }

    @Override
    public List<IRI> hash(
            InputStream is,
            OutputStream os,
            boolean shouldCloseInputStream) throws IOException {

        return Hasher
                .calcHashIRIs(
                        is,
                        os,
                        shouldCloseInputStream,
                        types.stream().map(HashType::getAlgorithm));
    }
}
