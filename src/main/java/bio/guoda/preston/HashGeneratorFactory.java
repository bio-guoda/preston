package bio.guoda.preston;

import org.apache.commons.rdf.api.IRI;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public final class HashGeneratorFactory {

    public HashGenerator<IRI> create(HashType type) {
        HashGenerator<IRI> generator = null;
        if (HashType.SHA256.equals(type)) {
            generator = Hasher.createSHA256HashIRIGenerator();
        } else if (HashType.TLSH.equals(type)) {
            generator = new HashGeneratorTLSHashIRI();
        }
        if (generator == null) {
            throw new NotImplementedException();
        }
        return generator;
    }
}


