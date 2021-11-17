package bio.guoda.preston;

import org.apache.commons.rdf.api.IRI;

public final class HashGeneratorFactory {

    public HashGenerator<IRI> create(HashType type) {
        HashGenerator<IRI> generator = null;
        if (HashType.sha256.equals(type)) {
            generator = Hasher.createSHA256HashIRIGenerator();
        }
        if (generator == null) {
            throw new IllegalArgumentException("unsupported hash type: [" + type + "]");
        }
        return generator;
    }
}


