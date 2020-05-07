package bio.guoda.preston;

import org.apache.commons.rdf.api.IRI;

public final class HashGeneratorFactory {

    public HashGenerator<IRI> create(HashType type) {
        HashGenerator<IRI> generator = null;
        if (HashType.SHA256.equals(type)) {
            generator = Hasher.createSHA256HashIRIGenerator();
        } else if (HashType.TLSH.equals(type)) {
            generator = new HashGeneratorTLSHashIRI();
        } else if (HashType.TLSH_TIKA.equals(type)) {
            generator = new HashGeneratorTLSHTika();
        }
        if (generator == null) {
            throw new IllegalArgumentException("unsupported hash type: [" + type + "]");
        }
        return generator;
    }
}


