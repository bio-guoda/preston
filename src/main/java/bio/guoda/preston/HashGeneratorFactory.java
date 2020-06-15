package bio.guoda.preston;

import org.apache.commons.rdf.api.IRI;

public final class HashGeneratorFactory {

    public HashGenerator<IRI> create(HashType type) {
        HashGenerator<IRI> generator = null;
        if (HashType.sha256.equals(type)) {
            generator = Hasher.createSHA256HashIRIGenerator();
        } else if (HashType.tlsh.equals(type)) {
            generator = new HashGeneratorTLSHTruncated();
        } else if (HashType.tika_tlsh.equals(type)) {
            generator = new HashGeneratorTikaTLSH();
        }
        if (generator == null) {
            throw new IllegalArgumentException("unsupported hash type: [" + type + "]");
        }
        return generator;
    }
}


