package bio.guoda.preston;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public final class HashGeneratorFactory {

    HashGenerator create(HashType type) {
        HashGenerator generator = null;
        if (HashType.SHA256.equals(type)) {
            generator = Hasher.createSHA256HashGenerator();
        } else if (HashType.LTSH.equals(type)) {
            generator = new HashGeneratorLTSH();
        }
        if (generator == null) {
            throw new NotImplementedException();
        }
        return generator;
    }
}


