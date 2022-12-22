package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.rdf.api.IRI;

import java.io.InputStream;

public class ValidatingKeyValueStreamContentAddressedFactory implements ValidatingKeyValueStreamFactory {

    public ValidatingKeyValueStreamContentAddressedFactory() {
    }
    @Override
    public ValidatingKeyValueStream forKeyValueStream(IRI key, InputStream is) {
        HashType type = HashKeyUtil.getHashTypeOrThrow(key);
        return new ValidatingKeyValueStreamContentAddressed(is, type);
    }
}
