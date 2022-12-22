package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;

public class ValidatingKeyValueStreamHashTypeIRIFactory implements ValidatingKeyValueStreamFactory {

    @Override
    public ValidatingKeyValueStream forKeyValueStream(IRI key, InputStream is) throws IOException {
        HashType type = HashKeyUtil.getHashTypeOrThrowIOException(key);
        return new ValidatingKeyValueStreamHashTypeIRI(is, type);
    }
}
