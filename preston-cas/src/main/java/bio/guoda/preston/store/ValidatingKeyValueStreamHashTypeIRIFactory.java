package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.rdf.api.IRI;

import java.io.InputStream;

public class ValidatingKeyValueStreamHashTypeIRIFactory implements ValidatingKeyValueStreamFactory {

    private final HashType type;

    public ValidatingKeyValueStreamHashTypeIRIFactory(HashType type) {
        this.type = type;
    }

    @Override
    public ValidatingKeyValueStream forKeyValueStream(IRI key, InputStream is) {
        return new ValidatingKeyValueStreamHashTypeIRI(is, type);
    }
}
