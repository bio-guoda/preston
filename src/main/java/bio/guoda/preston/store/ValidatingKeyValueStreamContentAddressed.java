package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
import org.apache.commons.io.input.MessageDigestCalculatingInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.InputStream;
import java.security.NoSuchAlgorithmException;

public class ValidatingKeyValueStreamContentAddressed implements ValidatingKeyValueStream {

    private final MessageDigestCalculatingInputStream value;

    public ValidatingKeyValueStreamContentAddressed(InputStream value) {
        try {
            this.value = new MessageDigestCalculatingInputStream(value, Hasher.getHashAlgorithm());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("failed to instantiate hash algorithm", e);
        }
    }

    @Override
    public InputStream getValueStream() {
        return value;
    }

    @Override
    public boolean acceptValueStreamForKey(IRI key) {
        IRI iri = Hasher.toHashURI(value.getMessageDigest());
        return iri != null
                && key != null
                && StringUtils.equals(key.getIRIString(), iri.getIRIString());
    }
}
