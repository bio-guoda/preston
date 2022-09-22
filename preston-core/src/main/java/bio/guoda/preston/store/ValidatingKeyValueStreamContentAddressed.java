package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import org.apache.commons.io.input.MessageDigestCalculatingInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.InputStream;
import java.security.NoSuchAlgorithmException;

public class ValidatingKeyValueStreamContentAddressed implements ValidatingKeyValueStream {

    private final MessageDigestCalculatingInputStream value;
    private final HashType type;

    public ValidatingKeyValueStreamContentAddressed(InputStream value, HashType type) {
        try {
            this.type = type;
            this.value = new MessageDigestCalculatingInputStream(value, type.getAlgorithm());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("failed to instantiate hash algorithm", e);
        }
    }

    @Override
    public InputStream getValueStream() {
        return value;
    }

    @Override
    public boolean acceptValueStreamForKey(IRI keyExpected) {
        IRI keyActual = Hasher.toHashIRI(value.getMessageDigest(), type);
        return keyActual != null
                && keyExpected != null
                && StringUtils.equals(keyExpected.getIRIString(), keyActual.getIRIString());
    }
}
