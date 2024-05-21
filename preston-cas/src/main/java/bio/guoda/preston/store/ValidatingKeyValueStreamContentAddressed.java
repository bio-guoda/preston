package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import org.apache.commons.io.input.MessageDigestInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;

public class ValidatingKeyValueStreamContentAddressed extends ValidatingKeyValueStreamWithViolations {

    private final MessageDigestInputStream value;
    private final HashType type;

    public ValidatingKeyValueStreamContentAddressed(InputStream value, HashType type) {
        try {
            this.type = type;
            this.value = MessageDigestInputStream.builder()
                    .setMessageDigest(type.getAlgorithm())
                    .setInputStream(value).get();
        } catch (NoSuchAlgorithmException | IOException e) {
            throw new IllegalStateException("failed to instantiate hash algorithm", e);
        }
    }

    @Override
    public InputStream getValueStream() {
        return value;
    }

    @Override
    public boolean acceptValueStreamForKey(IRI key) {
        IRI iri = Hasher.toHashIRI(value.getMessageDigest(), type);
        return iri != null
                && key != null
                && StringUtils.equals(key.getIRIString(), iri.getIRIString());
    }
}
