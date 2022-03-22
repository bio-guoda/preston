package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.io.input.TeeInputStream;
import org.apache.commons.rdf.api.IRI;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class ValidatingKeyValueStreamSHA256IRI implements ValidatingKeyValueStream {

    private final ByteArrayOutputStream baos;
    private final InputStream value;
    private final HashType type;

    public ValidatingKeyValueStreamSHA256IRI(InputStream value) {
        this(value, HashType.sha256);
    }

    public ValidatingKeyValueStreamSHA256IRI(InputStream value, HashType type) {
        this.baos = new ByteArrayOutputStream();
        this.value = new TeeInputStream(new BoundedInputStream(value, 79), baos);
        this.type = type;

    }

    @Override
    public InputStream getValueStream() {
        return value;
    }

    @Override
    public boolean acceptValueStreamForKey(IRI key) {
        byte[] bytes = baos.toByteArray();
        String sha256Hash = new String(bytes, StandardCharsets.UTF_8);
        return bytes.length == 78
                && type.getIRIPattern().matcher(sha256Hash).matches();
    }
}
