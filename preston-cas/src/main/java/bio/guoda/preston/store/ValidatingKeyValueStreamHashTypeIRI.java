package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.io.input.TeeInputStream;
import org.apache.commons.rdf.api.IRI;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class ValidatingKeyValueStreamHashTypeIRI implements ValidatingKeyValueStream {

    private final ByteArrayOutputStream baos;
    private final InputStream value;
    private final HashType type;

    public ValidatingKeyValueStreamHashTypeIRI(InputStream value, HashType type) {
        this.baos = new ByteArrayOutputStream();
        this.value = new TeeInputStream(new BoundedInputStream(value, type.getIriStringLength() + 1), baos);
        this.type = type;

    }

    @Override
    public InputStream getValueStream() {
        return value;
    }

    @Override
    public boolean acceptValueStreamForKey(IRI key) {
        byte[] bytes = baos.toByteArray();
        String hashString = new String(bytes, StandardCharsets.UTF_8);
        return bytes.length == type.getIriStringLength()
                && type.getIRIPattern().matcher(hashString).matches();
    }
}
