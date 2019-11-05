package bio.guoda.preston.store;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.io.input.TeeInputStream;
import org.apache.commons.rdf.api.IRI;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

public class ValidatingKeyValueStreamSHA256IRI implements ValidatingKeyValueStream {

    private final static Pattern SHA_256_PATTERN = Pattern.compile("hash://sha256/([a-z0-9]){64}");

    private final ByteArrayOutputStream baos;
    private final InputStream value;

    public ValidatingKeyValueStreamSHA256IRI(InputStream value) {
        this.baos = new ByteArrayOutputStream();
        this.value = new TeeInputStream(new BoundedInputStream(value, 79), baos);
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
                && SHA_256_PATTERN.matcher(sha256Hash).matches();
    }
}
