package bio.guoda.preston.store;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.io.input.TeeInputStream;
import org.apache.commons.rdf.api.IRI;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

public class ValidatingKeyValueStreamSHA256IRI implements ValidatingKeyValueStream {

    public final static String SHA_256_PATTERN_STRING = "hash://sha256/([a-fA-F0-9]){64}";
    public final static Pattern URI_PATTERN_HASH_URI_SHA_256_PATTERN = Pattern.compile(SHA_256_PATTERN_STRING);

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
                && URI_PATTERN_HASH_URI_SHA_256_PATTERN.matcher(sha256Hash).matches();
    }
}
