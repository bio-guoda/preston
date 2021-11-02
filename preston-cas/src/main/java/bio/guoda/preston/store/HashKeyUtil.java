package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HashKeyUtil {

    public static final int EXPECTED_LENGTH = 64 + Hasher.getHashPrefix().length();

    public static final Pattern URI_PATTERN_HASH_URI_COMPOSITE = Pattern
            .compile("([a-zA-Z0-9]+:)*(" + ValidatingKeyValueStreamSHA256IRI.SHA_256_PATTERN_STRING + "){1}(!/.*){0,1}");


    public static void validateHashKey(IRI hashKey) {
        if (!isValidHashKey(hashKey)) {
            throw new IllegalArgumentException("expected id [" + hashKey + "] of [" + EXPECTED_LENGTH + "] characters, instead got [" + hashKey.getIRIString().length() + "] characters.");
        }
    }

    public static boolean isValidHashKey(IRI hashKey) {
        Matcher matcher
                = ValidatingKeyValueStreamSHA256IRI
                .URI_PATTERN_HASH_URI_SHA_256_PATTERN
                .matcher(hashKey.getIRIString());

        return matcher.matches();
    }


    static URI insertSlashIfNeeded(URI uri, String suffix) {
        String baseURI = uri.toString();
        return StringUtils.endsWith(baseURI, "/")
                ? URI.create(baseURI + suffix)
                : URI.create(baseURI + "/" + suffix);
    }

    public static boolean isLikelyCompositeHashURI(IRI comboHashURI) {
        return comboHashURI != null
                && URI_PATTERN_HASH_URI_COMPOSITE
                .matcher(comboHashURI.getIRIString()).matches();
    }
}
