package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;

public class HashKeyUtil {

    public static final int EXPECTED_LENGTH = 64 + Hasher.getHashPrefix().length();

    public static void validateHashKey(IRI hashKey) {
        if (!isValidHashKey(hashKey)) {
            throw new IllegalArgumentException("expected id [" + hashKey + "] of [" + EXPECTED_LENGTH + "] characters, instead got [" + hashKey.getIRIString().length() + "] characters.");
        }
    }

    public static boolean isValidHashKey(IRI hashKey) {
        return StringUtils.length(hashKey.getIRIString()) == EXPECTED_LENGTH;
    }

    public static URI insertSlashIfNeeded(URI uri, String suffix) {
        String baseURI = uri.toString();
        return StringUtils.endsWith(baseURI, "/") ? URI.create(baseURI + suffix) : URI.create(baseURI + "/" + suffix);
    }
}
