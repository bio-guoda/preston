package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
import org.apache.commons.lang3.StringUtils;

public class HashKeyUtil {
    public static void validateHashKey(String hashKey) {
        int offset = Hasher.getHashPrefix().length();
        int expectedLength = 8 + offset;
        if (StringUtils.length(hashKey) < expectedLength) {
            throw new IllegalArgumentException("expected id [" + hashKey + "] of at least [" + expectedLength + "] characters, instead got [" + hashKey.length() + "] characters.");
        }
    }
}
