package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

public class KeyTo5LevelPath implements KeyToPath {
    @Override
    public String toPath(String key) {
        int offset = Hasher.getHashPrefix().length();
        int expectedLength = 8 + offset;
        if (StringUtils.length(key) < expectedLength) {
            throw new IllegalArgumentException("expected id [" + key + "] of at least [" + expectedLength + "] characters");
        }
        String u0 = key.substring(offset + 0, offset + 2);
        String u1 = key.substring(offset + 2, offset + 4);
        String u2 = key.substring(offset + 4, offset + 6);
        return StringUtils.join(Arrays.asList(u0, u1, u2, key.substring(offset), "data"), "/");
    }
}
