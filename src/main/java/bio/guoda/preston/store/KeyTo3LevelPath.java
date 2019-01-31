package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

public class KeyTo3LevelPath implements KeyToPath {
    @Override
    public String toPath(String key) {
        HashKeyUtil.validateHashKey(key);

        int offset = Hasher.getHashPrefix().length();
        String u0 = key.substring(offset + 0, offset + 2);
        String u1 = key.substring(offset + 2, offset + 4);
        return StringUtils.join(Arrays.asList(u0, u1, key.substring(offset)), "/");
    }

}
