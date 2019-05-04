package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;

public class KeyTo1LevelPath implements KeyToPath {
    @Override
    public String toPath(String key) {
        HashKeyUtil.validateHashKey(key);
        int offset = Hasher.getHashPrefix().length();
        return key.substring(offset);
    }

}
