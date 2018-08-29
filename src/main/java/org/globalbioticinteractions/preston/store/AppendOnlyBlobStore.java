package org.globalbioticinteractions.preston.store;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.globalbioticinteractions.preston.Hasher;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;


public class AppendOnlyBlobStore implements BlobStore {

    private final Persistence persistence;

    public AppendOnlyBlobStore(Persistence persistence) {
        this.persistence = persistence;
    }

    // write-once, read-many

    @Override
    public String putBlob(InputStream is) throws IOException {
        return persistence.put(Hasher::calcSHA256, is);
    }

    @Override
    public String putBlob(URI entity) throws IOException {
        return putBlob(IOUtils.toInputStream(entity.toString(), StandardCharsets.UTF_8));
    }

    @Override
    public InputStream get(String key) throws IOException {
        String scrubbedKey = StringUtils.replace(key, "preston:", "");
        return key == null ? null : persistence.get(scrubbedKey);
    }

}
