package org.globalbioticinteractions.preston.store;

import org.apache.commons.io.IOUtils;
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
    public URI putBlob(InputStream is) throws IOException {
        return URI.create(persistence.put((is1, os1)-> {
            URI key = Hasher.calcSHA256(is1, os1);
            return key.toString();
        }, is));
    }

    @Override
    public URI putBlob(URI entity) throws IOException {
        return putBlob(IOUtils.toInputStream(entity.toString(), StandardCharsets.UTF_8));
    }

    @Override
    public InputStream get(URI key) throws IOException {
        return key == null ? null : persistence.get(key.toString());
    }

}
