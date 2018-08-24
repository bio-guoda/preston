package org.globalbioticinteractions.preston;

import java.net.URI;
import java.util.UUID;

public class Dataset {
    private final UUID uuid;
    private final URI url;
    private final DatasetType type;

    public Dataset(UUID uuid, URI url, DatasetType type) {
        this.uuid = uuid;
        this.url = url;
        this.type = type;
    }

    public UUID getUuid() {
        return uuid;
    }

    public URI getUrl() {
        return url;
    }

    public DatasetType getType() {
        return type;
    }
}
