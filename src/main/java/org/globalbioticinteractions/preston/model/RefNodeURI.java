package org.globalbioticinteractions.preston.model;

import java.net.URI;

public class RefNodeURI implements RefNode {

    private final URI key;

    RefNodeURI(URI key) {
        this.key = key;
    }

    @Override
    public String toString() {
        return getContentHash().toString();
    }

    @Override
    public URI getContentHash() {
        return key;
    }

    @Override
    public boolean equals(RefNode node) {
        URI id = getContentHash();
        URI otherId = node == null ? null : node.getContentHash();
        return id != null && id.equals(otherId);
    }
}
