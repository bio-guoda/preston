package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefNode;

import java.net.URI;

public class RefNodeFromResource implements RefNode {


    private final String resourceName;

    public RefNodeFromResource() {
        this("gbifdatasets.json");
    }


    public RefNodeFromResource(String resourceName) {
        this.resourceName = resourceName;
    }

    @Override
    public String toString() {
        return "label@" + resourceName;
    }

    @Override
    public URI getContentHash() {
        return URI.create("hash://some/" + resourceName);
    }

    @Override
    public boolean equals(RefNode node) {
        return false;
    }
}
