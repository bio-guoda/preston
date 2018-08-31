package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefNode;

import java.io.IOException;
import java.io.InputStream;
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
    public InputStream getContent() throws IOException {
        return getClass().getResourceAsStream(resourceName);
    }

    @Override
    public String getLabel() {
        return "label@" + resourceName;
    }

    @Override
    public URI getContentHash() {
        return URI.create("hash://some/" + resourceName);
    }

    @Override
    public boolean equivalentTo(RefNode node) {
        return false;
    }
}
