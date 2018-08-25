package org.globalbioticinteractions.preston.model;

import org.globalbioticinteractions.preston.Resources;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;

public class RefNodeURI extends RefNodeImpl {
    private final URI dataURI;

    public RefNodeURI(RefNode parent, RefNodeType type, URI uri) {
        super(parent, type);
        this.dataURI = uri;
    }

    @Override
    public InputStream getData() throws IOException {
        return Resources.asInputStream(this.dataURI);
    }

    @Override
    public String getLabel() {
        return "data@" + dataURI.toString();
    }

    @Override
    public boolean equivalentTo(RefNode other) {
        return other instanceof RefNodeURI
                && Objects.equals(getType(), other.getType())
                && Objects.equals(dataURI, ((RefNodeURI) other).dataURI);
    }


}
