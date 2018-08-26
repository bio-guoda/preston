package org.globalbioticinteractions.preston.model;

import java.io.IOException;
import java.io.InputStream;

public class RefNodeProxy implements RefNode {
    private final RefNode refNode;

    public RefNodeProxy(RefNode refNode) {
        this.refNode = refNode;
    }

    @Override
    public RefNode getParent() {
        return this.refNode.getParent();
    }

    @Override
    public RefNodeType getType() {
        return this.refNode.getType();
    }

    @Override
    public InputStream getData() throws IOException {
        return this.refNode.getData();
    }

    @Override
    public String getLabel() {
        return this.refNode.getLabel();
    }

    @Override
    public String getId() {
        return this.refNode.getId();
    }

    @Override
    public Long getSize() {
        return this.refNode.getSize();
    }

    @Override
    public boolean equivalentTo(RefNode node) {
        return this.refNode.equivalentTo(node);
    }
}
