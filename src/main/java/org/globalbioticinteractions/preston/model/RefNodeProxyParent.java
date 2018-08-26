package org.globalbioticinteractions.preston.model;

public class RefNodeProxyParent extends RefNodeProxy {
    private final RefNode parentCached;

    public RefNodeProxyParent(RefNode parentCached, RefNode refNode) {
        super(refNode);
        this.parentCached = parentCached;
    }

    @Override
    public RefNode getParent() {
        return this.parentCached;
    }

}
