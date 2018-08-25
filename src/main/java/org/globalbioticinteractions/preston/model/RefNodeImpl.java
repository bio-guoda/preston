package org.globalbioticinteractions.preston.model;

public abstract class RefNodeImpl implements RefNode {
    private final RefNode parent;
    private final RefNodeType type;

    public RefNodeImpl(RefNode parent, RefNodeType type) {
        this.parent = parent;
        this.type = type;
    }

    @Override
    public RefNodeType getType() {
        return type;
    }

    @Override
    public RefNode getParent() {
        return parent;
    }

    @Override
    public String getId() { return null; }

    @Override
    public Long getSize() {
        return null;
    }




}
