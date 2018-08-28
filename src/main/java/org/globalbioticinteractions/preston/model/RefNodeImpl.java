package org.globalbioticinteractions.preston.model;

public abstract class RefNodeImpl implements RefNode {
    private final RefNodeType type;

    public RefNodeImpl(RefNodeType type) {
        this.type = type;
    }

    @Override
    public RefNodeType getType() {
        return type;
    }

    @Override
    public String getId() { return null; }

    @Override
    public Long getSize() {
        return null;
    }

}
