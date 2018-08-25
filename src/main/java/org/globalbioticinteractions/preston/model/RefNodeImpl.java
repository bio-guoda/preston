package org.globalbioticinteractions.preston.model;

public abstract class RefNodeImpl implements RefNode {
    private final RefNode parent;
    private final RefNodeType type;
    private String id;

    public RefNodeImpl(RefNode parent, RefNodeType type) {
        this.parent = parent;
        this.type = type;
    }

    public RefNodeType getType() {
        return type;
    }

    public RefNode getParent() {
        return parent;
    }

    public String getId() { return id; }

    public void setId(String id) {
        this.id = id;
    }




}
