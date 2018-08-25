package org.globalbioticinteractions.preston.model;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class RefNodeCached implements RefNode {
    private final RefNode refNode;
    private final String id;
    private final File dataFile;

    public RefNodeCached(RefNode refNode, String id, File dataFile) {
        this.refNode = refNode;
        this.id = id;
        this.dataFile = dataFile;
    }

    public RefNodeCached(RefNode refNode, String id) {
        this(refNode, id, null);
    }

    @Override
    public RefNode getParent() {
        return refNode.getParent();
    }

    @Override
    public RefNodeType getType() {
        return refNode.getType();
    }

    @Override
    public InputStream getData() throws IOException {
        return null == dataFile ? refNode.getData() : FileUtils.openInputStream(dataFile);
    }

    @Override
    public Long getSize() {
        return dataFile != null && dataFile.exists() ? dataFile.length() : null;
    }

    @Override
    public String getLabel() {
        return refNode.getLabel();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean equivalentTo(RefNode node) {
        return refNode.equivalentTo(node);
    }

}
