package org.globalbioticinteractions.preston.model;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class RefNodeProxyData extends RefNodeProxy {
    private final String id;
    private final File dataFile;

    public RefNodeProxyData(RefNode refNode, String id, File dataFile) {
        super(refNode);
        this.id = id;
        this.dataFile = dataFile;
    }

    public RefNodeProxyData(RefNode refNode, String id) {
        this(refNode, id, null);
    }

    @Override
    public InputStream getData() throws IOException {
        return null == dataFile ? super.getData() : FileUtils.openInputStream(dataFile);
    }

    @Override
    public Long getSize() {
        return dataFile != null && dataFile.exists() ? dataFile.length() : null;
    }

    @Override
    public String getId() {
        return id;
    }

}
