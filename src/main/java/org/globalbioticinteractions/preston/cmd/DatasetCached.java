package org.globalbioticinteractions.preston.cmd;

import org.apache.commons.io.FileUtils;
import org.globalbioticinteractions.preston.Dataset;
import org.globalbioticinteractions.preston.DatasetType;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class DatasetCached implements Dataset {
    private final Dataset dataset;
    private final String id;
    private final File dataFile;

    public DatasetCached(Dataset dataset, String id, File dataFile) {
        this.dataset = dataset;
        this.id = id;
        this.dataFile = dataFile;
    }

    public DatasetCached(Dataset dataset, String id) {
        this(dataset, id, null);
    }

    @Override
    public Dataset getParent() {
        return dataset.getParent();
    }

    @Override
    public DatasetType getType() {
        return dataset.getType();
    }

    @Override
    public InputStream getData() throws IOException {
        return null == dataFile ? dataset.getData() : FileUtils.openInputStream(dataFile);
    }

    @Override
    public String getLabel() {
        return dataset.getLabel();
    }

    @Override
    public String getId() {
        return id;
    }

}
