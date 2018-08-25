package org.globalbioticinteractions.preston;

import java.io.IOException;
import java.io.InputStream;

interface IDataset {

    Dataset getParent();

    DatasetType getType();

    InputStream getData() throws IOException;

    String getLabel();

    String getId();
}

public abstract class Dataset implements IDataset {
    private final Dataset parent;
    private final DatasetType type;
    private String id;

    public Dataset(Dataset parent, DatasetType type) {
        this.parent = parent;
        this.type = type;
    }

    public DatasetType getType() {
        return type;
    }

    public Dataset getParent() {
        return parent;
    }

    public String getId() { return id; }

    public void setId(String id) {
        this.id = id;
    }

}
