package org.globalbioticinteractions.preston;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class DatasetString extends DatasetImpl {
    private final String data;

    public DatasetString(Dataset parent, DatasetType type, String data) {
        super(parent, type);
        this.data = data;
    }

    @Override
    public InputStream getData() throws IOException {
        return IOUtils.toInputStream(data, StandardCharsets.UTF_8);
    }

    @Override
    public String getLabel() {
        return data;
    }

}
