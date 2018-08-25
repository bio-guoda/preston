package org.globalbioticinteractions.preston;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class DatasetString extends Dataset {
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

    @Override
    public String getId() {
        try {
            return CrawlerGBIF.calcSHA256(data);
        } catch (IOException e) {
            throw new IllegalStateException("unexpected failure of hashing of [" + data + "]", e);
        }
    }


}
