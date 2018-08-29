package org.globalbioticinteractions.preston.model;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.globalbioticinteractions.preston.Hasher;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class RefNodeString implements RefNode {
    private final String data;
    private URI id;

    public RefNodeString(String data) {
        super();
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
    public URI getId() {
        if (id == null) {
            id = Hasher.calcSHA256(data);
        }
        return id;
    }

    @Override
    public boolean equivalentTo(RefNode other) {
        return other instanceof RefNodeString
                && StringUtils.equals(data, ((RefNodeString) other).data);
    }

}
