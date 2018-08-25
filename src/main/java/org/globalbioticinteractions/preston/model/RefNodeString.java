package org.globalbioticinteractions.preston.model;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class RefNodeString extends RefNodeImpl {
    private final String data;

    public RefNodeString(RefNode parent, RefNodeType type, String data) {
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
    public boolean equivalentTo(RefNode other) {
        return other instanceof RefNodeString
                && Objects.equals(getType(), other.getType())
                && StringUtils.equals(data, ((RefNodeString)other).data);
    }

}
