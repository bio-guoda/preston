package org.globalbioticinteractions.preston.model;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.globalbioticinteractions.preston.Hasher;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class RefNodeString extends RefNodeImpl {
    private final String data;
    private String id;

    public RefNodeString(RefNodeType type, String data) {
        super(type);
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
    public Long getSize() {
        return data == null ? null : (long) data.getBytes().length;
    }

    @Override
    public String getId() {
        if (id == null) {
            id = Hasher.calcSHA256(data);
        }
        return id;
    }

    @Override
    public boolean equivalentTo(RefNode other) {
        return other instanceof RefNodeString
                && Objects.equals(getType(), other.getType())
                && StringUtils.equals(data, ((RefNodeString) other).data);
    }

}
