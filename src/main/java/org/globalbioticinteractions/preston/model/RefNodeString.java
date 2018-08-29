package org.globalbioticinteractions.preston.model;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.globalbioticinteractions.preston.Hasher;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class RefNodeString implements RefNode {
    private final String content;
    private URI hash;

    public RefNodeString(String content) {
        super();
        this.content = content;
    }

    @Override
    public InputStream getContent() throws IOException {
        return IOUtils.toInputStream(content, StandardCharsets.UTF_8);
    }

    @Override
    public String getLabel() {
        return content;
    }

    @Override
    public URI getContentHash() {
        if (hash == null) {
            hash = Hasher.calcSHA256(content);
        }
        return hash;
    }

    @Override
    public boolean equivalentTo(RefNode other) {
        return other instanceof RefNodeString
                && StringUtils.equals(content, ((RefNodeString) other).content);
    }

}
