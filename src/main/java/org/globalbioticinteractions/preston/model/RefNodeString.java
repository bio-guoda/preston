package org.globalbioticinteractions.preston.model;

import org.apache.commons.lang3.StringUtils;
import org.globalbioticinteractions.preston.Hasher;

import java.net.URI;

public class RefNodeString implements RefNode {
    private final String content;
    private URI hash;

    RefNodeString(String content) {
        super();
        this.content = content;
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
        return other != null
                && other instanceof RefNodeString
                && StringUtils.isNotBlank(content)
                && StringUtils.equals(content, ((RefNodeString) other).content);
    }

}
