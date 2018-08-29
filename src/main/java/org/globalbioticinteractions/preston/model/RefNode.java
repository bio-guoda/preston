package org.globalbioticinteractions.preston.model;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public interface RefNode {

    InputStream getContent() throws IOException;

    String getLabel();

    URI getContentHash();

    boolean equivalentTo(RefNode node);
}

