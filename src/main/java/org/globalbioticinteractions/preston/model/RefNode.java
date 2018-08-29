package org.globalbioticinteractions.preston.model;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public interface RefNode {

    InputStream getData() throws IOException;

    String getLabel();

    URI getId();

    boolean equivalentTo(RefNode node);
}

