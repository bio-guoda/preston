package org.globalbioticinteractions.preston.model;

import java.io.IOException;
import java.io.InputStream;

public interface RefNode {

    InputStream getData() throws IOException;

    String getLabel();

    String getId();

    boolean equivalentTo(RefNode node);
}

