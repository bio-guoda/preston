package org.globalbioticinteractions.preston.model;

import java.io.IOException;
import java.io.InputStream;

public interface RefNode {

    RefNodeType getType();

    InputStream getData() throws IOException;

    String getLabel();

    String getId();

    Long getSize();

    boolean equivalentTo(RefNode node);
}

