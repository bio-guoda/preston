package org.globalbioticinteractions.preston.model;

import java.net.URI;

public interface RefNode {

    String getLabel();

    URI getContentHash();

    boolean equivalentTo(RefNode node);
}

