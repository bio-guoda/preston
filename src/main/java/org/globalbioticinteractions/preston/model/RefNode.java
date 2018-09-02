package org.globalbioticinteractions.preston.model;

import java.net.URI;

public interface RefNode {

    String toString();

    URI getContentHash();

    boolean equals(RefNode node);
}

