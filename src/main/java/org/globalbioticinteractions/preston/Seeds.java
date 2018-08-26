package org.globalbioticinteractions.preston;

import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefNodeType;

public final class Seeds {

    public final static RefNode SEED_NODE_GBIF = new RefNodeString(null, RefNodeType.URI, "https://gbif.org");
    public final static RefNode SEED_NODE_IDIGBIO = new RefNodeString(null, RefNodeType.URI, "https://idigbio.org");
}
