package org.globalbioticinteractions.preston;

import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefNodeType;

public class RefNodeConstants {
    public static final RefNodeString DEREFERENCE_OF = new RefNodeString(RefNodeType.URI, "http://example.org/dereferenceOf");
    public static final RefNodeString DATASET_REGISTRY_OF = new RefNodeString(RefNodeType.URI, "http://example.org/registryOf");
    public static final RefNodeString PUBLISHER_REGISTRY_OF = new RefNodeString(RefNodeType.URI, "http://example.org/publisherRegistryOf");
    public static final RefNodeString PART_OF = new RefNodeString(RefNodeType.URI, "http://example.org/partOf");
    public static final RefNodeString FEED_OF = new RefNodeString(RefNodeType.URI, "http://example.org/feedOf");
    public static final RefNodeString SEED_OF = new RefNodeString(RefNodeType.URI, "http://example.org/seedOf");

    public static final RefNodeString SEED_ROOT = new RefNodeString(RefNodeType.URI, "https://preston.globalbioticinteractions.org");
}
