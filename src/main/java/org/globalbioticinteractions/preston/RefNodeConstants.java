package org.globalbioticinteractions.preston;

import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefNodeType;
import org.globalbioticinteractions.preston.store.Predicate;

public class RefNodeConstants {
    public static final RefNodeString DATASET_REGISTRY_OF = new RefNodeString(RefNodeType.URI, "http://example.org/registryOf");
    public static final RefNodeString PUBLISHER_REGISTRY_OF = new RefNodeString(RefNodeType.URI, "http://example.org/publisherRegistryOf");
    public static final RefNodeString HAS_PART = new RefNodeString(RefNodeType.URI, "http://example.org/hasPart");
    public static final RefNodeString HAS_FEED = new RefNodeString(RefNodeType.URI, "http://example.org/hasFeed");
    public static final RefNodeString SEED_OF = new RefNodeString(RefNodeType.URI, "http://example.org/seedOf");
    public static final RefNodeString HAS_CONTENT = new RefNodeString(RefNodeType.URI, Predicate.HAS_CONTENT.toString());
    public static final RefNodeString DEREFERENCE_OF = HAS_CONTENT;

    public static final RefNodeString SEED_ROOT = new RefNodeString(RefNodeType.URI, "https://preston.globalbioticinteractions.org");
}
