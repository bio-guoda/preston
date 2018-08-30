package org.globalbioticinteractions.preston;

import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.store.Predicate;

public class RefNodeConstants {
    public static final RefNodeString DATASET_REGISTRY_OF = new RefNodeString("http://example.org/registryOf");
    public static final RefNodeString PUBLISHER_REGISTRY_OF = new RefNodeString("http://example.org/publisherRegistryOf");
    public static final RefNodeString HAS_PART = new RefNodeString("http://example.org/hasPart");
    public static final RefNodeString CONTINUED_AT = new RefNodeString("http://example.org/continuedAt");
    public static final RefNodeString HAS_FEED = new RefNodeString("http://example.org/hasFeed");
    public static final RefNodeString SEED_OF = new RefNodeString("http://example.org/seedOf");
    public static final RefNodeString HAS_CONTENT = new RefNodeString(Predicate.HAS_CONTENT.toString());

    public static final RefNodeString SEED_ROOT = new RefNodeString("https://preston.globalbioticinteractions.org");
    public static final RefNodeString HAS_FORMAT = new RefNodeString("http://purl.org/dc/elements/1.1/format");
}
