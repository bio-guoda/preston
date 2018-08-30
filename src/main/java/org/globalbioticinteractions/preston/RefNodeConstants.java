package org.globalbioticinteractions.preston;

import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.store.Predicate;

public class RefNodeConstants {
    public static final RefNodeString DATASET_REGISTRY_OF = new RefNodeString("http://example.org/registryOf");
    public static final RefNodeString PUBLISHER_REGISTRY_OF = new RefNodeString("http://example.org/publisherRegistryOf");

    public static final RefNodeString HAD_MEMBER = new RefNodeString("http://www.w3.org/ns/prov#hadMember");

    public static final RefNodeString CONTINUED_AT = new RefNodeString("http://example.org/continuedAt");
    public static final RefNodeString HAS_FEED = new RefNodeString("http://example.org/hasFeed");
    public static final RefNodeString SEED_OF = new RefNodeString("http://example.org/seedOf");

    public static final RefNodeString WAS_DERIVED_FROM = new RefNodeString(Predicate.WAS_DERIVED_FROM.toString());

    public static final RefNodeString SOFTWARE_AGENT = new RefNodeString("https://preston.globalbioticinteractions.org");

    public static final RefNodeString HAS_FORMAT = new RefNodeString("http://purl.org/dc/elements/1.1/format");

    public static final RefNodeString HAS_TYPE = new RefNodeString("http://www.w3.org/ns/prov#type");
    public static final RefNodeString COLLECTION = new RefNodeString("http://www.w3.org/ns/prov#collection");
}
