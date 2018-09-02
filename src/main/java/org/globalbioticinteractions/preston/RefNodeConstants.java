package org.globalbioticinteractions.preston;

import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeFactory;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefNodeURI;
import org.globalbioticinteractions.preston.store.Predicate;

import java.net.URI;

public class RefNodeConstants {
    public static final RefNode PUBLISHER_REGISTRY_OF = RefNodeFactory.toURI("http://example.org/publisherRegistryOf");

    public static final RefNode HAD_MEMBER = RefNodeFactory.toURI(URI.create("http://www.w3.org/ns/prov#hadMember"));

    public static final RefNode CONTINUATION_OF = RefNodeFactory.toURI(URI.create("http://example.org/continuationOf"));
    public static final RefNode SEED_OF = RefNodeFactory.toURI(URI.create("http://example.org/seedOf"));

    public static final RefNode WAS_DERIVED_FROM = RefNodeFactory.toURI(Predicate.WAS_DERIVED_FROM);
    public static final RefNode WAS_REVISION_OF = RefNodeFactory.toURI(Predicate.WAS_REVISION_OF);

    public static final RefNode SOFTWARE_AGENT = RefNodeFactory.toURI(URI.create("https://preston.globalbioticinteractions.org"));

    public static final RefNode HAS_FORMAT = RefNodeFactory.toURI(URI.create("http://purl.org/dc/elements/1.1/format"));

    public static final RefNode HAS_TYPE = RefNodeFactory.toURI(URI.create("http://www.w3.org/ns/prov#type"));
    public static final RefNode COLLECTION = RefNodeFactory.toURI(URI.create("http://www.w3.org/ns/prov#collection"));
    public static final RefNode GENERATED_AT_TIME = RefNodeFactory.toURI(URI.create(Predicate.GENERATED_AT_TIME.toString()));
}
