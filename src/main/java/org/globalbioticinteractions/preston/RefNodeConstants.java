package org.globalbioticinteractions.preston;

import org.apache.commons.rdf.api.IRI;
import org.globalbioticinteractions.preston.model.RefNodeFactory;
import org.globalbioticinteractions.preston.store.Predicate;

import java.net.URI;

public class RefNodeConstants {
    public static final IRI PUBLISHER_REGISTRY_OF = RefNodeFactory.toIRI("http://example.org/publisherRegistryOf");

    public static final IRI HAD_MEMBER = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#hadMember"));

    public static final IRI CONTINUATION_OF = RefNodeFactory.toIRI(URI.create("http://example.org/continuationOf"));
    public static final IRI SEED_OF = RefNodeFactory.toIRI(URI.create("http://example.org/seedOf"));

    public static final IRI SOFTWARE_AGENT = RefNodeFactory.toIRI(URI.create("https://preston.globalbioticinteractions.org"));

    public static final IRI HAS_FORMAT = RefNodeFactory.toIRI(URI.create("http://purl.org/dc/elements/1.1/format"));

    public static final IRI HAS_TYPE = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#type"));
    public static final IRI COLLECTION = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#collection"));
    public static final IRI GENERATED_AT_TIME = Predicate.GENERATED_AT_TIME;
}
