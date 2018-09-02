package org.globalbioticinteractions.preston.store;

import org.apache.commons.rdf.api.IRI;
import org.globalbioticinteractions.preston.model.RefNodeFactory;

import java.net.URI;

public final class Predicate {
    public static final IRI WAS_REVISION_OF = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#wasRevisionOf"));
    public static final IRI WAS_DERIVED_FROM = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#wasDerivedFrom"));
    public static final IRI GENERATED_AT_TIME = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#generatedAtTime"));
}
