package org.globalbioticinteractions.preston;

import org.apache.commons.rdf.api.IRI;
import org.globalbioticinteractions.preston.model.RefNodeFactory;

import java.net.URI;

public class RefNodeConstants {

    public static final IRI HAD_MEMBER = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#hadMember"));

    public static final IRI SOFTWARE_AGENT = RefNodeFactory.toIRI(URI.create("https://preston.globalbioticinteractions.org"));

    public static final IRI HAS_FORMAT = RefNodeFactory.toIRI(URI.create("http://purl.org/dc/elements/1.1/format"));

    public static final IRI HAS_TYPE = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#type"));
    public static final IRI COLLECTION = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#collection"));


    public static final IRI WAS_REVISION_OF = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#wasRevisionOf"));
    public static final IRI HAS_PREVIOUS_VERSION = RefNodeFactory.toIRI(URI.create("http://purl.org/pav/previousVersion"));

    public static final IRI WAS_DERIVED_FROM = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#wasDerivedFrom"));
    public static final IRI HAS_VERSION = RefNodeFactory.toIRI(URI.create("http://purl.org/pav/hasVersion"));

    public static final IRI GENERATED_AT_TIME = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#generatedAtTime"));
}
