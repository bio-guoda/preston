package org.globalbioticinteractions.preston;

import org.apache.commons.rdf.api.IRI;
import org.globalbioticinteractions.preston.model.RefNodeFactory;

import java.net.URI;
import java.util.UUID;

import static org.globalbioticinteractions.preston.model.RefNodeFactory.toIRI;

public class RefNodeConstants {

    public static final IRI HAD_MEMBER = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#hadMember"));

    public static final String PRESTON_URI = "https://preston.guoda.org";

    public static final IRI PRESTON = RefNodeFactory.toIRI(URI.create(PRESTON_URI));


    public static final IRI HAS_FORMAT = RefNodeFactory.toIRI(URI.create("http://purl.org/dc/elements/1.1/format"));

    public static final IRI HAS_TYPE = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#type"));
    public static final IRI COLLECTION = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#collection"));

    public static final IRI HAS_VERSION = RefNodeFactory.toIRI(URI.create("http://purl.org/pav/hasVersion"));
    public static final IRI HAS_PREVIOUS_VERSION = RefNodeFactory.toIRI(URI.create("http://purl.org/pav/previousVersion"));

    public static final IRI GENERATED_AT_TIME = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#generatedAtTime"));

    public static final UUID ARCHIVE_COLLECTION = UUID.fromString("0659a54f-b713-4f86-a917-5be166a14110");
    public static final UUID GRAPH_COLLECTION = UUID.fromString("2c1946b9-0871-42fb-8eef-580b16d17294");

    public static final IRI USED_BY = toIRI("http://www.w3.org/ns/prov#usedBy");
}
