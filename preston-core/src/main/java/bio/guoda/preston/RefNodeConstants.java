package bio.guoda.preston;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;

import java.net.URI;
import java.util.UUID;

import static bio.guoda.preston.RefNodeFactory.toIRI;

public class RefNodeConstants {

    public static final String URN_UUID_PREFIX = "urn:uuid:";

    public static final IRI HAD_MEMBER = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#hadMember"));
    public static final IRI SEE_ALSO = RefNodeFactory.toIRI(URI.create("http://www.w3.org/1999/02/22-rdf-syntax-ns#seeAlso"));

    public static final String PRESTON_URI = "https://preston.guoda.bio";

    public static final IRI PRESTON = RefNodeFactory.toIRI(URI.create(PRESTON_URI));

    public static final IRI HAS_FORMAT = RefNodeFactory.toIRI(URI.create("http://purl.org/dc/elements/1.1/format"));

    public static final IRI HAS_TYPE = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#type"));

    public static final IRI HAS_VALUE = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#value"));

    public static final IRI HAS_VERSION = RefNodeFactory.toIRI(URI.create("http://purl.org/pav/hasVersion"));
    public static final IRI HAS_PREVIOUS_VERSION = RefNodeFactory.toIRI(URI.create("http://purl.org/pav/previousVersion"));

    public static final IRI GENERATED_AT_TIME = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#generatedAtTime"));
    public static final IRI WAS_GENERATED_BY = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#wasGeneratedBy"));
    public static final IRI WAS_DERIVED_FROM = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#wasDerivedFrom"));
    public static final IRI QUALIFIED_GENERATION = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#qualifiedGeneration"));


    public static final String BIODIVERSITY_DATASET_GRAPH_UUID_STRING = "0659a54f-b713-4f86-a917-5be166a14110";
    public static final UUID BIODIVERSITY_DATASET_GRAPH_UUID = UUID.fromString(BIODIVERSITY_DATASET_GRAPH_UUID_STRING);
    public static final IRI BIODIVERSITY_DATASET_GRAPH = toIRI(BIODIVERSITY_DATASET_GRAPH_UUID);


    public static final IRI BIODIVERSITY_DATASET_GRAPH_URN_UUID = toIRI(URN_UUID_PREFIX + BIODIVERSITY_DATASET_GRAPH_UUID_STRING);

    // Provenance Root Query is the starting point of any biodiversity dataset graph
    // for backwards compatibility, this root query is calculated from the *bare*, none URN uuid,
    // of the Biodiversity Dataset Graph Concept UUID.
    public static final Pair<RDFTerm, RDFTerm> PROVENANCE_ROOT_QUERY = Pair.of(BIODIVERSITY_DATASET_GRAPH, HAS_VERSION);
    public static final String PROVENANCE_ROOT_QUERY_HASH = "hash://sha256/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a";
    public static final IRI PROVENANCE_ROOT_QUERY_HASH_URI = toIRI(PROVENANCE_ROOT_QUERY_HASH);

    public static final IRI STARTED_AT_TIME = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#startedAtTime"));
    public static final IRI ENDED_AT_TIME = RefNodeFactory.toIRI(URI.create("http://www.w3.org/ns/prov#endedAtTime"));

    public static final IRI USED_BY = toIRI("http://www.w3.org/ns/prov#usedBy");
    public static final IRI AGENT = toIRI("http://www.w3.org/ns/prov#Agent");
    public static final IRI SOFTWARE_AGENT = toIRI("http://www.w3.org/ns/prov#SoftwareAgent");
    public static final IRI DESCRIPTION = toIRI("http://purl.org/dc/terms/description");
    public static final IRI COLLECTION = toIRI("http://www.w3.org/ns/prov#Collection");
    public static final IRI ORGANIZATION = toIRI("http://www.w3.org/ns/prov#Organization");
    public static final IRI WAS_ASSOCIATED_WITH = toIRI("http://www.w3.org/ns/prov#wasAssociatedWith");
    public static final IRI IS_A = toIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    public static final IRI CREATED_BY = toIRI("http://purl.org/pav/createdBy");
    public static final IRI WAS_INFORMED_BY = toIRI("http://www.w3.org/ns/prov#wasInformedBy");
    public static final IRI ENTITY = toIRI("http://www.w3.org/ns/prov#Entity");
    public static final IRI ACTIVITY = toIRI("http://www.w3.org/ns/prov#Activity");
    public static final IRI USED = toIRI("http://www.w3.org/ns/prov#used");
    public static final IRI OVERLAPS = toIRI("http://purl.obolibrary.org/obo/RO_0002131");
    public static final IRI WAS_STARTED_BY = toIRI("http://www.w3.org/ns/prov#wasStartedBy");

    public static final IRI DEPICTS = toIRI("http://xmlns.com/foaf/0.1/depicts");

    public static final String BLOOM_HASH_PREFIX = "gz:bloom:";
    public static final String THETA_SKETCH_PREFIX = "theta:";

    public static final IRI STATISTICAL_ERROR = RefNodeFactory.toIRI("http://purl.obolibrary.org/obo/STATO_0000242");
    public static final IRI CONFIDENCE_INTERVAL_95 = RefNodeFactory.toIRI("http://purl.obolibrary.org/obo/STATO_0000231");
}
