package org.globalbioticinteractions.preston;

import org.apache.commons.rdf.api.IRI;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeFactory;

public final class Seeds {

    public final static IRI SEED_NODE_GBIF = RefNodeFactory.toIRI("https://gbif.org");
    public final static IRI SEED_NODE_IDIGBIO = RefNodeFactory.toIRI("https://idigbio.org");
    public final static IRI SEED_NODE_BIOCASE = RefNodeFactory.toIRI("http://biocase.org");
}
