package bio.guoda.preston.process;

import org.apache.commons.rdf.api.IRI;

public class IRIExplodingProcessor implements IRIProcessor {

    @Override
    public IRI process(IRI iri) {
        if (iri != null) {
            String iriString = iri.getIRIString();
            if (StatementIRIProcessor.IS_UUID.test(iriString)) {
                throw new IllegalArgumentException("found relative uuid: ["+ iriString +"]");
            } else if (!StatementIRIProcessor.IS_ABSOLUTE_URI.test(iriString)) {
                throw new IllegalArgumentException("found relative uri: ["+ iriString +"]");
            }
        }

        return iri;
    }
}
