package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;

public class IRIFixingProcessor implements IRIProcessor {

    @Override
    public IRI process(IRI iri) {
        IRI fixedIRI = iri;
        if (iri != null) {
            String iriString = iri.getIRIString();
            String updatedString;
            if (StatementIRIProcessor.IS_UUID.test(iriString)) {
                updatedString = "urn:uuid:" + iriString;
                fixedIRI = RefNodeFactory.toIRI(updatedString);
            } else if (!StatementIRIProcessor.IS_ABSOLUTE_URI.test(iriString)) {
                updatedString = "urn:example:" + iriString;
                fixedIRI = RefNodeFactory.toIRI(updatedString);
            }
        }

        return fixedIRI;
    }
}
