package org.globalbioticinteractions.preston;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.RDFTerm;

public class RDFUtil {
    public static String getValueFor(RDFTerm entity) {
        String value = null;
        if (entity instanceof IRI) {
            value = ((IRI) entity).getIRIString();
        } else if (entity instanceof Literal){
            value = ((Literal) entity).getLexicalForm();
        }
        value = (value == null) ? entity.toString() : value;
        return value;
    }
}
