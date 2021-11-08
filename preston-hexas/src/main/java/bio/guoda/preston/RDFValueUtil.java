package bio.guoda.preston;

import bio.guoda.preston.Hasher;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.RDFTerm;

public class RDFValueUtil {

    public static String getValueFor(RDFTerm entity) {
        String value = null;
        if (entity instanceof IRI) {
            value = ((IRI) entity).getIRIString();
        } else if (entity instanceof Literal) {
            value = ((Literal) entity).getLexicalForm();
        }
        value = (value == null) ? entity.toString() : value;
        return value;
    }


}
