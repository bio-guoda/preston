package bio.guoda.preston.store;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;

import java.io.IOException;

public interface StatementStore extends StatementStoreReadOnly {

    void put(Pair<RDFTerm, RDFTerm> queryKey, RDFTerm value) throws IOException;


}
