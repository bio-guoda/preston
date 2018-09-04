package bio.guoda.preston.model;

import org.apache.commons.rdf.api.Triple;
import bio.guoda.preston.RefNodeConstants;
import org.junit.Test;

import static bio.guoda.preston.model.RefNodeFactory.*;
import static org.junit.Assert.assertFalse;

public class RefNodeFactoryTest {

    @Test(expected = NullPointerException.class)
    public void createNullTriple() {
        toStatement(null, null, null);
    }

    @Test
    public void hasContent() {
        Triple statement = toStatement(toIRI("http://some"),
                RefNodeConstants.HAS_VERSION,
                toBlank());
        assertFalse(hasVersionAvailable(statement));
    }


}