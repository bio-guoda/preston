package org.globalbioticinteractions.preston.model;

import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.junit.Test;

import static org.globalbioticinteractions.preston.model.RefNodeFactory.*;
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
        assertFalse(hasDerivedContentAvailable(statement));
    }


}