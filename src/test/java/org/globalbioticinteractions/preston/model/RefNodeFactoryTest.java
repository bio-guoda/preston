package org.globalbioticinteractions.preston.model;

import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class RefNodeFactoryTest {

    @Test(expected = NullPointerException.class)
    public void createNullTriple() {
        RefNodeFactory.toStatement(null, null, null);
    }

    @Test
    public void hasContent() {
        Triple statement = RefNodeFactory.toStatement(RefNodeFactory.toBlank(),
                RefNodeConstants.WAS_DERIVED_FROM,
                RefNodeFactory.toIRI("http://some"));
        assertFalse(RefNodeFactory.hasDerivedContentAvailable(statement));
    }


}