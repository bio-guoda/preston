package bio.guoda.preston.model;

import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Triple;
import bio.guoda.preston.RefNodeConstants;
import org.junit.Test;

import java.io.IOException;

import static bio.guoda.preston.model.RefNodeFactory.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

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


    @Test
    public void recordGenerationTime() throws IOException {
        Literal dateTime = toDateTime("2018-10-25");
        assertNotNull(dateTime);
        assertThat(dateTime.toString(), is("\"2018-10-25\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"));
    }


}