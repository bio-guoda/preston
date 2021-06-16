package bio.guoda.preston;

import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

public class HashGeneratorFactoryTest {

    @Test
    public void createAll() throws IOException {
        for (HashType value : HashType.values()) {
            HashGenerator<IRI> generator = new HashGeneratorFactory().create(value);
            assertNotNull(generator.hash(getClass().getResourceAsStream("/bio/guoda/preston/dwca-20180905.zip")));
        }
    }

}