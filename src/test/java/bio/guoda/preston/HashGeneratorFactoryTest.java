package bio.guoda.preston;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class HashGeneratorFactoryTest {

    @Test
    public void createAll() throws IOException {
        for (HashType value : HashType.values()) {
            HashGenerator generator = new HashGeneratorFactory().create(value);
            assertNotNull(generator.hash(getClass().getResourceAsStream("/bio/guoda/preston/dwca-20180905.zip")));
        }
    }

}