package bio.guoda.preston.process;


import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

public class RegistryReaderEPPOIT {
    @Test
    public void eppoAuthGetRestrictedContent() throws IOException {
        InputStream resourceAsStream = getClass().getResourceAsStream("eppo-token.hidden");
        assertNotNull(resourceAsStream);
        System.setProperty("EPPO_TOKEN", IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8));
        try (InputStream is
                     = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI(URI.create("https://api.eppo.int/gd/v2/taxons/list")))) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            IOUtils.copy(is, outputStream);

            JsonNode taxons = new ObjectMapper().readTree(outputStream.toByteArray());

            assertThat(taxons.at("/data").isArray(), Is.is(true));

        }

    }

}