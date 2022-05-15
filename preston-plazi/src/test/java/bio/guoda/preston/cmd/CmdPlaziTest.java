package bio.guoda.preston.cmd;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class CmdPlaziTest {


    @Ignore
    @Test
    public void processXML() throws IOException {
        InputStream is = getClass().getResourceAsStream("03D587F2FFC94C03F8F13AECFBD8F765.xml");

        String s = IOUtils.toString(is, StandardCharsets.UTF_8);

        assertThat(s, containsString("Taphozous troughtoni"));

        JsonNode jsonNode = expectedResponse();

        for (JsonNode node : jsonNode) {
            assertThat(s, containsString(node.asText()));
        }

    }


    public JsonNode expectedResponse() throws JsonProcessingException {
        return new ObjectMapper().readTree("{\n" +
                "  \"taxonID\": \"03D587F2FFC94C03F8F13AECFBD8F765\",\n" +
                "  \"Family\": \"Emballonuridae\",\n" +
                "  \"Genus\": \"Taphozous\",\n" +
                "  \"Species\": \"troughtoni\",\n" +
                "  \"taxonRank\": \"species\",\n" +
                "  \"scientificNameAuthorityName\": \"Tate\",\n" +
                "  \"scientificNameAuthorityYear\": \"1952\",\n" +
                "  \"canonicalName\": \"Taphozous troughtoni\",\n" +
                "  \"verbatimScientificName\": \"Taphozous troughtoni\",\n" +
                "  \"references\": \"http://treatment.plazi.org/id/03D587F2FFC94C03F8F13AECFBD8F765\",\n" +
                "  \"common name\": \"Troughton’s Sheath-tailed Bat\",\n" +
                //"  \"french\": \"Taphien deTroughton\",\n" +
                "  \"german\": \"Troughton-Grabfledermaus\",\n" +
                //"  \"spanish\": \"Tafozo de Troughton\",\n" +
                //"  \"taxonomy text\": \"Taphozous troughtoni Tate, 1952 , “ Rifle Creek, Mt. Isa, northwest Queensland,” Australia .\\r \\r Taphozous troughtoni is in the subgenus Taphozous . It was considered ajunior synonym of T georgianus , but. T. Chimimba and D. J. Kitchener in 1991 raised it to a distinct species. Monotypic.\",\n" +
                "  \"Distribution\": \"NE Australia endemic, in WC, C & E Queensland.\",\n" +
                "  \"Descriptive notes\": \"Head-body 79-4-86-3 mm, tail 31-5-36-9 mm, ear 22-4-27-1 mm, hindfoot 9-8-10-3 mm, forearm 73-76 mm; weight. 20-29 g. Dorsum of Troughton’s Sheath-tailed Bat is predominately olive\u00AD brown, with pale mouse-gray guard hairs. Venter surface hairs are olive-brown from chin to shoulders and posteriorly dark yellow-brown, with guard hairs of pale mouse-gray. Uropatagium close to abdomen is heavily furred. Throat pouches are absent, and radio-metacarpal sacs are present in both sexes. Skin of rhinarium, wings, uropatagium, lips, face, and tragus are fuscous (pale yellow).\",\n" +
                "  \"Measurements\": \"Head-body 79-4-86-3 mm, tail 31-5-36-9 mm, ear 22-4-27-1 mm, hindfoot 9-8-10-3 mm, forearm 73-76 mm; weight. 20-29 g.\",\n" +
                "  \"Habitat\": \"Wide variety of habitats and bioregions of interior Queensland.\",\n" +
                "  \"Food and Feeding\": \"Troughton’s Sheath-tailed Bats forage for insects well above tree canopies and high over open habitats. Large, high-flying grasshoppers are preferred food items and often taken back to cave roosts to eat.\",\n" +
                "  \"Breeding\": \"No information.\",\n" +
                "  \"Activity patterns\": \"Troughton’s Sheath-tailed Bat roosts in caves, mines and tunnels, rock crevices, and rocky escarpments. Echolocation call is less than 25 kHz and distinguishes it from the Common Sheath-tailed Bat (. georgianus ) where they co-occur. Movements, Home range and Social organization. Large colonies of Troughton’s Sheath-tailed Bat can be found in landscapes with abundant rocky outcrops, especially in tower karst. Colony size might be limited by roosting structures, especially in more arid areas where there are few caves deep enough to support large colonies.\",\n" +
                "  \"Movement/Home range/Social organization\": \"\",\n" +
                "  \"Status and Conservation\": \"Classified as Least Concern on TheIUCNRed List. Troughton’s Sheath-tailed Bat has a large distribution and presumably large and stable overall population, uses a wide variety of habitats, occurs in protected areas, and does not face significant threats. It was originally recorded only from a small area in the Mount Isa Inland bioregion of Queensland, but recent studies based on isozymes and echolocation calls extend distribution further east throughout much of interior and near coastal region of central Queensland, formerly attributed to the Common Sheathtailed Bat. Recent reports of absence of Troughton’s Sheath-tailed Bat in western parts of its distribution require additional verification, possibly leading to re-evaluation of its conservation status after taxonomic issues are clarified.\",\n" +
                "  \"Bibliography\": \"Chimimba & Kitchener (1991), Hall (2008b), McKean & Price (1967), Reardon & Thomson (2002), Tate (1952),Thomson eta /. (2001), Woinarski eta/. (2014).\",\n" +
                "  \"DOI\": \"http://doi.org/10.5281/zenodo.3740269\",\n" +
                "  \"page number\": 355,\n" +
                "  \"media\": \"Fig 1, Fig 2\"\n" +
                "}\n");
    }


}