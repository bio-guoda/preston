package bio.guoda.preston.cmd;

import com.fasterxml.jackson.databind.JsonNode;
import org.hamcrest.core.Is;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStream;

import static junit.framework.TestCase.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;

public class PlaziUtilTest {


    @Test
    public void processXML() throws IOException, XPathExpressionException, SAXException, ParserConfigurationException {
        InputStream is = getClass().getResourceAsStream("03D587F2FFC94C03F8F13AECFBD8F765.xml");


        JsonNode treatment = PlaziUtil.parseTreatment(is);

        assertThat(treatment.get("docId").asText(), Is.is("03D587F2FFC94C03F8F13AECFBD8F765"));
        assertThat(treatment.get("docMasterId").asText(), Is.is("hash://md5/ffecff8affcf4c04ffa53577fff8ffe9"));
        assertThat(treatment.get("docName").asText(), Is.is("hbmw-9.emballorunidae.pdf.imd"));
        assertThat(treatment.get("docOrigin").asText(), Is.is("Handbook of the Mammals of the World, Vol. 9, Lyny Edicions"));
        assertThat(treatment.get("docISBN").asText(), Is.is("978-84-16728-19-0"));
        assertThat(treatment.get("docPageNumber").asText(), Is.is("355"));
        assertNull(treatment.get("box"));
        assertThat(treatment.get("interpretedGenus").asText(), Is.is("Taphozous"));
        assertThat(treatment.get("interpretedSpecies").asText(), Is.is("troughtoni"));
        assertThat(treatment.get("name").asText(), Is.is("Taphozous troughtoni"));


        assertThat(treatment.get("subspeciesAndDistribution").asText(), Is.is("NE Australia endemic, in WC, C & E Queensland."));
        assertThat(treatment.get("distributionImageURL").asText(), Is.is("https://zenodo.org/record/3747930/files/figure.png"));


        assertThat(treatment.get("foodAndFeeding").asText(), Is.is("Troughton’s Sheath-tailed Bats forage for insects well above tree canopies and high over open habitats. Large, high-flying grasshoppers are preferred food items and often taken back to cave roosts to eat."));


        assertThat(treatment.get("activityPatterns").asText(),
                Is.is("Troughton’s Sheath-tailed Bat roosts in caves, mines and tunnels, rock crevices, and rocky escarpments. Echolocation call is less than 25 kHz and distinguishes it from the Common Sheath-tailed Bat (. georgianus ) where they co-occur . Movements, Home range and Social organization. Large colonies of Troughton’s Sheath-tailed Bat can be found in landscapes with abundant rocky outcrops, especially in tower karst. Colony size might be limited by roosting structures, especially in more arid areas where there are few caves deep enough to support large colonies."));

        assertThat(treatment.get("bibliography").asText(),
                Is.is("Chimimba & Kitchener (1991)" +
                        " | Hall (2008b)" +
                        " | McKean & Price (1967)" +
                        " | Reardon & Thomson (2002)" +
                        " | Tate (1952)" +
                        " | Thomson et al. (2001)" +
                        " | Woinarski et al. (2014)"));

        assertThat(treatment.get("habitat").asText(),
                Is.is("Wide variety of habitats and bioregions of interior Queensland."));

        assertThat(treatment.get("descriptiveNotes").asText(), Is.is("Head-body 79-4-86-3 mm, tail 31-5-36-9 mm, ear 22-4-27-1 mm, hindfoot 9-8-10-3 mm, forearm 73-76 mm; weight. 20-29 g. Dorsum of Troughton’s Sheath-tailed Bat is predominately olive\u00AD brown, with pale mouse-gray guard hairs. Venter surface hairs are olive-brown from chin to shoulders and posteriorly dark yellow-brown, with guard hairs of pale mouse-gray. Uropatagium close to abdomen is heavily furred. Throat pouches are absent, and radio-metacarpal sacs are present in both sexes. Skin of rhinarium, wings, uropatagium, lips, face, and tragus are fuscous (pale yellow)."));

        assertThat(treatment.get("movementsHomeRangeAndSocialOrganization").asText(),
                Is.is("Large colonies of Troughton’s Sheath-tailed Bat can be found in landscapes with abundant rocky outcrops, especially in tower karst. Colony size might be limited by roosting structures, especially in more arid areas where there are few caves deep enough to support large colonies."));

        assertThat(treatment.get("statusAndConservation").asText(),
                Is.is("Classified as Least Concern on TheIUCNRed List. Troughton’s Sheath-tailed Bat has a large distribution and presumably large and stable overall population, uses a wide variety of habitats, occurs in protected areas, and does not face significant threats. It was originally recorded only from a small area in the Mount Isa Inland bioregion of Queensland, but recent studies based on isozymes and echolocation calls extend distribution further east throughout much of interior and near coastal region of central Queensland, formerly attributed to the Common Sheathtailed Bat. Recent reports of absence of Troughton’s Sheath-tailed Bat in western parts of its distribution require additional verification, possibly leading to re-evaluation of its conservation status after taxonomic issues are clarified."));

        assertThat(treatment.get("commonNames").asText(),
                Is.is("Troughton’s Sheath-tailed Bat @en" +
                        " | Taphien de Troughton @fr" +
                        " | Troughton-Grabfledermaus @de" +
                        " | Tafozo de Troughton @es" +
                        " | Troughton's Tomb Bat @en"));

        assertThat(treatment.get("taxonomy").asText(),
                Is.is("Taphozous troughtoni Tate, 1952 , “ Rifle Creek, Mt. Isa, northwest Queensland ,” Australia .Taphozous troughtoni is in the subgenus Taphozous . It was considered ajunior synonym of T georgianus , but. T. Chimimba and D. J. Kitchener in 1991 raised it to a distinct species. Monotypic."));

        assertThat(treatment.get("breeding").asText(),
                Is.is("No information."));

    }


}