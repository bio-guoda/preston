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
        assertThat(treatment.get("docName").asText(), Is.is("hbmw-9.emballorunidae.pdf.imd"));
        assertThat(treatment.get("docOrigin").asText(), Is.is("Handbook of the Mammals of the World, Vol. 9, Lyny Edicions"));
        assertThat(treatment.get("docISBN").asText(), Is.is("978-84-16728-19-0"));
        assertNull(treatment.get("box"));
        assertThat(treatment.get("genus").asText(), Is.is("Taphozous"));
        assertThat(treatment.get("species").asText(), Is.is("troughtoni"));


        assertThat(treatment.get("distribution").asText(), Is.is("NE Australia endemic, in WC, C & E Queensland."));
        assertThat(treatment.get("distributionImageURL").asText(), Is.is("https://zenodo.org/record/3747930/files/figure.png"));


        assertThat(treatment.get("eats").asText(), Is.is("Troughton’s Sheath-tailed Bats forage for insects well above tree canopies and high over open habitats. Large, high-flying grasshoppers are preferred food items and often taken back to cave roosts to eat."));


        assertThat(treatment.get("activity").asText(),
                Is.is("Troughton’s Sheath-tailed Bat roosts in caves, mines and tunnels, rock crevices, and rocky escarpments. Echolocation call is less than 25 kHz and distinguishes it from the Common Sheath-tailed Bat (. georgianus ) where they co-occur . Movements, Home range and Social organization. Large colonies of Troughton’s Sheath-tailed Bat can be found in landscapes with abundant rocky outcrops, especially in tower karst. Colony size might be limited by roosting structures, especially in more arid areas where there are few caves deep enough to support large colonies."));


        assertThat(treatment.get("bibliography").asText(),
                Is.is("Chimimba & Kitchener (1991), Hall (2008b), McKean & Price (1967), Reardon & Thomson (2002), Tate (1952),Thomson eta /. (2001), Woinarski eta/. (2014)."));


        assertThat(treatment.get("habitat").asText(),
                Is.is("Wide variety of habitats and bioregions of interior Queensland."));


    }


}