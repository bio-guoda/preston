package bio.guoda.preston.cmd;

import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import bio.guoda.preston.util.UUIDUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MBDPageStreamHandler implements ContentStreamHandler {

    public static final String NAME_PREFIX = "https://mbd-db.osu.edu/hol/taxon_name/";
    private final Dereferencer<InputStream> dereferencer;
    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;
    public static final Pattern UUID_MATCHER = Pattern.compile("(.*)(?<uuid>" + UUIDUtil.UUID_PATTERN_PART + ")(.*)");

    public MBDPageStreamHandler(ContentStreamHandler contentStreamHandler,
                                Dereferencer<InputStream> inputStreamDereferencer,
                                OutputStream os) {
        this.contentStreamHandler = contentStreamHandler;
        this.dereferencer = inputStreamDereferencer;
        this.outputStream = os;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        AtomicBoolean foundAtLeastOne = new AtomicBoolean(false);
        String iriString = version.getIRIString();
        try {
            Charset charset = new UniversalEncodingDetector().detect(is, new Metadata());
            if (charset != null) {

                Document parse = Jsoup.parse(is, charset.name(), "https://example.org");

                ObjectNode objectNode = new ObjectMapper().createObjectNode();
                Elements select = parse.select("div#specimen-images div");
                for (Element element : select) {
                    if (element.hasAttr("additional_parameters")) {
                        String params = element.attr("additional_parameters");
                        JsonNode jsonNode = new ObjectMapper().readTree(params);
                        JsonNode collecting_unit_id = jsonNode.get("collecting_unit_id");
                        if (collecting_unit_id != null) {
                            String collectingUnitId = StringUtils.trim(collecting_unit_id.asText());
                            String collectingUnitIdString = "https://mbd-db.osu.edu/hol/collecting_units/" + collectingUnitId;
                            setValue(objectNode, "sourceOccurrenceId", collectingUnitIdString);
                            setValue(objectNode, "collecting_unit_id", collectingUnitId);

                        }
                    }
                }

                Elements determinations = parse.select("div#determinations tbody tr");
                for (Element determination : determinations) {
                    Elements determinationValues = determination.select("td");
                    if (determinationValues.size() > 3) {
                        Element element = determinationValues.get(0);
                        Elements a = element.select("a");
                        if (a.size() == 1) {
                            setValue(objectNode, "sourceTaxonName", StringUtils.trim(a.text()));
                            if (a.hasAttr("href")) {
                                Matcher href = UUID_MATCHER.matcher(a.attr("href"));
                                if (href.matches()) {
                                    setValue(objectNode, "sourceTaxonId", NAME_PREFIX + StringUtils.trim(href.group("uuid")));
                                }
                            }
                        }
                        List<org.jsoup.nodes.TextNode> textNodes = element.textNodes();
                        if (textNodes.size() > 1) {
                            setValue(objectNode, "sourceTaxonAuthorship", StringUtils.trim(textNodes.get(1).text()));
                        }
                    }
                }

                Elements title = parse.select("head title");
                if (title.size() == 1) {
                    String text = title.text();
                    int colon = StringUtils.indexOf(text, ":");
                    if (StringUtils.length(text) > colon) {
                        setValue(objectNode, "sourceCatalogNumber", StringUtils.trim(StringUtils.substring(text, colon + 1)));
                    }
                }

                Elements elements = parse.select("div#unvouchered-association tbody tr");
                for (Element row : elements) {
                    Elements td = row.select("td");
                    if (td.size() > 1) {
                        setValue(objectNode, "http://www.w3.org/ns/prov#wasDerivedFrom", iriString);
                        setValue(objectNode, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "text/html");
                        Elements a = td.select("a");
                        if (a.size() == 1) {
                            String taxonId = StringUtils.trim(a.get(0).attr("href"));
                            Matcher uuidMatcher = UUID_MATCHER.matcher(taxonId);
                            if (uuidMatcher.matches()) {
                                setValue(objectNode, "targetTaxonId", NAME_PREFIX + uuidMatcher.group("uuid"));
                            }
                        }
                        String specimenAssociateName = td.get(0).text();
                        String[] split = StringUtils.split(specimenAssociateName, "|");
                        if (split.length > 0) {
                            setValue(objectNode, "targetTaxonName", StringUtils.trim(split[0]));
                        }
                        if (split.length > 2) {
                            setValue(objectNode, "targetTaxonAuthorship", StringUtils.trim(split[1]));
                            setValue(objectNode, "targetTaxonStatus", StringUtils.trim(split[2]));
                        }
                        String associationType = td.get(1).text();
                        if (StringUtils.isNotBlank(associationType)) {
                            setValue(objectNode, "interactionTypeName", StringUtils.trim(associationType));
                        }

                        IOUtils.copy(IOUtils.toInputStream(objectNode.toString(), StandardCharsets.UTF_8), outputStream);
                        IOUtils.write("\n", outputStream, StandardCharsets.UTF_8);

                    }

                }

            }

        } catch (IOException e) {
            throw new ContentStreamException("no charset detected");
        }

        return foundAtLeastOne.get();
    }

    private void setValue(ObjectNode objectNode, String key, String value) {
        objectNode.set(key, TextNode.valueOf(value));
    }

    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


}
