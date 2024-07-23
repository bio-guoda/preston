package bio.guoda.preston.excel;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.NotOLE2FileException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class XLSHandler {


    public static final String APPLICATION_VND_MS_EXCEL = "application/vnd.ms-excel";

    public static void asJsonStream(OutputStream out, IRI resourceIRI, KeyValueStoreReadOnly contentStore, Integer skipLines, Boolean headerless) throws IOException {
        try (HSSFWorkbook workbook = new HSSFWorkbook(contentStore.get(resourceIRI))) {
            asJsonStream(out, resourceIRI, workbook, APPLICATION_VND_MS_EXCEL, skipLines, headerless);
        } catch (RuntimeException | NotOLE2FileException ex) {
            // ignore runtime exception to implement opportunistic handling
        }
    }

    public static void asJsonStream(OutputStream out, IRI resourceIRI, Workbook workbook, String mimeType, Integer skipLines, Boolean headerless) throws IOException {
        final DataFormatter formatter = new DataFormatter();
        for (Sheet sheet : workbook) {
            HashMap<Integer,String> header = new HashMap<>();
            int rowNumber = 0;
            for (Row r : sheet) {
                if (rowNumber == skipLines || headerless) {
                    for (Cell c : r) {
                        int column = c.getColumnIndex();
                        if (headerless) {
                            header.put(column, Integer.toString(column + 1));
                        } else {
                            String value = getCellValue(formatter, c);
                            header.put(column, StringUtils.isBlank(value) ? ("column" + column) : value);
                        }
                    }
                }

                if (isDataRowWithoutHeader(skipLines, headerless, rowNumber)
                        || isDataRowWithHeader(skipLines, headerless, rowNumber)) {
                    ObjectMapper obj = new ObjectMapper();
                    ObjectNode objectNode = obj.createObjectNode();
                    setMetaData(resourceIRI, mimeType, sheet, r, objectNode);

                    for (Cell c : r) {
                        int columnNumber = c.getColumnIndex();
                        String fieldName = columnNumber < header.size() ? header.get(columnNumber) : ("column" + columnNumber);
                        objectNode.put(fieldName, getCellValue(formatter, c));
                    }

                    writeObjectNode(out, objectNode);
                }
                rowNumber++;
            }
        }
    }

    private static void writeObjectNode(OutputStream out, ObjectNode objectNode) throws IOException {
        if (objectNode.size() > 0) {
            IOUtils.copy(IOUtils.toInputStream(objectNode.toString(), StandardCharsets.UTF_8), out);
            IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), out);
        }
    }

    static boolean isDataRowWithHeader(Integer skipLines, Boolean headerless, int rowNumber) {
        return !headerless && rowNumber > skipLines;
    }

    static boolean isDataRowWithoutHeader(Integer skipLines, Boolean headerless, int rowNumber) {
        return headerless && rowNumber >= skipLines;
    }

    private static String getCellValue(DataFormatter formatter, Cell c) {
        String value = "";

        if (c != null) {
            value = formatter.formatCellValue(c);
        }
        return value;
    }

    private static void setMetaData(IRI resourceIRI, String mimeType, Sheet sheet, Row r, ObjectNode objectNode) throws IOException {
        String prefix = APPLICATION_VND_MS_EXCEL.equals(mimeType)
                ? "line:xls:"
                : "line:xlsx:";

        String sheetName;
        try {
            sheetName = StringUtils.substring(new URI("https", "example.org", "/" + sheet.getSheetName(), null).getRawPath(), 1);
        } catch (URISyntaxException e) {
            throw new IOException("failed to create resource location", e);
        }

        int rowNumberWithOffsetOne = r.getRowNum() + 1;
        URI resourceAddress = URI.create(prefix + resourceIRI.getIRIString() + "!/" + sheetName + "!/L" + rowNumberWithOffsetOne);
        TextNode resourceAddressIRI = TextNode.valueOf(RefNodeFactory.toIRI(resourceAddress).getIRIString());
        objectNode.set("http://www.w3.org/ns/prov#wasDerivedFrom", resourceAddressIRI);
        objectNode.set("http://purl.org/dc/elements/1.1/format", TextNode.valueOf(mimeType));
    }
}