package bio.guoda.preston.excel;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.collections4.CollectionUtils;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class XLSHandler {


    public static final String APPLICATION_VND_MS_EXCEL = "application/vnd.ms-excel";
    public static final String WAS_DERIVED_FROM = "http://www.w3.org/ns/prov#wasDerivedFrom";
    public static final String HAS_FORMAT = "http://purl.org/dc/elements/1.1/format";

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
            HashMap<Integer, String> header = new HashMap<>();
            int rowNumber = 0;
            for (Row r : sheet) {
                if (rowNumber == skipLines || headerless) {
                    populateHeader(headerless, formatter, header, r);
                }

                if (isDataRowWithoutHeader(skipLines, headerless, rowNumber)
                        || isDataRowWithHeader(skipLines, headerless, rowNumber)) {
                    handleDataRow(out, resourceIRI, mimeType, formatter, sheet, header, r);
                }
                rowNumber++;
            }
        }
    }

    private static void handleDataRow(OutputStream out, IRI resourceIRI, String mimeType, DataFormatter formatter, Sheet sheet, HashMap<Integer, String> header, Row row) throws IOException {
        ObjectMapper obj = new ObjectMapper();
        ObjectNode objectNode = obj.createObjectNode();
        setMetaData(resourceIRI, mimeType, sheet, row, objectNode);

        short maxColIx = row.getLastCellNum();
        for (short colIx = 0; colIx < maxColIx; colIx++) {
            int cellIndex = Short.toUnsignedInt(colIx);
            String fieldName = getFieldName(header, cellIndex);
            Cell cell = row.getCell(cellIndex);
            if (null == cell) {
                objectNode.set(fieldName, new ObjectMapper().nullNode());
            } else {
                objectNode.put(fieldName, getCellValue(formatter, cell));
            }
        }

        writeObjectNode(out, objectNode);
    }

    private static void populateHeader(Boolean headerless, DataFormatter formatter, HashMap<Integer, String> header, Row r) {
        short maxColIx = r.getLastCellNum();
        for (short colIx = 0; colIx < maxColIx; colIx++) {
            int cellIndex = Short.toUnsignedInt(colIx);
            String columnName;
            if (headerless) {
                columnName = Integer.toString(cellIndex + 1);
            } else {
                Cell cell = r.getCell(colIx);
                if (cell == null) {
                    columnName = ("column" + colIx);
                } else {
                    String value = getCellValue(formatter, cell);
                    columnName = StringUtils.isBlank(value) ? ("column" + cellIndex + 1) : value;
                }
            }
            header.put(cellIndex, columnName);
        }
    }

    private static String getFieldName(HashMap<Integer, String> header, int columnNumber) {
        return columnNumber < header.size() ? header.get(columnNumber) : ("column" + columnNumber);
    }

    public static void writeObjectNode(OutputStream out, ObjectNode objectNode) throws IOException {
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
            throw new IOException("failed to createEmptyDeposit resource location", e);
        }

        int rowNumberWithOffsetOne = r.getRowNum() + 1;
        URI resourceAddress = URI.create(prefix + resourceIRI.getIRIString() + "!/" + sheetName + "!/L" + rowNumberWithOffsetOne);
        TextNode resourceAddressIRI = TextNode.valueOf(RefNodeFactory.toIRI(resourceAddress).getIRIString());
        objectNode.set(WAS_DERIVED_FROM, resourceAddressIRI);
        objectNode.set(HAS_FORMAT, TextNode.valueOf(mimeType));
    }
}