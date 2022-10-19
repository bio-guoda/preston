package bio.guoda.preston.excel;

import bio.guoda.preston.store.KeyValueStoreReadOnly;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.NotOLE2FileException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class XLSHandler {


    public static void asJsonStream(OutputStream out, IRI resourceIRI, KeyValueStoreReadOnly contentStore) throws IOException {
        try (HSSFWorkbook workbook = new HSSFWorkbook(contentStore.get(resourceIRI))) {
            asJsonStream(out, resourceIRI, workbook, "application/vnd.ms-excel");
        } catch (RuntimeException | NotOLE2FileException ex) {
            // ignore runtime exception to implement opportunistic handling
        }
    }

    public static void asJsonStream(OutputStream out, IRI resourceIRI, Workbook workbook, String mimeType) throws IOException {
        for (Sheet sheet : workbook) {
            List<String> header = new ArrayList<>();
            boolean isFirstRow = true;
            for (Row r : sheet) {
                if (isFirstRow) {
                    for (Cell cell : r) {
                        header.add(cell.getStringCellValue());
                    }
                } else {
                    ObjectMapper obj = new ObjectMapper();
                    ObjectNode objectNode = obj.createObjectNode();

                    Iterator<Cell> cellIterator = r.cellIterator();
                    int i = 0;
                    while (cellIterator.hasNext()) {
                        if (i > header.size() - 1) {
                            throw new IOException("found row with more cells [" + i + "] than were defined in the first header row [" + header.size() + "] of [" + resourceIRI.getIRIString() + "]");
                        }
                        if (i == 0) {
                            objectNode.set("http://www.w3.org/ns/prov#wasDerivedFrom", TextNode.valueOf(resourceIRI.getIRIString()));
                            objectNode.set("http://purl.org/dc/elements/1.1/format", TextNode.valueOf(mimeType));
                        }
                        Cell c = cellIterator.next();
                        String fieldName = header.get(i);
                        String value;
                        switch (c.getCellType()) {
                            case BOOLEAN:
                                value = c.getBooleanCellValue() ? "true" : "false";
                                break;
                            case STRING:
                                value = c.getStringCellValue();
                                break;
                            default:
                                value = "";
                        }
                        objectNode.put(fieldName, value);
                        i++;
                    }

                    if (objectNode.size() > 0) {
                        IOUtils.copy(IOUtils.toInputStream(objectNode.toString(), StandardCharsets.UTF_8), out);
                        IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), out);
                    }
                }
                isFirstRow = false;
            }
        }
    }
}