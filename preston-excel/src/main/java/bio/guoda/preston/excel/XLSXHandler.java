package bio.guoda.preston.excel;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.monitorjbl.xlsx.StreamingReader;
import com.monitorjbl.xlsx.exceptions.ReadException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;
import org.apache.poi.EmptyFileException;
import org.apache.poi.ooxml.POIXMLException;
import org.apache.poi.openxml4j.exceptions.InvalidOperationException;
import org.apache.poi.openxml4j.exceptions.NotOfficeXmlFileException;
import org.apache.poi.openxml4j.opc.PackagePartName;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFPictureData;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

public class XLSXHandler {


    public static void rowsAsJsonStream(OutputStream out, IRI resourceIRI, KeyValueStoreReadOnly contentStore, Integer skipLines, Boolean headerless) throws IOException {
        try (Workbook workbook = StreamingReader
                .builder()
                .open(contentStore.get(resourceIRI))) {
            XLSHandler.asJsonStream(
                    out,
                    resourceIRI,
                    workbook,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    skipLines,
                    headerless);
        } catch (NotOfficeXmlFileException | InvalidOperationException | ReadException | POIXMLException | EmptyFileException ex) {
            // ignore runtime exception to implement opportunistic handling
        }
    }

    public static void emitPictureStatementsForXLSX(HashType hashType, IRI archiveContentId, StatementsListener listener, Dereferencer<InputStream> contentStore) throws IOException {

        try {
            emitPictureStatements(hashType, archiveContentId, listener, contentStore);
        } catch (NotOfficeXmlFileException | InvalidOperationException | ReadException | POIXMLException | EmptyFileException ex) {
            // ignore runtime exception to implement opportunistic handling
        }
    }

    private static void emitPictureStatements(HashType hashType, IRI archiveContentId, StatementsListener listener, Dereferencer<InputStream> contentStore) throws IOException {
        XSSFWorkbook workbook = new XSSFWorkbook(contentStore.get(archiveContentId));

        List<XSSFPictureData> pictures = workbook.getAllPictures();
        for (XSSFPictureData picture : pictures) {
            emitPictureStatements(
                    picture,
                    archiveContentId,
                    listener,
                    hashType
            );

        }
    }

    private static void emitPictureStatements(XSSFPictureData picture, IRI expectedArchiveContentId, StatementsListener statementsListener, HashType hashType) throws IOException {
        PackagePartName partName = picture.getPackagePart().getPartName();
        String relativeImageIRI = "zip:" + expectedArchiveContentId.getIRIString() + "!" + partName.getName();
        IRI iri = Hasher.calcHashIRI(new ByteArrayInputStream(picture.getData()), NullOutputStream.INSTANCE, hashType);

        statementsListener.on(Arrays.asList(
                RefNodeFactory.toStatement(RefNodeFactory.toIRI(relativeImageIRI), RefNodeConstants.HAS_FORMAT, RefNodeFactory.toLiteral(picture.getMimeType())),
                RefNodeFactory.toStatement(RefNodeFactory.toIRI(relativeImageIRI), RefNodeConstants.HAS_VERSION, iri)
        ));
    }

    public static void picturesAsJsonStream(HashType hashType, IRI archiveContentId, OutputStream os, Dereferencer<InputStream> contentStore) throws IOException {

        try {
            XSSFWorkbook workbook = new XSSFWorkbook(contentStore.get(archiveContentId));

            List<XSSFPictureData> pictures = workbook.getAllPictures();
            for (XSSFPictureData picture : pictures) {
                pictureAsJsonStream(
                        picture,
                        archiveContentId,
                        os,
                        hashType
                );

            }
        } catch (NotOfficeXmlFileException | InvalidOperationException | ReadException | POIXMLException | EmptyFileException ex) {
            // ignore runtime exception to implement opportunistic handling
        }
    }

    private static void pictureAsJsonStream(XSSFPictureData picture, IRI expectedArchiveContentId, OutputStream os, HashType hashType) throws IOException {
        PackagePartName partName = picture.getPackagePart().getPartName();
        String relativeImageIRI = "zip:" + expectedArchiveContentId.getIRIString() + "!" + partName.getName();
        IRI iri = Hasher.calcHashIRI(new ByteArrayInputStream(picture.getData()), NullOutputStream.INSTANCE, hashType);

        ObjectNode imageNode = new ObjectMapper()
                .createObjectNode()
                .put(XLSHandler.WAS_DERIVED_FROM, relativeImageIRI)
                .put(XLSHandler.HAS_FORMAT, picture.getMimeType())
                .put(RefNodeConstants.HAS_VERSION.getIRIString(), iri.getIRIString());

        XLSHandler.writeObjectNode(os, imageNode);
    }
}