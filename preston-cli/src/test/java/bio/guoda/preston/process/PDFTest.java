package bio.guoda.preston.process;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.Preston;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.Version;
import bio.guoda.preston.stream.ContentStreamException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentCatalog;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.common.PDPageLabelRange;
import org.apache.pdfbox.pdmodel.common.PDPageLabels;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class PDFTest {

    @Test
    public void extractPage() throws IOException {
        String pageLabel = "19";
        int expectedPageCount = 232;

        // Also see BatLit - Krumbach, T., Koopman, K. F., & Krumbach, T. (1994). Chiroptera, Teilbd. 60: systematics. de Gruyter. https://doi.org/10.5281/zenodo.13422270 https://linker.bio/hash://md5/70c3e0fba7379e09e95a38569fe29da7

        String resourceName = "koopman1994.pdf";
        IRI baseContentId = Hasher
                .calcHashIRI(getClass().getResourceAsStream(resourceName), NullOutputStream.INSTANCE, HashType.md5);


        IRI contentId = toIRI("pdf:" + baseContentId.getIRIString() + "!/p" + pageLabel);

        PDDocument pdDocument = Loader.loadPDF(new File(getClass().getResource(resourceName).getFile()));

        assertThat(pdDocument.getNumberOfPages(), is(expectedPageCount));

        PDDocumentCatalog documentCatalog = pdDocument.getDocumentCatalog();

        PDPageLabels pageLabels = documentCatalog.getPageLabels();
        String[] labelsByPageIndices = pageLabels.getLabelsByPageIndices();
        assertThat(labelsByPageIndices[0], is("I"));
        assertThat(labelsByPageIndices[1], is("II"));
        assertThat(labelsByPageIndices[2], is("III"));
        assertThat(labelsByPageIndices[3], is("IV"));
        assertThat(labelsByPageIndices[4], is("V"));
        assertThat(labelsByPageIndices[5], is("VI"));
        assertThat(labelsByPageIndices[6], is("VII"));
        assertThat(labelsByPageIndices[7], is("VIII"));
        assertThat(labelsByPageIndices[8], is("1"));
        assertThat(labelsByPageIndices[26], is("19"));

        Integer integer = documentCatalog.getPageLabels().getPageIndicesByLabels().get(pageLabel);

        assertThat(integer, is(26));

        PDPage page = pdDocument.getPage(26);

        PDPageLabelRange pageLabelRange = documentCatalog.getPageLabels().getPageLabelRange(26);

        ByteArrayOutputStream output1 = toOutputStream(org.apache.commons.lang3.tuple.Triple.of(page, 0, pageLabelRange), contentId, new ByteArrayOutputStream());
        PDDocument actualDoc = Loader.loadPDF(output1.toByteArray());
        String customMetadataValue = actualDoc.getDocumentInformation().getCustomMetadataValue(RefNodeConstants.WAS_DERIVED_FROM.getIRIString());

        InputStream resourceAsStream = getClass().getResourceAsStream("koopman1994-page19.pdf");
        ByteArrayOutputStream output2 = new ByteArrayOutputStream();
        IOUtils.copy(resourceAsStream, output2);

        assertThat(customMetadataValue, is(contentId.getIRIString()));

        assertThat(output1.toString(), is(output2.toString()));


    }

    @Test
    public void extractAnotherPage() throws IOException, ContentStreamException {
        String requestedPageLabel = "3";
        int expectedPageCount = 13;

        // Also see BatLit - Krumbach, T., Koopman, K. F., & Krumbach, T. (1994). Chiroptera, Teilbd. 60: systematics. de Gruyter. https://doi.org/10.5281/zenodo.13422270 https://linker.bio/hash://md5/70c3e0fba7379e09e95a38569fe29da7

        String resourceName = "elliott2023.pdf";
        IRI baseContentId = Hasher
                .calcHashIRI(getClass().getResourceAsStream(resourceName), NullOutputStream.INSTANCE, HashType.md5);


        IRI contentId = toIRI("pdf:" + baseContentId.getIRIString() + "!/p" + requestedPageLabel);

        PDDocument pdDocument = Loader.loadPDF(new File(getClass().getResource(resourceName).getFile()));

        assertThat(pdDocument.getNumberOfPages(), is(expectedPageCount));

        Pair<Integer, PDPageLabelRange> indexAndLabel = PDFUtil.getPageIndexAndPageLabelRange(requestedPageLabel, contentId, pdDocument);

        PDPage page = pdDocument.getPage(indexAndLabel.getKey());

        ByteArrayOutputStream output1 = toOutputStream(org.apache.commons.lang3.tuple.Triple.of(page, indexAndLabel.getKey(), indexAndLabel.getValue()), contentId, new ByteArrayOutputStream());
        PDDocument actualDoc = Loader.loadPDF(output1.toByteArray());

        PDPageLabelRange firstPageRange = actualDoc.getDocumentCatalog().getPageLabels().getPageLabelRange(0);

        assertThat(firstPageRange.getStart(), is(3));
        assertThat(firstPageRange.getStyle(), is(PDPageLabelRange.STYLE_DECIMAL));
        assertThat(firstPageRange.getPrefix(), is(""));
        String customMetadataValue = actualDoc.getDocumentInformation().getCustomMetadataValue(RefNodeConstants.WAS_DERIVED_FROM.getIRIString());

        InputStream resourceAsStream = getClass().getResourceAsStream("elliott-page3.pdf");
        ByteArrayOutputStream output2 = new ByteArrayOutputStream();
        IOUtils.copy(resourceAsStream, output2);


        assertThat(customMetadataValue, is(contentId.getIRIString()));

        assertThat(output1.toString(), is(output2.toString()));


    }

    private static ByteArrayOutputStream toOutputStream(org.apache.commons.lang3.tuple.Triple<PDPage, Integer, PDPageLabelRange> pageIndexAndLabel, IRI contentId, ByteArrayOutputStream output) throws IOException {
        return toOutputStream(pageIndexAndLabel.getLeft(), contentId, pageIndexAndLabel.getRight(), pageIndexAndLabel.getMiddle(), output);
    }

    private static ByteArrayOutputStream toOutputStream(PDPage page, IRI contentId, Pair<Integer, PDPageLabelRange> indexAndLabel, ByteArrayOutputStream output) throws IOException {
        return toOutputStream(page, contentId, indexAndLabel.getValue(), indexAndLabel.getKey(), output);
    }

    private static ByteArrayOutputStream toOutputStream(PDPage page, IRI contentId, PDPageLabelRange range, int startPage, ByteArrayOutputStream output) throws IOException {
        PDDocument selectedPages = new PDDocument();


        selectedPages.getDocumentInformation().setCustomMetadataValue(RefNodeConstants.WAS_DERIVED_FROM.getIRIString(), contentId.getIRIString());
        selectedPages.getDocumentInformation().setTitle(contentId.toString());
        selectedPages.getDocumentInformation().setProducer(Preston.class.getSimpleName() + " v" + Version.getVersionString() + "@" + Version.getGitCommitHash());
        selectedPages.addPage(page);
        selectedPages.setDocumentId(1L);
        PDPageLabels pdPageLabels = new PDPageLabels(selectedPages);
        pdPageLabels.setLabelItem(startPage, range);
        selectedPages.getDocumentCatalog().setPageLabels(pdPageLabels);

        selectedPages.save(output);
        return output;
    }

}
