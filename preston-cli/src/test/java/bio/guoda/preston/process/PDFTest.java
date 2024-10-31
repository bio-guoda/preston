package bio.guoda.preston.process;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.Preston;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.Version;
import bio.guoda.preston.stream.ContentStreamException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentCatalog;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.common.PDPageLabelRange;
import org.apache.pdfbox.pdmodel.common.PDPageLabels;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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

        ByteArrayOutputStream output1 = new ByteArrayOutputStream();

        saveAsPDF(0, pageLabelRange, page, contentId, output1);

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

        SelectedPage selectedPage = selectPage(requestedPageLabel, pdDocument);

        ByteArrayOutputStream output1 = new ByteArrayOutputStream();

        saveAsPDF(selectedPage, contentId, output1);

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

    private static SelectedPage selectPage(String requestedPageLabel, PDDocument doc) throws IOException, ContentStreamException {
        PDDocumentCatalog documentCatalog = doc.getDocumentCatalog();

        PDPageLabels pageLabels = documentCatalog.getPageLabels();
        int pageIndex;
        PDPageLabelRange pageLabelRange;
        if (useIndexAsLabel(requestedPageLabel, pageLabels)) {
            int requestedPageNumber = Integer.parseInt(requestedPageLabel);
            pageIndex = requestedPageNumber - 1;
            pageLabelRange = new PDPageLabelRange();
            pageLabelRange.setPrefix("");
            pageLabelRange.setStart(requestedPageNumber);
            pageLabelRange.setStyle(PDPageLabelRange.STYLE_DECIMAL);
        } else if (useLabel(requestedPageLabel, pageLabels)) {
            pageIndex = pageLabels.getPageIndicesByLabels().get(requestedPageLabel);
            pageLabelRange = pageLabels.getPageLabelRange(pageIndex);
        } else {
            throw new ContentStreamException("cannot find page [" + requestedPageLabel + "]");
        }

        PDPage page = doc.getPage(pageIndex);

        return new SelectedPage(page, 0, pageLabelRange);
    }

    private static boolean useLabel(String pageLabel, PDPageLabels pageLabels) {
        return pageLabels != null && pageLabels.getPageIndicesByLabels().containsKey(pageLabel);
    }

    private static boolean useIndexAsLabel(String pageLabel, PDPageLabels pageLabels) {
        return pageLabels == null && NumberUtils.isDigits(pageLabel);
    }

    public static void saveAsPDF(SelectedPage selectedPage,
                                 IRI contentId,
                                 OutputStream output) throws IOException {
        saveAsPDF(selectedPage.getIndex(),
                selectedPage.getPageLabelRange(),
                selectedPage.getPage(),
                contentId,
                output);
    }

    public static void saveAsPDF(int pageIndex,
                                 PDPageLabelRange pageLabel,
                                 PDPage page,
                                 IRI contentId,
                                 OutputStream output) throws IOException {
        PDDocument subset = new PDDocument();


        subset.getDocumentInformation().setCustomMetadataValue(RefNodeConstants.WAS_DERIVED_FROM.getIRIString(), contentId.getIRIString());
        subset.getDocumentInformation().setTitle(contentId.toString());
        subset.getDocumentInformation().setProducer(Preston.class.getSimpleName() + " v" + Version.getVersionString() + "@" + Version.getGitCommitHash());
        subset.addPage(page);
        subset.setDocumentId(1L);
        PDPageLabels pdPageLabels = new PDPageLabels(subset);
        pdPageLabels.setLabelItem(pageIndex, pageLabel);
        subset.getDocumentCatalog().setPageLabels(pdPageLabels);

        subset.save(output);
    }

}
