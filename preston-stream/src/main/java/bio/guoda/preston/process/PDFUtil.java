package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.stream.ContentStreamException;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentCatalog;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.common.PDPageLabelRange;
import org.apache.pdfbox.pdmodel.common.PDPageLabels;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

public class PDFUtil {
    public static PageSelected selectPage(String requestedPageLabel, PDDocument doc, int index) throws IOException, ContentStreamException {
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

        return new PageSelected(page, index, pageLabelRange);
    }

    private static boolean useLabel(String pageLabel, PDPageLabels pageLabels) {
        return pageLabels != null && pageLabels.getPageIndicesByLabels().containsKey(pageLabel);
    }

    private static boolean useIndexAsLabel(String pageLabel, PDPageLabels pageLabels) {
        return pageLabels == null && NumberUtils.isDigits(pageLabel);
    }

    public static void saveAsPDF(PageSelected pageSelected,
                                 IRI contentId,
                                 OutputStream output) throws IOException {
        saveAsPDF(Arrays.asList(pageSelected), contentId, output);
    }


    public static void saveAsPDF(List<PageSelected> selectedPages,
                                 IRI contentId,
                                 OutputStream output) throws IOException {
        PDDocument subset = new PDDocument();


        subset.getDocumentInformation().setCustomMetadataValue(RefNodeConstants.WAS_DERIVED_FROM.getIRIString(), contentId.getIRIString());
        subset.getDocumentInformation().setTitle(contentId.toString());
        //subset.getDocumentInformation().setProducer(Preston.class.getSimpleName() + " v" + Version.getVersionString() + "@" + Version.getGitCommitHash());
        PDPageLabels pdPageLabels = new PDPageLabels(subset);
        for (PageSelected selectedPage : selectedPages) {
            subset.addPage(selectedPage.getPage());
            pdPageLabels.setLabelItem(selectedPage.getIndex(), selectedPage.getPageLabelRange());
        }
        subset.setDocumentId(1L);
        subset.getDocumentCatalog().setPageLabels(pdPageLabels);

        subset.save(output);
    }
}
