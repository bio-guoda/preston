package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.csv;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.docx;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.epub;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.html;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.jpg;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.odp;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.ods;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.odt;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.pdf;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.png;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.pptx;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.rtf;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.svg;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.tsv;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.txt;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.xlsx;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.zip;

public class RegistryReaderGoogleDrive extends ProcessorReadOnly {

    private static final Pattern GOOGLE_DRIVE_URL_PATTERN
            = Pattern.compile("https://.*google.com/(?<type>[a-z]+)/d/(?<id>[a-zA-Z0-9-_]+)/{0,1}.*(?<slideId>#slide=id[.][a-z0-9_]+){0,1}");
    private static final Pattern GID_PATTERN
            = Pattern.compile(".*gid=(?<gid>[0-9]+).*");
    private static final Pattern TAB_PATTERN
            = Pattern.compile(".*tab=(?<tab>[a-z0-9.]+).*");
    private static final Pattern SLIDE_PATTERN
            = Pattern.compile(".*#slide=id[.](?<slideId>[a-z0-9_]+).*");
    private static final GoogleResourceId NOT_A_GOOGLE_RESOURCE_ID
            = new GoogleResourceId(null, null, null);

    public static final String PRESENTATION = "presentation";
    public static final String SPREADSHEETS = "spreadsheets";
    public static final String DOCUMENT = "document";

    private static final TreeMap<String, List<Type>> TYPES_FOR_TYPES = new TreeMap<String, List<Type>>() {{
        put(DOCUMENT, Arrays.asList(pdf, docx, txt, odt, rtf, epub, html));
        put(PRESENTATION, Arrays.asList(pdf, pptx, odp, txt, html, png, jpg, svg));
        put(SPREADSHEETS, Arrays.asList(xlsx, ods, pdf, csv, tsv, zip));
    }};
    public static final List<Type> TYPES_SINGLE_TABLE = Arrays.asList(Type.tsv, Type.csv);

    public RegistryReaderGoogleDrive(KeyValueStoreReadOnly blobStoreReadOnly, StatementsListener... listeners) {
        super(blobStoreReadOnly, listeners);
    }

    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)) {
            IRI versionSource = RefNodeFactory.getVersionSource(statement);
            String versionSourceString = versionSource.getIRIString();
            GoogleResourceId googleResourceId = getGoogleResourceId(versionSourceString);
            if (likelyGoogleResource(googleResourceId)) {
                exportTypesFor(googleResourceId)
                        .forEach(idType -> emitExportStatements(
                                idType.getLeft(),
                                idType.getRight(),
                                versionSourceString,
                                this)
                        );
            }
        }
    }

    private Stream<Pair<GoogleResourceId, Type>> exportTypesFor(GoogleResourceId googleResourceId) {
        List<Type> types = TYPES_FOR_TYPES.get(googleResourceId.getType());
        return types == null ? Stream.empty() : types.stream().map(t -> Pair.of(googleResourceId, t));
    }

    private static boolean likelyGoogleResource(GoogleResourceId googleResourceId) {
        return !NOT_A_GOOGLE_RESOURCE_ID.equals(googleResourceId);
    }

    enum Type {
        txt("txt", "text/plain"),
        csv("csv", "text/csv"),
        tsv("tsv", "text/tab-separated-values"),
        zip("zip", "application/zip"),
        html("html", "application/zip"),
        pdf("pdf", "application/pdf"),
        docx("docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        xlsx("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        pptx("pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation"),
        epub("epub", "application/epub+zip"),
        png("png", "image/png"),
        jpg("jpg", "image/jpeg"),
        svg("svg", "image/svg+xml"),
        odt("odt", "application/vnd.oasis.opendocument.text"),
        ods("ods", "application/vnd.oasis.opendocument.spreadsheet"),
        odp("odp", "application/vnd.oasis.opendocument.presentation"),
        rtf("rtf", "application/application/rtf");

        public String getMimeType() {
            return mimeType;
        }

        public String getType() {
            return type;
        }

        private final String mimeType;
        private final String type;

        Type(String type, String mimeType) {
            this.type = type;
            this.mimeType = mimeType;
        }
    }

    static class GoogleResourceId {
        private final String type;

        private final String id;
        private final String gid;

        GoogleResourceId(String id, String type) {
            this(id, type, null);
        }

        GoogleResourceId(String id, String type, String gid) {
            this.id = id;
            this.type = type;
            this.gid = gid;
        }

        public String getType() {
            return type;
        }

        public String getId() {
            return id;
        }

        public String getGid() {
            return gid;
        }

    }

    static GoogleResourceId getGoogleResourceId(String url) {
        GoogleResourceId grid = NOT_A_GOOGLE_RESOURCE_ID;
        Matcher matcher = GOOGLE_DRIVE_URL_PATTERN.matcher(url);
        if (matcher.matches()) {
            grid = new GoogleResourceId(matcher.group("id"), matcher.group("type"));
            if (StringUtils.equals(SPREADSHEETS, matcher.group("type"))) {
                Matcher matcher1 = GID_PATTERN.matcher(url);
                if (matcher1.matches()) {
                    grid = new GoogleResourceId(matcher.group("id"), matcher.group("type"), matcher1.group("gid"));
                }
            } else if (StringUtils.equals(PRESENTATION, matcher.group("type"))) {
                Matcher matcher1 = SLIDE_PATTERN.matcher(url);
                if (matcher1.matches()) {
                    grid = new GoogleResourceId(matcher.group("id"), matcher.group("type"), matcher1.group("slideId"));
                }
            } else if (StringUtils.equals(DOCUMENT, matcher.group("type"))) {
                Matcher matcher1 = TAB_PATTERN.matcher(url);
                if (matcher1.matches()) {
                    grid = new GoogleResourceId(matcher.group("id"), matcher.group("type"), matcher1.group("tab"));
                }
            }
        }
        return grid;
    }

    static void emitExportStatements(GoogleResourceId id, Type type, String originalUrl, StatementEmitter emitter) {
        if (likelyGoogleResource(id)) {
            Stream.of(
                    toStatement(
                            toExportIRI(id, type),
                            RefNodeConstants.WAS_DERIVED_FROM,
                            RefNodeFactory.toIRI(originalUrl)),
                    toStatement(
                            toExportIRI(id, type),
                            RefNodeConstants.HAS_FORMAT,
                            RefNodeFactory.toLiteral(type.mimeType)),
                    toStatement(
                            toExportIRI(id, type),
                            RefNodeConstants.HAS_VERSION,
                            RefNodeFactory.toBlank())
            ).forEach(emitter::emit);
        }
    }

    static IRI toExportIRI(GoogleResourceId id, Type type) {
        StringBuilder exportUrl = new StringBuilder();
        exportUrl.append("https://docs.google.com/");
        exportUrl.append(id.getType());
        exportUrl.append("/u/0/export?id=");
        exportUrl.append(id.getId());

        if (PRESENTATION.equals(id.getType())) {
            if (StringUtils.isNotBlank(id.getGid())
                    && Arrays.asList(png, jpg, svg).contains(type)) {
                exportUrl.append("&pageid=");
                exportUrl.append(id.getGid());
            }
        } else if (DOCUMENT.equals(id.getType())
                && StringUtils.isNotBlank(id.getGid())) {
            exportUrl.append("&tab=");
            exportUrl.append(id.getGid());
        } else if (StringUtils.isNotBlank(id.getGid())
                && TYPES_SINGLE_TABLE.contains(type)) {
            exportUrl.append("&gid=");
            exportUrl.append(id.getGid());
        }
        exportUrl.append("&format=");
        exportUrl.append(type.type);

        return RefNodeFactory.toIRI(exportUrl.toString());
    }

}
