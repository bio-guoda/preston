package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.EmittingStreamOfAnyVersions;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.commons.io.output.NullPrintStream;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;

@CommandLine.Command(
        name = "ris-stream",
        header = "translates bibliographic citations from RIS format into Zenodo metadata in JSON lines format",
        description = "Stream RIS records into line-json with Zenodo metadata",
        footerHeading = "Examples",
        footer = {
                "%n1.",
                "First, append the associated bhl pdf via: ",
                "----",
                "preston track https://www.biodiversitylibrary.org/partpdf/326364",
                "----",
                "Finally, generate Zenodo metadata record.json using: ",
                "----",
                "Following, generate a RIS record, record.ris:",
                "----",
                "cat > record.ris <<__EOL__",
                "TY  - BOOK",
                "TI  - Faber, Helen R May 5, 1913",
                "T2  - Walter Deane correspondence",
                "UR  - https://www.biodiversitylibrary.org/part/326364",
                "PY  - 1913-05-05",
                "AU  - Faber, Helen R.,",
                "ER  -",
                "__EOL__",
                "----",
                "Then, track record.ris using Preston into Zenodo metadata using: ",
                "----",
                "cat record.ris\\",
                " | preston track",
                "----",
                "preston head\\",
                " | preston cat\\",
                " | preston ris-stream\\",
                " > record.json",
                "----",
                "where record.json:",
                "----",
                "{\n" +
                        "  \"metadata\": {\n" +
                        "    \"description\": \"(Uploaded by Plazi from the Biodiversity Heritage Library) No abstract provided.\",\n" +
                        "    \"communities\": [],\n" +
                        "    \"http://www.w3.org/ns/prov#wasDerivedFrom\": \"https://linker.bio/line:hash://sha256/5fd5944b52b22efc56f901d96ff53a64c42e1f2264763e2f1074ac2c589e47cf!/L1-L7\",\n" +
                        "    \"http://www.w3.org/1999/02/22-rdf-syntax-ns#type\": \"application/x-research-info-systems\",\n" +
                        "    \"title\": \"Faber, Helen R May 5, 1913\",\n" +
                        "    \"upload_type\": \"publication\",\n" +
                        "    \"publication_type\": \"other\",\n" +
                        "    \"journal_title\": \"Walter Deane correspondence\",\n" +
                        "    \"publication_date\": \"1913-05-05\",\n" +
                        "    \"referenceId\": \"https://www.biodiversitylibrary.org/part/326364\",\n" +
                        "    \"filename\": \"bhlpart326364.pdf\",\n" +
                        "    \"keywords\": [\n" +
                        "      \"Biodiversity\",\n" +
                        "      \"BHL-Corpus\",\n" +
                        "      \"Source: Biodiversity Heritage Library\",\n" +
                        "      \"Source: https://biodiversitylibrary.org\",\n" +
                        "      \"Source: BHL\"\n" +
                        "    ],\n" +
                        "    \"creators\": [\n" +
                        "      {\n" +
                        "        \"name\": \"Faber, Helen R.\"\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"related_identifiers\": [\n" +
                        "      {\n" +
                        "        \"relation\": \"isDerivedFrom\",\n" +
                        "        \"identifier\": \"https://linker.bio/line:hash://sha256/5fd5944b52b22efc56f901d96ff53a64c42e1f2264763e2f1074ac2c589e47cf!/L1-L7\"\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"relation\": \"isDerivedFrom\",\n" +
                        "        \"identifier\": \"https://www.biodiversitylibrary.org/part/326364\"\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"relation\": \"isAlternateIdentifier\",\n" +
                        "        \"identifier\": \"urn:lsid:biodiversitylibrary.org:part:326364\"\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"relation\": \"isPartOf\",\n" +
                        "        \"identifier\": \"hash://sha256/3983c9abbba981838de5d47a5dadf94c4afcea7df63486effb71d780e592ebe8\"\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"relation\": \"hasVersion\",\n" +
                        "        \"identifier\": \"hash://md5/7fddbf186c6bbddb0b49919fc340bb61\"\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"relation\": \"hasVersion\",\n" +
                        "        \"identifier\": \"hash://sha256/9b30af8f432b78e0d739b0457376dac998057a5b4b5fccd52b81560ec1f4f146\"\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  }\n" +
                        "}\n",
                "----"
        }

)
public class CmdRISStream extends LoggingPersisting implements Runnable {

    @CommandLine.Option(
            names = {"--community", "--communities"},
            split = ",",
            description = "select which Zenodo communities to submit to. If community is known (e.g., batlit, taxodros), default metadata is included."
    )

    private List<String> communities = new ArrayList<>();

    @CommandLine.Option(
            names = {"--reuse-doi"},
            defaultValue = "false",
            description = "use existing DOI in Zenodo deposit if available"
    )
    private Boolean ifAvailableUseExistingDOI = false;


    @Override
    public void run() {
        BlobStoreReadOnly blobStoreReadonly
                = new BlobStoreAppendOnly(getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()), true, getHashType());
        BlobStoreReadOnly blobStoreWithIndexedVersions = BlobStoreUtil.createIndexedBlobStoreFor(blobStoreReadonly, this);

        run(blobStoreWithIndexedVersions);

    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                NullPrintStream.INSTANCE,
                LogErrorHandlerExitOnError.EXIT_ON_ERROR);

        StatementsListener textMatcher = new RISFileExtractor(
                this,
                blobStoreReadOnly,
                getOutputStream(),
                communities,
                ifAvailableUseExistingDOI,
                listener);

        StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {

            @Override
            public void emit(Quad statement) {
                textMatcher.on(statement);
            }
        };

        new EmittingStreamOfAnyVersions(emitter, this)
                .parseAndEmit(getInputStream());

    }

}

