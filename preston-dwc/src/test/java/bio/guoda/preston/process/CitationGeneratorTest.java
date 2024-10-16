package bio.guoda.preston.process;

import bio.guoda.preston.cmd.CitationGenerator;
import bio.guoda.preston.cmd.Cmd;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.stream.ArchiveStreamHandler;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;

public class CitationGeneratorTest {
    @Test
    public void onZipBatchSize100() {

        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource("/bio/guoda/preston/plazidwca.zip");

                IRI iri = toIRI(resource.toExternalForm());

                if (StringUtils.equals("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1", key.getIRIString())) {
                    try {
                        return new FileInputStream(new File(URI.create(iri.getIRIString())));
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
                return null;
            }
        };


        Quad statement = toStatement(toIRI("blip"), HAS_VERSION, toIRI("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1"));


        Cmd cmd = new Cmd();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmd.setOutputStream(outputStream);

        CitationGenerator citationGenerator = new CitationGenerator(cmd, blobStore);
        citationGenerator.on(statement);

        String actual = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);

        assertThat(actual, Is.is("Campos, Ernesto, Hernández-Ávila, Iván (2010): Phylogeny of Calyptraeotheres Campos, 1990 (Crustacea, Decapoda, Brachyura, Pinnotheridae) with the description of C. pepeluisi new species from the tropical Mexican Pacific. Zootaxa 2691: 41-52, DOI: 10.5281/zenodo.199558. Accessed at <zip:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/eml.xml> .\n"
));
    }

    @Test
    public void onEmlWithBiography() {

        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource("/bio/guoda/preston/Ramírez-Chaves-et-al-2022.zip");

                IRI iri = toIRI(resource.toExternalForm());

                if (StringUtils.equals("hash://sha256/dbcb0c2feb882a172ccba4e56a87e2572874546a718adbcf43c62c82af22855d", key.getIRIString())) {
                    try {
                        return new FileInputStream(new File(URI.create(iri.getIRIString())));
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
                return null;
            }
        };


        Quad statement = toStatement(toIRI("blip"), HAS_VERSION, toIRI("hash://sha256/dbcb0c2feb882a172ccba4e56a87e2572874546a718adbcf43c62c82af22855d"));


        Cmd cmd = new Cmd();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmd.setOutputStream(outputStream);

        CitationGenerator citationGenerator = new CitationGenerator(cmd, blobStore);
        citationGenerator.on(statement);

        String actual = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);

        assertThat(actual.split("\n").length, Is.is(1));

        assertThat(actual, Is.is("Ramírez-Chaves H E, Mejía Fontecha I Y, Velasquez D, Castaño D (2022): Colección de Mamíferos (Mammalia) del Museo de Historia Natural de la Universidad de Caldas, Colombia. v2.7. Universidad de Caldas. Dataset/Occurrence. https://doi.org/10.15472/mnevig. Accessed at <zip:hash://sha256/dbcb0c2feb882a172ccba4e56a87e2572874546a718adbcf43c62c82af22855d!/eml.xml> .\n"));

    }

    @Test
    public void onEmlEmbeddedInTarGZ() {

        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource("/bio/guoda/preston/dwca.tar.gz");

                IRI iri = toIRI(resource.toExternalForm());

                if (StringUtils.equals("hash://sha256/bedcc1f122d59ec002e0e6d2802c0e422eadf6208669fff141a895bd3ed15d4a", key.getIRIString())) {
                    try {
                        return new FileInputStream(new File(URI.create(iri.getIRIString())));
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
                return null;
            }
        };


        Quad statement = toStatement(toIRI("blip"), HAS_VERSION, toIRI("hash://sha256/bedcc1f122d59ec002e0e6d2802c0e422eadf6208669fff141a895bd3ed15d4a"));


        Cmd cmd = new Cmd();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmd.setOutputStream(outputStream);

        CitationGenerator citationGenerator = new CitationGenerator(cmd, blobStore);
        citationGenerator.on(statement);

        String actual = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);

        assertThat(actual.split("\n").length, Is.is(1));

        assertThat(actual, Is.is("de Jong, Y.S.D.M. (ed.) (2010) Fauna Europaea version 2.4. Web Service available online at http://www.faunaeur.org. Accessed at <tar:gz:hash://sha256/bedcc1f122d59ec002e0e6d2802c0e422eadf6208669fff141a895bd3ed15d4a!/FaEu-DWCA/eml.xml> .\n"));

    }

    @Test
    public void checkSignatureZip() throws ArchiveException {
        InputStream resource = getClass().getResourceAsStream("/bio/guoda/preston/Ramírez-Chaves-et-al-2022.zip");


        Pair<ArchiveInputStream, String> archiveStreamAndFormat = ArchiveStreamHandler.getArchiveStreamAndFormat(resource);

        assertThat(archiveStreamAndFormat.getValue(), Is.is("zip"));

    }


    @Test
    public void checkFamousOrganism() throws ArchiveException {

        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource("/bio/guoda/preston/famous-organism.zip");

                IRI iri = toIRI(resource.toExternalForm());

                if (StringUtils.equals("hash://sha256/bedcc1f122d59ec002e0e6d2802c0e422eadf6208669fff141a895bd3ed15d4a", key.getIRIString())) {
                    try {
                        return new FileInputStream(new File(URI.create(iri.getIRIString())));
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
                return null;
            }
        };


        Quad statement = toStatement(toIRI("blip"), HAS_VERSION, toIRI("hash://sha256/bedcc1f122d59ec002e0e6d2802c0e422eadf6208669fff141a895bd3ed15d4a"));


        Cmd cmd = new Cmd();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmd.setOutputStream(outputStream);

        CitationGenerator citationGenerator = new CitationGenerator(cmd, blobStore);
        citationGenerator.on(statement);

        String actual = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);

        assertThat(actual.split("\n").length, Is.is(1));

        assertThat(actual, Is.is("Species named after famous people. Accessed at <zip:hash://sha256/bedcc1f122d59ec002e0e6d2802c0e422eadf6208669fff141a895bd3ed15d4a!/famous-organism-master/metadata.yaml>.\n"));

    }
    @Test
    public void checkFamousOrganismMissingMetadata() throws ArchiveException {

        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource("/bio/guoda/preston/famous-organism-missing-eml.zip");

                IRI iri = toIRI(resource.toExternalForm());

                if (StringUtils.equals("hash://sha256/bedcc1f122d59ec002e0e6d2802c0e422eadf6208669fff141a895bd3ed15d4a", key.getIRIString())) {
                    try {
                        return new FileInputStream(new File(URI.create(iri.getIRIString())));
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
                return null;
            }
        };


        Quad statement = toStatement(toIRI("blip"), HAS_VERSION, toIRI("hash://sha256/bedcc1f122d59ec002e0e6d2802c0e422eadf6208669fff141a895bd3ed15d4a"));


        Cmd cmd = new Cmd();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmd.setOutputStream(outputStream);

        CitationGenerator citationGenerator = new CitationGenerator(cmd, blobStore);
        citationGenerator.on(statement);

        String actual = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);

        assertThat(actual.split("\n").length, Is.is(1));

        assertThat(actual, Is.is("Accessed at <zip:hash://sha256/bedcc1f122d59ec002e0e6d2802c0e422eadf6208669fff141a895bd3ed15d4a!/famous-organism-24fcd76c70d1a55c99021d252dcd4e6a1e6c3707/meta.xml>.\n"));

    }

}
