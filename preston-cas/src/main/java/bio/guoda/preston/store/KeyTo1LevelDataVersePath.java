package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KeyTo1LevelDataVersePath implements KeyToPath {

    public static final String API_QUERY_FRAGMENT = "api/search?q=fileMd5:";

    public static final String MAGIC_HOST = "dataverse.org";

    private final URI baseURI;
    private final Dereferencer<InputStream> deref;

    private static final List<String> registeredDataVerseEndpoints = Collections.synchronizedList(new ArrayList<>());
    private static final List<String> registeredDataVerseHosts = Collections.synchronizedList(new ArrayList<>());
    private static final List<String> failedHosts = Collections.synchronizedList(new ArrayList<>());

    public KeyTo1LevelDataVersePath(URI baseURI,
                                    Dereferencer<InputStream> deref) {
        this.deref = deref;
        this.baseURI = baseURI;
    }

    @Override
    public URI toPath(IRI key) {
        URI path = null;
        lazyInitHostList();
        if (isSupportedHost()) {
            HashType hashType = HashKeyUtil.hashTypeFor(key);
            int offset = hashType.getPrefix().length();
            String md5HexHash = StringUtils.substring(key.getIRIString(), offset);

            if (StringUtils.equals(baseURI.getHost(), MAGIC_HOST)) {
                Optional<URI> first = registeredDataVerseHosts
                        .stream()
                        .filter(x -> !failedHosts.contains(x))
                        .map(host -> Pair.of(queryForHost(md5HexHash, URI.create("https://" + host)), host))
                        .map(q -> Optional.ofNullable(findFirstAndDisqualifyIfNeeded(q.getLeft(), q.getRight())))
                        .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                        .findFirst();
                path = first.orElse(null);
            } else {
                path = findFirst(queryForHost(md5HexHash, baseURI));
            }
        }
        return path;
    }

    private boolean isSupportedHost() {
        return baseURI != null
                && (StringUtils.equals(baseURI.getHost(), MAGIC_HOST) || registeredDataVerseHosts.contains(baseURI.getHost()));
    }

    private IRI queryForHost(String md5HexHash, URI host) {
        String s = host.toString();
        String prefixEndingWithSlash = StringUtils.endsWith(s, "/") ? s : s + "/";
        return RefNodeFactory.toIRI(prefixEndingWithSlash + API_QUERY_FRAGMENT + md5HexHash);
    }

    private URI findFirst(IRI query) {
        URI path = null;
        try (InputStream inputStream = deref.get(query)) {
            path = findFirstHit(inputStream);
        } catch (IOException e) {
            // opportunistic
        }
        return path;
    }

    private URI findFirstAndDisqualifyIfNeeded(IRI query, String host) {
        URI path = null;
        try (InputStream inputStream = deref.get(query)) {
            path = findFirstHit(inputStream);
        } catch (IOException e) {
            // opportunistic
            failedHosts.add(host);
        }
        return path;
    }

    private void lazyInitHostList() {
        if (registeredDataVerseEndpoints.size() == 0) {
            try (InputStream inputStream = deref.get(RefNodeFactory.toIRI("https://iqss.github.io/dataverse-installations/data/data.json"))) {
                registeredDataVerseEndpoints.addAll(findHostNames(inputStream));
            } catch (IOException e) {
                // opportunistic
            } finally {
                if (registeredDataVerseEndpoints.size() == 0) {
                    registeredDataVerseEndpoints.addAll(getHardcodedEndpoints());
                }
            }
            registeredDataVerseHosts.addAll(registeredDataVerseEndpoints
                    .stream()
                    .map(endpoint -> URI.create("https://" + endpoint)
                            .getHost())
                    .collect(Collectors.toList()));
        }
    }


    static URI findFirstHit(InputStream inputStream) throws IOException {
        if (inputStream == null) {
            throw new IOException("no input found");
        }
        JsonNode jsonNode = new ObjectMapper().readTree(inputStream);

        if (jsonNode != null && jsonNode.has("data")) {
            JsonNode data = jsonNode.get("data");
            if (data.has("items")) {
                JsonNode items = data.get("items");
                for (JsonNode item : items) {
                    if (item.has("url")) {
                        return URI.create(item.get("url").asText());
                    }
                }
            }
        }
        return null;
    }

    static List<String> findHostNames(InputStream inputStream) throws IOException {

        List<String> hosts = new ArrayList<>();
        if (inputStream == null) {
            throw new IOException("no input found");
        }
        JsonNode jsonNode = new ObjectMapper().readTree(inputStream);

        if (jsonNode != null && jsonNode.has("installations")) {
            JsonNode installations = jsonNode.get("installations");
            for (JsonNode installation : installations) {
                if (installation.has("host")) {
                    hosts.add(installation.get("host").asText());
                }

            }
        }
        return hosts;
    }

    public List<String> getHardcodedEndpoints() {
        String[] split = StringUtils.split("abacus.library.ubc.ca\n" +
                "dataverse.theacss.org\n" +
                "dataverse.ada.edu.au\n" +
                "dadosdepesquisa.fiocruz.br\n" +
                "dataverse.asu.edu\n" +
                "data.aussda.at\n" +
                "bonndata.uni-bonn.de\n" +
                "borealisdata.ca\n" +
                "dataverse.bhp.org.bw\n" +
                "data.brin.go.id\n" +
                "dataverse.cbpf.br\n" +
                "opendata.cesa.edu.co\n" +
                "dataverse.cidacs.org\n" +
                "data.cifor.org\n" +
                "data.cimmyt.org\n" +
                "dataverse.cirad.fr\n" +
                "science-data.hu\n" +
                "dataverse.csuc.cat\n" +
                "datasets.coronawhy.org\n" +
                "data.crossda.hr\n" +
                "researchdata.cuhk.edu.hk\n" +
                "dados.ipb.pt\n" +
                "archaeology.datastations.nl\n" +
                "ssh.datastations.nl\n" +
                "dare.uol.de\n" +
                "dataverse.dartmouth.edu\n" +
                "darus.uni-stuttgart.de\n" +
                "dataverse.ird.fr\n" +
                "data.sciencespo.fr\n" +
                "datarepositorium.sdum.uminho.pt\n" +
                "dataspace.ust.hk\n" +
                "edatos.consorciomadrono.es\n" +
                "dataverse.nl\n" +
                "dataverse.no\n" +
                "dataverse.rhi.hi.is\n" +
                "dorel.univ-lorraine.fr\n" +
                "researchdata.ntu.edu.sg\n" +
                "dunas.ua.pt\n" +
                "edmond.mpdl.mpg.de\n" +
                "dataverse.fgv.br\n" +
                "dataverse.fiu.edu\n" +
                "dvn.fudan.edu.cn\n" +
                "dataverse.orc.gmu.edu\n" +
                "data.univ-gustave-eiffel.fr\n" +
                "data.goettingen-research-online.de\n" +
                "dataverse.harvard.edu\n" +
                "heidata.uni-heidelberg.de\n" +
                "repositoriopesquisas.ibict.br\n" +
                "dataverse.icrisat.org\n" +
                "dataverse.mpi-sws.org\n" +
                "dataverse.ifdc.org\n" +
                "datasets.iisg.amsterdam\n" +
                "indata.cedia.edu.ec\n" +
                "dataverse.pushdom.ru\n" +
                "data.cipotato.org\n" +
                "dataverse.ipgp.fr\n" +
                "dataverse.iit.it\n" +
                "archive.data.jhu.edu\n" +
                "dataverse.jpl.nasa.gov\n" +
                "data.fz-juelich.de\n" +
                "keen.zih.tu-dresden.de\n" +
                "rdr.kuleuven.be\n" +
                "dataverse.lib.virginia.edu\n" +
                "lida.dataverse.lt\n" +
                "dataverse.acg.maine.edu/dvn\n" +
                "data.mel.cgiar.org\n" +
                "researchdata.nie.edu.sg\n" +
                "dataverse.nioz.nl\n" +
                "dataverse.lib.nycu.edu.tw\n" +
                "portal.odissei.nl\n" +
                "dataverse.uclouvain.be\n" +
                "dataverse.openforestdata.pl\n" +
                "osnadata.ub.uni-osnabrueck.de\n" +
                "papyrus-datos.co\n" +
                "opendata.pku.edu.cn\n" +
                "datos.pucp.edu.pe\n" +
                "data.qdr.syr.edu\n" +
                "entrepot.recherche.data.gouv.fr\n" +
                "redape.dados.embrapa.br\n" +
                "dataverse.unr.edu.ar\n" +
                "datos.uchile.cl\n" +
                "datos.unlp.edu.ar\n" +
                "research-data.urosario.edu.co\n" +
                "datav.udec.cl\n" +
                "repositoriodedados.unifesp.br\n" +
                "dataverse.ufabc.edu.br\n" +
                "dataverse.ileel.ufu.br\n" +
                "repositorio.polen.fccn.pt\n" +
                "dadosabertos.rnp.br\n" +
                "solo.mapbiomas.org\n" +
                "rodbuk.pl\n" +
                "agh.rodbuk.pl\n" +
                "pk.rodbuk.pl\n" +
                "uek.rodbuk.pl\n" +
                "uj.rodbuk.pl\n" +
                "dataverse.rsu.lv\n" +
                "data.scielo.org\n" +
                "sodha.be\n" +
                "datahub.tec.mx\n" +
                "dataverse.tdl.org\n" +
                "planetary-data-portal.org\n" +
                "dataverse.ucla.edu\n" +
                "dataverse.lib.unb.ca\n" +
                "dataverse.unc.edu\n" +
                "dataverse.lib.umanitoba.ca\n" +
                "dataverse.unimi.it\n" +
                "dataverse.vtti.vt.edu\n" +
                "data.worldagroforestry.org", "\n");
        return Arrays.asList(split);
    }

    @Override
    public boolean supports(IRI key) {
        return HashType.md5.equals(HashKeyUtil.hashTypeFor(key));
    }


}
