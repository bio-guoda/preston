package bio.guoda.preston.server;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import picocli.CommandLine;

import java.util.Map;
import java.util.TreeMap;

@CommandLine.Command(
        name = "redirect",
        aliases = {"r", "proxy"},
        description = "attempts to redirect to content associated with provided identifier in a defined content universe"
)
public class CmdRedirect implements Runnable {

    @CommandLine.Option(
            names = {"-p", "--port"},
            description = "port to listen on"
    )
    Integer port = 8080;

    @CommandLine.Option(
            names = {"-h", "--host"},
            description = "host to listen on"
    )
    String host = "localhost";

    @CommandLine.Option(
            names = {"--registry"},
            description = "sparql endpoint to query provenance registry"
    )
    String registry = "http://localhost:7878/query";

    @CommandLine.Option(
            names = {"--repository"},
            description = "endpoint to access content of known provenance"
    )
    String repository = "https://linker.bio/";


    @Override
    public void run() {
        Server server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);
        connector.setHost(host);
        server.setConnectors(new Connector[] {connector});
        ServletHandler servletHandler = new ServletHandler();
        ServletHolder servletHolder = new ServletHolder(RedirectingServlet.class);
        Map<String, String> properties = new TreeMap<String, String>() {{
            put(PropertyNames.PRESTON_CONTENT_RESOLVER_ENDPONT, repository);
            put(PropertyNames.PRESTON_SPARQL_ENDPONT, registry);
        }};

        servletHolder.setInitParameters(properties);

        servletHandler.addServletWithMapping(servletHolder, "/");
        server.insertHandler(servletHandler);
        try {
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

