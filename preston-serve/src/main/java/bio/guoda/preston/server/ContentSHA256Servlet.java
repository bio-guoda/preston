package bio.guoda.preston.server;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class ContentSHA256Servlet extends HttpServlet {

    protected void doGet(
            HttpServletRequest request,
            HttpServletResponse response)
            throws ServletException, IOException {

        ServletConfig servletConfig = getServletConfig();


        response.setContentType("text/plain;utf8");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("pong");
    }

}
