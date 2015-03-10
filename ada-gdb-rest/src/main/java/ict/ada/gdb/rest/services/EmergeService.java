package ict.ada.gdb.rest.services;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Path("/emerge/*")
@Component
@Scope("request")
public class EmergeService {
  @GET
  @Path("{var:.*}")
  @Produces("text/plain")
  public Response redirect(@Context HttpServletRequest request) {
    String result = null;
    String URL = InternalServiceResources.getAdaGdbRestConf().getProperty("emerge");
    String URI = request.getRequestURI();
    // System.out.println("URI = "+URI);
    URL += URI;
    String queryString = request.getQueryString();
    // System.out.println("queryString = "+queryString);
    if (queryString != null && queryString.trim().length() != 0) URL += "?" + queryString;
    System.out.println("URL = " + URL);
    result = InternalServiceResources.downloadHtml(URL);
    return Response.status(Status.OK).entity(result).build();
  }
}
