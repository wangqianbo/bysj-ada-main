package ict.ada.gdb.rest.services;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Path("/wderefs/")
@Component
@Scope("request")
public class WdeService {
  @GET
  @Produces("text/plain")
  @Path("/{id}/detail")
  public Response getDetail(@DefaultValue("NOEXISTS") @PathParam("id") String id,
      @DefaultValue("0") @QueryParam("offset") String offset,
      @DefaultValue("0") @QueryParam("len") String len) {
    return Response.status(Status.OK).entity(InternalWdeService.getDetail(id, offset, len)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{id}/content")
  public Response getContent(@DefaultValue("NOEXISTS") @PathParam("id") String id) {
    return Response.status(Status.OK).entity(InternalWdeService.getContent(id)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{id}/html")
  public Response getHtml(@DefaultValue("NOEXISTS") @PathParam("id") String id) {
    return Response.status(Status.OK).entity(InternalWdeService.getHtml_url(id)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/details")
  public Response getDetails(@Context HttpServletRequest request) {
    return Response.status(Status.OK)
        .entity(InternalWdeService.getDetails(request.getHeader("DETAILS"))).build();
  }
}
