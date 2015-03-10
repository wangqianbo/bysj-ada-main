package ict.ada.gdb.rest.services;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Path("/edge/")
@Component
@Scope("request")
public class EdgeService {

  @GET
  @Produces("text/plain")
  @Path("/{id}/info")
  public Response getEdgeInfomationById(@DefaultValue("NOEXISTS") @PathParam("id") String id) {
    return Response.status(Status.OK).entity(InternalEdgeService.getEdgeInfomationById(id)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{id}/timeline")
  public Response getEdgeTimeLine(@DefaultValue("NOEXISTS") @PathParam("id") String id,
      @DefaultValue("-1") @QueryParam("st") int st, @DefaultValue("-1") @QueryParam("et") int et) {

    return Response.status(Status.OK).entity(InternalEdgeService.getEdgeTimeLine(id, st, et))
        .build();
  }
}