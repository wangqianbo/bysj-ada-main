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

@Path("/openio_ada/")
@Component
@Scope("request")
public class OpenioService {
  @GET
  @Produces("text/plain")
  @Path("/inference/{target}")
  public Response getRelationSourceById(@DefaultValue("NOEXISTS") @PathParam("target") String target) {
    return Response.status(Status.OK).entity(InternalOpenioService.getOpenioInference(target))
        .build();
  }

  @GET
  @Produces("text/plain")
  @Path("/inference_scan/{target}")
  public Response scanRelationSourceById(
      @DefaultValue("NOEXISTS") @PathParam("target") String target,
      @DefaultValue("0") @QueryParam("userid") int userid,
      @DefaultValue("0") @QueryParam("start") int start,
      @DefaultValue("10") @QueryParam("len") int len) {
    return Response.status(Status.OK)
        .entity(InternalOpenioService.scanOpenioInference(target, userid, start, len)).build();
  }
}
