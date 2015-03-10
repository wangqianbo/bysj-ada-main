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

@Path("/nodeset/")
@Component
@Scope("request")
public class NodeSetService {

  @GET
  @Produces("text/plain")
  @Path("/{nodes}/correlate/{type}")
  public Response getNodesRelRelativeNodes(
      @DefaultValue("NOEXISTS") @PathParam("nodes") String nodeids, @PathParam("type") String type,
      @DefaultValue("0") @QueryParam("start") int start,
      @DefaultValue("10") @QueryParam("len") int len) {
    return Response.status(Status.OK)
        .entity(InternalNodeService.getNodesRelRelativeNodes(nodeids, type, start, len)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{nodes}/correlate")
  public Response getNodesRelRelativeNodesAll(
      @DefaultValue("NOEXISTS") @PathParam("nodes") String nodeids,
      @DefaultValue("0") @QueryParam("start") int start,
      @DefaultValue("10") @QueryParam("len") int len) {
    return Response.status(Status.OK)
        .entity(InternalNodeService.getNodesRelRelativeNodes(nodeids, "all", start, len)).build();
  }

}