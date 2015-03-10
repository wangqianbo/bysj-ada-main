package ict.ada.gdb.rest.services;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Path("/state/")
@Component
@Scope("request")
public class StateService {

  @GET
  @Produces("text/plain")
  @Path("/rowcount/{channel}")
  public Response getRelationSourceById(@DefaultValue("all") @PathParam("channel") String channel) {
    return Response.status(Status.OK).entity(InternalStateService.getTableRowCount(channel))
        .build();
  }
  @GET
  @Produces("text/plain")
  @Path("/relationtype/{channel}")
  public Response getRelationTypes(@DefaultValue("knowledge") @PathParam("channel") String channel) {
    return Response.status(Status.OK).entity(InternalStateService.getRelationTypes(channel))
        .build();
  }
}