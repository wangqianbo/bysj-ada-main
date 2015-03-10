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

@Path("/relation/")
@Component
@Scope("request")
public class RelationService {

  @GET
  @Produces("text/plain")
  @Path("/{id}/reference")
  public Response getRelationSourceById(@DefaultValue("NOEXISTS") @PathParam("id") String id) {
    return Response.status(Status.OK).entity(InternalRelationService.getRelationSourceById(id))
        .build();
  }

  @GET
  @Produces("text/plain")
  @Path("/inference/{target}")
  public Response getRelationInfer(@DefaultValue("NOEXISTS") @PathParam("target") String target) {
    return Response.status(Status.OK)
        .entity(InternalRelationService.getRelationInferRulePath(target)).build();
  }

}