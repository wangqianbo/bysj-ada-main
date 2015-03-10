package ict.ada.gdb.rest.services;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

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

@Path("/node/")
@Component
@Scope("request")
public class NodeService {

  @GET
  @Produces("text/plain")
  @Path("/search")
  public Response getNodeNameFromInternalIndex(
      @DefaultValue("NOEXISTS") @QueryParam("q") String name,
      @DefaultValue("all") @QueryParam("type") String type,
      @DefaultValue("all") @QueryParam("channel") String channel,
      @DefaultValue("0") @QueryParam("start") int start,
      @DefaultValue("20") @QueryParam("len") int len,
      @DefaultValue("false")@QueryParam("uniq")boolean uniq) {
    return Response.status(Status.OK)
        .entity(InternalNodeService.getNodeNameFromInternalIndex(name, channel, type, start, len,uniq))
        .build();
  }

  @GET
  @Produces("text/plain")
  @Path("/id/{name}")
  public Response getNodeIdByName(@DefaultValue("NOEXISTS") @PathParam("name") String name) {
    // String nameDecoded = "NOEXISTS";
    // try {
    // / System.out.println("name: " +name);
    // nameDecoded = URLDecoder.decode(name, "UTF-8");
    // System.out.println(nameDecoded);
    // return Response.status(Status.OK).entity(InternalNodeService.getNodeIdByName(name, type))
    // .build();
    // } catch (UnsupportedEncodingException e) {
    // nameDecoded = "NOEXISTS";
    // e.printStackTrace();
    // }
    // return Response.status(Status.OK).entity(Constants.getNodeXmls().get(nameDecoded)).build();

    return Response.status(Status.OK).entity(InternalNodeService.getNodeIdByName(name)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{id}/edge")
  public Response getRelationByNodeId(@Context HttpServletRequest request,
      @PathParam("id") String nodeId, @DefaultValue("false") @QueryParam("relinfo") boolean weight,
      @DefaultValue("0") @QueryParam("start") int start,
      @DefaultValue("100") @QueryParam("len") int len,
      @DefaultValue("-1") @QueryParam("st") long st, @DefaultValue("-1") @QueryParam("et") long et,
      @DefaultValue("None") @QueryParam("fnode") String fnode,
      @DefaultValue("false") @QueryParam("disambiguation") boolean disambiguation,
      @DefaultValue("all") @QueryParam("relation") String relations,
      @DefaultValue("false") @QueryParam("staticstic") boolean staticstic
      ) {
    return Response
        .status(Status.OK)
        .entity(
            InternalNodeService.getNodeRelById(nodeId, null, null, weight, start, len, st, et,
                fnode, disambiguation, request.getHeader("WDEList"), relations,staticstic)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{id}/edge/{type}")
  public Response getRelationByNodeId(@Context HttpServletRequest request,
      @PathParam("id") String nodeId, @PathParam("type") String type,
      @DefaultValue("false") @QueryParam("relinfo") boolean weight,
      @DefaultValue("0") @QueryParam("start") int start,
      @DefaultValue("100") @QueryParam("len") int len,
      @DefaultValue("-1") @QueryParam("st") long st, @DefaultValue("-1") @QueryParam("et") long et,
      @DefaultValue("None") @QueryParam("fnode") String fnode,
      @DefaultValue("false") @QueryParam("disambiguation") boolean disambiguation,
      @DefaultValue("all") @QueryParam("relation") String relations,
      @DefaultValue("false") @QueryParam("staticstic") boolean staticstic
      ) {
    if (type.equals("all") || type.equals("ALL")) type = null;
    return Response
        .status(Status.OK)
        .entity(
            InternalNodeService.getNodeRelById(nodeId, null, type, weight, start, len, st, et,
                fnode, disambiguation, request.getHeader("WDEList"), relations,staticstic)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{id}/timeline")
  public Response getRelationTimeLineByNodeId(@PathParam("id") String nodeId) {
    return Response.status(Status.OK)
        .entity(InternalNodeService.getRelationTimeLineByNodeId(nodeId)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{id}/att")
  public Response getAttributesByNodeId(@Context HttpServletRequest request,
      @PathParam("id") String nodeId,
      @DefaultValue("false") @QueryParam("disambiguation") boolean disambiguation,
      @DefaultValue("false") @QueryParam("source") boolean source,@DefaultValue("true") @QueryParam("all") boolean all) {
    return Response
        .status(Status.OK)
        .entity(
            InternalNodeService.getAttributesById(nodeId, disambiguation,
                request.getHeader("WDEList"), source,all)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{id}/att/{filter}")
  public Response getFilteredAttributesByNodeId(@Context HttpServletRequest request,
      @PathParam("id") String nodeId, @PathParam("filter") String filter,
      @DefaultValue("false") @QueryParam("disambiguation") boolean disambiguation,
      @DefaultValue("false") @QueryParam("source") boolean source,@DefaultValue("true") @QueryParam("all") boolean all) {
    return Response
        .status(Status.OK)
        .entity(
            InternalNodeService.getFilteredAttributesById(nodeId, filter, disambiguation,
                request.getHeader("WDEList"), source,all)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{id}/action")
  public Response getNodeActionInfo(@PathParam("id") String nodeId,
      @DefaultValue("-1") @QueryParam("st") long st, @DefaultValue("-1") @QueryParam("et") long et) {
    return Response.status(Status.OK).entity(InternalNodeService.getNodeActionInfo(nodeId, st, et))
        .build();
  }
  @GET
  @Produces("text/plain")
  @Path("/{id}/action_reference/{value}")
  public Response getNodeActionRefInfo(@PathParam("id") String nodeId,@PathParam("value") String value,
      @DefaultValue("-1") @QueryParam("st") long st, @DefaultValue("-1") @QueryParam("et") long et) {
    return Response.status(Status.OK).entity(InternalNodeService.getNodeActionRefInfo(nodeId, value,st, et))
        .build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{id}/att_reference")
  public Response getAttributeSourceByNodeId(@PathParam("id") String nodeId,
      @QueryParam("type") String type, @QueryParam("value") String value) {
    return Response.status(Status.OK)
        .entity(InternalNodeService.getAttributeSourceById(nodeId, type, value)).build();
  }
  @GET
  @Produces("text/plain")
  @Path("/{id}/att_reference_detail")
  public Response getAttributeSourceByNodeId1(@PathParam("id") String nodeId,
      @QueryParam("type") String type, @QueryParam("value") String value) {
    return Response.status(Status.OK)
        .entity(InternalNodeService.getAttributeSourceById1(nodeId, type, value)).build();
  }
  @GET
  @Produces("text/plain")
  @Path("/{id}/name")
  public Response getNodeNameById(@PathParam("id") String nodeId) {
    return Response.status(Status.OK).entity(InternalNodeService.getNodeNameById(nodeId)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{id}/hierarchy")
  public Response getNodeHierarchyById(@PathParam("id") String nodeId,@DefaultValue("true")@QueryParam("sametype") boolean sameType) {
    return Response.status(Status.OK).entity(InternalNodeService.getNodeHierarchyById(nodeId,sameType))
        .build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{id}/subordinate")
  public Response getNodeSubordinateById(@PathParam("id") String nodeId,
      @QueryParam("addtion") String addtion,@DefaultValue("true")@QueryParam("sametype") boolean sameType) {
    return Response.status(Status.OK)
        .entity(InternalNodeService.getNodeSubordinateById(nodeId, addtion,sameType)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{id}/superior")
  public Response getNodeSuperiorById(@PathParam("id") String nodeId,
      @QueryParam("addtion") String addtion,@DefaultValue("true")@QueryParam("sametype") boolean sameType) {
    return Response.status(Status.OK)
        .entity(InternalNodeService.getNodeSuperiorById(nodeId, addtion,sameType)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{startNodeId}/inference")
  public Response getRelationInferData(
      @DefaultValue("NOEXISTS") @PathParam("startNodeId") String startNodeId,
      @Context HttpServletRequest request) {
    String rule = request.getHeader("Rule");
    if (rule != null) try {
      rule = new String(rule.getBytes("ISO-8859-1"), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return Response.status(Status.OK)
        .entity(InternalNodeService.getRelationInferData(startNodeId, rule)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{id}/edge/{type}/cluster")
  public Response getNodeRelNodeClusterById(@PathParam("id") String nodeId,
      @PathParam("type") String type, @DefaultValue("10") @QueryParam("min") int mincluster,
      @DefaultValue("20") @QueryParam("maxcluster") int maxcluster,
      @DefaultValue("500") @QueryParam("maxnode") int maxnode,
      @DefaultValue("1") @QueryParam("weight") int weight) {
    return Response
        .status(Status.OK)
        .entity(
            InternalNodeService.getNodeRelNodeClusterById(nodeId, type, mincluster, maxcluster,
                maxnode, weight)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{startNodeId}/path/{endNodeId}")
  public Response getPathBetNodes(
      @DefaultValue("NOEXISTS") @PathParam("startNodeId") String startNodeId,
      @DefaultValue("NOEXISTS") @PathParam("endNodeId") String endNodeId,
      @DefaultValue("4") @QueryParam("degree") int degree,
      @DefaultValue("1000") @QueryParam("max") int max,
      @DefaultValue("NOEXISTS") @QueryParam("type") String type) {
    return Response.status(Status.OK)
        .entity(InternalNodeService.getPathBetNodes(startNodeId, endNodeId, degree, max, type))
        .build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{startNodeId}/path2/{endNodeId}")
  public Response getPathBetNodes2(
      @DefaultValue("NOEXISTS") @PathParam("startNodeId") String startNodeId,
      @DefaultValue("NOEXISTS") @PathParam("endNodeId") String endNodeId,
      @DefaultValue("4") @QueryParam("degree") int degree,
      @DefaultValue("1000") @QueryParam("max") int max,
      @DefaultValue("NOEXISTS") @QueryParam("type") String type) {
    return Response.status(Status.OK)
        .entity(InternalNodeService.getPathBetNodes2(startNodeId, endNodeId, degree, max, type))
        .build();
  }
  @GET
  @Produces("text/plain")
  @Path("/{startNodeId}/path3/{endNodeId}")
  public Response getPathBetNodes3(
      @DefaultValue("NOEXISTS") @PathParam("startNodeId") String startNodeId,
      @DefaultValue("NOEXISTS") @PathParam("endNodeId") String endNodeId,
      @DefaultValue("4") @QueryParam("degree") int degree,
      @DefaultValue("1000") @QueryParam("max") int max,
      @DefaultValue("NOEXISTS") @QueryParam("type") String type) {
    return Response.status(Status.OK)
        .entity(InternalNodeService.getPathBetNodes3(startNodeId, endNodeId, degree, max, type))
        .build();
  }
  @GET
  @Produces("text/plain")
  @Path("/{id}/edge/{type}/clique")
  public Response getCliquesOfNode(@PathParam("id") String nodeId, @PathParam("type") String type,
      @DefaultValue("1000") @QueryParam("maxnode") int limit,
      @DefaultValue("1") @QueryParam("maxclique") int maxcliques) {
    return Response.status(Status.OK)
        .entity(InternalNodeService.getCliquesOfNode(nodeId, type, limit, maxcliques)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{nodeId}/recommend")
  public Response getRecommendOfNode(@PathParam("nodeId") String nodeId) {
    return Response.status(Status.OK).entity(InternalNodeService.getRecommendOfNode(nodeId))
        .build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{nodeId}/map")
  public Response getMap(@PathParam("nodeId") String nodeId) {
    return Response.status(Status.OK).entity(InternalNodeService.getMap(nodeId)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{nodeId}/edge/{type}/community")
  public Response getNodeClusterC(@PathParam("nodeId") String nodeId,
      @PathParam("type") String type,
      @DefaultValue("20") @QueryParam("maxcommunity") int maxcommunity,
      @DefaultValue("500") @QueryParam("maxnode") int maxnode) {
    return Response.status(Status.OK)
        .entity(InternalNodeService.getNodeClusterC(nodeId, type, maxcommunity, maxnode)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{id}/documents")
  public Response getWdeRefsByNodeId(@DefaultValue("NOEXISTS") @PathParam("id") String id,
      @DefaultValue("0") @QueryParam("start") int start,
      @DefaultValue("100") @QueryParam("len") int len) {
    return Response.status(Status.OK).entity(InternalWdeService.getWdeRefsById(id, start, len))
        .build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{id}/gcommunity")
  public Response getGcommunity(@DefaultValue("NOEXISTS") @PathParam("id") String id,
      @DefaultValue("cm_hm") @QueryParam("method") String method) {
    return Response.status(Status.OK).entity(InternalNodeService.getGcommunity(id, method)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{id}/graph")
  public Response getTwoLevelRelationGraph(@DefaultValue("NOEXISTS") @PathParam("id") String id,
      @DefaultValue("0") @QueryParam("start") int start,
      @DefaultValue("10") @QueryParam("len") int len,
      @DefaultValue("10") @QueryParam("limit") int limit,
      @DefaultValue("false") @QueryParam("weight") boolean weight) {
    return Response.status(Status.OK)
        .entity(InternalNodeService.getTwoLevelRelationGraph(id, start, len, limit, weight))
        .build();
  }

  @GET
  @Produces("text/plain")
  @Path("/{id}/track")
  public Response getEventTrackInfo(@DefaultValue("NOEXISTS") @PathParam("id") String id) {
    return Response.status(Status.OK).entity(InternalNodeService.getEventTrackInfo(id)).build();
  }
  
  @GET
  @Produces("text/plain")
  @Path("/relevent/{nodeid}/ele-statistics/{type}")
  public Response getReleventEvents(@DefaultValue("NOEXISTS") @PathParam("nodeid") String nodeid,
      @DefaultValue("NOEXISTS") @PathParam("type") String typestr,
      @DefaultValue("10") @QueryParam("len") int len) {
    String[] typeArray = typestr.split(",");
    List<String> types = new ArrayList<String>(typeArray.length);
    for (String type : typeArray)
      types.add(type);
    return Response.status(Status.OK)
        .entity(InternalNodeService.getReleventNodes(nodeid, types, len)).build();
  }
  @GET
  @Produces("text/plain")
  @Path("/{id}/ents")
  public Response getNodeEnts(@PathParam("id") String nodeId) {
    return Response.status(Status.OK).entity(InternalNodeService.getNodeEnts(nodeId))
        .build();
  }
}