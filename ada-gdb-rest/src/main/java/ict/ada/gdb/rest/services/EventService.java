package ict.ada.gdb.rest.services;

import java.util.ArrayList;
import java.util.List;

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

@Path("/event/")
@Component
@Scope("request")
public class EventService {
  @GET
  @Produces("text/plain")
  @Path("/search")
  public Response getNodeNameFromInternalIndex(
      @DefaultValue("NOEXISTS") @QueryParam("q") String name,
      @DefaultValue("NOEXISTS") @QueryParam("channel") String channel,
      @DefaultValue("NOEXISTS") @QueryParam("method") String method,
      @DefaultValue("0") @QueryParam("start") int start,
      @DefaultValue("20") @QueryParam("len") int len) {
    String result = InternalEventService.getEventNodeName(name, channel, method, start, len);
    return Response.status(Status.OK).entity(result).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/list")
  public Response getEventList(@DefaultValue("-1") @QueryParam("st") long st,
      @DefaultValue("-1") @QueryParam("et") long et,
      @DefaultValue("0") @QueryParam("start") int start,
      @DefaultValue("20") @QueryParam("len") int len,
      @DefaultValue("NOEXISTS") @QueryParam("channel") String channels,
      @DefaultValue("NOEXISTS") @QueryParam("method") String methods) {
    if (st != -1) st = st * 1000;
    if (et != -1) et = et * 1000;
    return Response
        .status(Status.OK)
        .entity(
            InternalEventService.getEventListInMultipleClasses(start, len, st, et, channels,
                methods, "it")).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/listc")
  public Response getCompleteEventList(@DefaultValue("-1") @QueryParam("st") long st,
      @DefaultValue("-1") @QueryParam("et") long et,
      @DefaultValue("0") @QueryParam("start") int start,
      @DefaultValue("20") @QueryParam("len") int len,
      @DefaultValue("NOEXISTS") @QueryParam("channel") String channels,
      @DefaultValue("NOEXISTS") @QueryParam("method") String methods) {
    if (st != -1) st = st * 1000;
    if (et != -1) et = et * 1000;
    return Response
        .status(Status.OK)
        .entity(
            InternalEventService.getEventListInMultipleClasses(start, len, st, et, channels,
                methods, "pt")).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/searchtitle")
  public Response getEventByTitle(@DefaultValue("0") @QueryParam("start") int start,
      @DefaultValue("20") @QueryParam("len") int len,
      @DefaultValue("NOEXISTS") @QueryParam("q") String q) {
    return Response.status(Status.OK).entity(InternalEventService.getEventByTitle(q, start, len))
        .build();
  }

  @GET
  @Produces("text/plain")
  @Path("/info/{eventid}/documents/")
  public Response getEventDocuments(@DefaultValue("NOEXISTS") @PathParam("eventid") String eventid,
      @DefaultValue("0") @QueryParam("start") int start,
      @DefaultValue("20") @QueryParam("len") int len) {
    eventid = eventid.startsWith("c9") ? eventid.substring(4) : eventid;
    return Response.status(Status.OK)
        .entity(InternalWdeService.getEventWdeRefsById(eventid, start, len)).build();
  }
  @GET
  @Produces("text/plain")
  @Path("/info/{eventid}/documents_detail/")
  public Response getEventDocuments1(@DefaultValue("NOEXISTS") @PathParam("eventid") String eventid,
      @DefaultValue("0") @QueryParam("start") int start,
      @DefaultValue("20") @QueryParam("len") int len) {
    eventid = eventid.startsWith("c9") ? eventid.substring(4) : eventid;
    return Response.status(Status.OK)
        .entity(InternalWdeService.getEventWdeRefsById1(eventid, start, len)).build();
  }
  @GET
  @Produces("text/plain")
  @Path("/relevent/{eventid}/ele-statistics/{type}")
  public Response getReleventEvents(@DefaultValue("NOEXISTS") @PathParam("eventid") String eventid,
      @DefaultValue("NOEXISTS") @PathParam("type") String typestr,
      @DefaultValue("20") @QueryParam("len") int len) {
    // eventid=eventid.startsWith("e601")?eventid.substring(4):eventid;
    String[] typeArray = typestr.split(",");
    List<String> types = new ArrayList<String>(typeArray.length);
    for (String type : typeArray)
      types.add(type);
    return Response.status(Status.OK)
        .entity(InternalEventService.getReleventEvents(eventid, types, len)).build();
  }

  @GET
  @Produces("text/plain")
  @Path("/relevent/{eventid}/tag-statistics")
  public Response getReleventEventsByEventId(
      @DefaultValue("NOEXISTS") @PathParam("eventid") String eventid,
      @DefaultValue("20") @QueryParam("len") int len) {
    String id = eventid.startsWith("c9") ? eventid.substring(4) : eventid;
    int _id = Integer.parseInt(id);
    return Response.status(Status.OK).entity(InternalEventService.getReleventEvents(_id, len))
        .build();
  }

}
