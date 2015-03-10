package ict.ada.gdb.rest.services;

import ict.ada.gdb.rest.dtos.ExceptionDTO;
import ict.ada.gdb.rest.dtos.MessageConverter;
import ict.ada.gdb.rest.dtos.MessageDTO;
import ict.ada.gdb.rest.dtos.MessageListDTO;
import ict.ada.gdb.rest.entities.Message;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Path("/gdb-example/")
@Component
@Scope("request")
public class ExampleService {

  private static List<Message> messages = new ArrayList<Message>(Arrays.asList(new Message(
      "First message"), new Message("Second message")));

  @GET
  @Produces("application/xml")
  public MessageListDTO getMessages() {
    return MessageConverter.toDTO(messages);
  }

  @GET
  @Produces("application/json")
  @Path("/json")
  public MessageListDTO getMessagesJSON() {
    return getMessages();
  }

  @GET
  @Produces("application/xml")
  @Path("/{index}")
  public Response getMessage(@PathParam("index") int index) {
    try {
      return Response.status(Status.OK).entity(MessageConverter.toDTO(messages.get(index))).build();
    } catch (IndexOutOfBoundsException e) {
      return Response.status(Status.NOT_FOUND).entity(new ExceptionDTO(e)).build();
    }
  }

  @POST
  @Consumes("application/xml")
  public Response addMessage(MessageDTO messageDTO) {
    Message entity = MessageConverter.toEntity(messageDTO);
    messages.add(entity);
    return Response.status(Status.OK).build();
  }

  @PUT
  @Consumes("application/xml")
  @Path("/{index}")
  public Response updateMessage(@PathParam("index") int index, MessageDTO messageDTO) {
    try {
      messages.set(index, MessageConverter.toEntity(messageDTO));
      return Response.status(Status.OK).build();
    } catch (IndexOutOfBoundsException e) {
      return Response.status(Status.NOT_FOUND).entity(new ExceptionDTO(e)).build();
    }
  }

  @DELETE
  @Produces("application/xml")
  @Path("/{index}")
  public Response removeMessage(@PathParam("index") int index) {
    try {
      MessageDTO messageDTO = MessageConverter.toDTO(messages.get(index));
      messages.remove(index);
      return Response.status(Status.OK).entity(messageDTO).build();
    } catch (IndexOutOfBoundsException e) {
      return Response.status(Status.NOT_FOUND).entity(new ExceptionDTO(e)).build();
    }
  }
}