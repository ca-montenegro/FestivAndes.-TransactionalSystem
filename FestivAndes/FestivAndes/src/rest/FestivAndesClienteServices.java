package rest;

import java.awt.List;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.sun.xml.internal.ws.client.sei.ResponseBuilder;

//import oracle.sql.DATE;
import tm.FestivAndesMaster;
import vos.Abonamiento;
import vos.Boleta;
import vos.NotaDebito;
import vos.Preferencia;
import vos.Usuario;

/**
 * Clase que expone servicios REST con ruta base: http://"ip o nombre de host":8080/FestivAndes/rest/cliente/...
 * @author Camilo
 */
@Path("cliente/{id}")
public class FestivAndesClienteServices {

	// Servicios REST tipo GET:


	/**
	 * Atributo que usa la anotación @Context para tener el ServletContext de la conexión actual.
	 */
	@Context
	private ServletContext context;

	/**
	 * Método que retorna el path de la carpeta WEB-INF/ConnectionData en el deploy actual dentro del servidor.
	 * @return path de la carpeta WEB-INF/ConnectionData en el deploy actual.
	 */	
	private String getPath() {
		return context.getRealPath("WEB-INF/ConnectionData");
	}


	private String doErrorMessage(Exception e){
		return "{ \"ERROR\": \""+ e.getMessage() + "\"}" ;
	}

	/**
	 * Método que expone servicio REST usando PUT que agrega una preferencia a el usuario que recibe en Json
	 * <b>URL: </b> http://"ip o nombre de host":8080/FestivAndes/rest/administrado/cliente/id/...
	 * @param preferencia - preferencia a agregar
	 * @return Json con la preferencia que agrego o Json con el error que se produjo
	 */
	@POST
	@Path("/preferencia")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addPreferencia(Preferencia prefe, @javax.ws.rs.PathParam("id")Long id) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		try {
			tm.addPreferencia(prefe, id);
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return  Response.status(200).entity(prefe).build();
	}

	@DELETE
	@Path("/preferencia/{idP}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response deletePreferencia(@javax.ws.rs.PathParam("id")Long id,@javax.ws.rs.PathParam("idP")Long idP) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		Preferencia prefe = null;
		try {
			prefe = tm.deletePreferencia(id, idP);
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return  Response.status(200).entity(prefe).build();
	}

	/**
	 * Método que expone servicio REST usando PUT que agrega una preferencia a el usuario que recibe en Json
	 * <b>URL: </b> http://"ip o nombre de host":8080/FestivAndes/rest/administrado/cliente/id/...
	 * @param preferencia - preferencia a agregar
	 * @return Json con la preferencia que agrego o Json con el error que se produjo
	 */
	@PUT
	@Path("/preferencia")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response updatePreferencia(Preferencia prefe, @javax.ws.rs.PathParam("id")Long id) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		try {
			tm.updatePreferencia(prefe, id);
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return  Response.status(200).entity(prefe).build();
	}

	/**
	 * Método que expone servicio REST usando PUT que agrega una preferencia a el usuario que recibe en Json
	 * <b>URL: </b> http://"ip o nombre de host":8080/FestivAndes/rest/administrado/cliente/id/...
	 * @param preferencia - preferencia a agregar
	 * @return Json con la preferencia que agrego o Json con el error que se produjo
	 */
	@POST
	@Path("/boleta")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addBoleta(Boleta boleta, @javax.ws.rs.PathParam("id")Long id) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		Boleta bole = null;
		try {
			bole  = tm.venderBoleta(boleta.getIdFuncion(), boleta.getIdSilla(), id,(long) 0);
			//System.out.println(" numero: " + bole.getSilla().getNumero() + 
			//	" localidad: " +bole.getLocalidad().getNombre() );
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return  Response.status(200).entity(bole).build();
	}

	/**
	 * Método que expone servicio REST usando PUT que agrega varias boletas a un cliente
	 * <b>URL: </b> http://"ip o nombre de host":8080/FestivAndes/rest/administrado/cliente/id/...
	 * @param preferencia - preferencia a agregar
	 * @return Json con la preferencia que agrego o Json con el error que se produjo
	 */
	@POST
	@Path("/boleta/multiples")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addMultiplesBoletas(Boleta boleta, @PathParam("id")Long id) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		Boleta bole = boleta;
		ArrayList<Boleta> vendidas = new ArrayList<>();
		try {
			tm.verificarLocalidadSillas(bole.getIdSilla(), bole.getCantidad());
			//while(bole.getCantidad()>0);
			try{
				vendidas  = tm.venderMultiplesBoleta(bole.getIdFuncion(), bole.getIdSilla(), id, bole.getCantidad());

			}
			catch(Exception e)
			{
				return Response.status(500).entity(doErrorMessage(e)).build();
			}

		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return  Response.status(200).entity(vendidas).build();
	}

	/**
	 * Método que expone servicio REST usando PUT que agrega varias boletas a un cliente
	 * <b>URL: </b> http://"ip o nombre de host":8080/FestivAndes/rest/administrado/cliente/id/...
	 * @param preferencia - preferencia a agregar
	 * @return Json con la preferencia que agrego o Json con el error que se produjo
	 */
	@PUT
	@Path("/boleta/{idBoleta}/devolucion")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response modifyBoletaDev(@PathParam("id")Long idUsuario, @PathParam("idBoleta")Long idBoleta) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		NotaDebito notaDebito = null;
		try {

			DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
			Date date = new Date();
			String fechaHoy = dateFormat.format(date);
			System.out.println(fechaHoy); //2016/11/16 12:08:43
			notaDebito = tm.inicActualizarDevBoleta(idUsuario, idBoleta, fechaHoy);
			
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
//		return  Response.status(200).entity(vendidas).build();
		return Response.status(200).type("text/plain").entity(notaDebito.toString()).build();
	}
	
	
	@POST
	@Path("/abonar")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response crearAbonamiento(@PathParam("id")Long idUsuario, Abonamiento abonamiento)
	{
		ArrayList<Boleta> listBoletas = null;
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		try{
		listBoletas = tm.crearAbonamiento(idUsuario, abonamiento);
		}
		catch(Exception e)
		{
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).type("text/plain").entity(listBoletas.toString()).build();
	}


	@PUT
	@Path("/abonar/{idAbonamiento}/devolucion")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response devolerAbonamiento(@PathParam("id")Long idUsuario, @PathParam("idAbonamiento") Long idAbonamiento, Abonamiento abonamiento)
	{
		ArrayList<NotaDebito> notasDebito = null;
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		try{
		notasDebito = (tm.devolverAbonamiento(idUsuario, idAbonamiento, abonamiento.getFechaConsulta()));
		}
		catch(Exception e)
		{
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).type("text/plain").entity(notasDebito.toString()).build();
	}



}
