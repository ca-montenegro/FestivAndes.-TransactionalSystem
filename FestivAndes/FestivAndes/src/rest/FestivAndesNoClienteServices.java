package rest;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import tm.FestivAndesMaster;
import vos.VOBoleta;

/**
 * Clase que expone servicios REST con ruta base: http://"ip o nombre de host":8080/FestivAndes/rest/noCliente/...
 * @author Camilo
 */
@Path("noCliente")
public class FestivAndesNoClienteServices {

	



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
	@PUT
	@Path("/boleta")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addBoleta(VOBoleta boleta) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		VOBoleta bole = null;
		try {
			bole  = tm.venderBoleta(boleta.getIdFuncion(), boleta.getIdSilla(), null,(long)0);
			//System.out.println(" numero: " + bole.getSilla().getNumero() + 
			//	" localidad: " +bole.getLocalidad().getNombre() );
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return  Response.status(200).entity(bole).build();
	}
}

