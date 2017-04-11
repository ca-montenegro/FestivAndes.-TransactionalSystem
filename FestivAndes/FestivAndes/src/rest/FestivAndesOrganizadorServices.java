package rest;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import tm.FestivAndesMaster;
import vos.Funcion;
import vos.Preferencia;

@Path("organizador")
public class FestivAndesOrganizadorServices {

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
		 * Método que expone servicio REST usando POST que modifica una funcion a realizada
		 * <b>URL: </b> http://"ip o nombre de host":8080/FestivAndes/rest/organizador/funcion/id/...
		 * @param preferencia - preferencia a agregar
		 * @return Json con la preferencia que agrego o Json con el error que se produjo
		 */
		@PUT
		@Path("/funcion/{id}")
		@Consumes(MediaType.APPLICATION_JSON)
		@Produces(MediaType.APPLICATION_JSON)
		public Response updateFuncion(Funcion fun, @javax.ws.rs.PathParam("id")Long id) {
			FestivAndesMaster tm = new FestivAndesMaster(getPath());
			try {
				fun = tm.marcarRealizadaFuncion(id);
			} catch (Exception e) {
				return Response.status(500).entity(doErrorMessage(e)).build();
			}
			return  Response.status(200).entity(fun).build();
		}
		

}
