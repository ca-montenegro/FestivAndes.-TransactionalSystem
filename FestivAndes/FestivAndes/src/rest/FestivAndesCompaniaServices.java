	package rest;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import tm.FestivAndesMaster;
import vos.ListaRentabilidad;
import vos.ListaRespuestaAsistencia;
import vos.ListaUsuarios;
import vos.Rentabilidad;

/**
 * Clase que expone servicios REST con ruta base: http://"ip o nombre de host":8080/FestivAndes/rest/administrador/...
 * @author Camilo
 */
@Path("compania/{id}")
public class FestivAndesCompaniaServices {


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
		@GET
		@Path("sayHello")
		public String sayHello(@QueryParam("name")String name) {
			System.out.println("aasaaa");
			return "asa: "+name;

		}
		
		@POST
		@Path("/rentabilidad")
		@Consumes(MediaType.APPLICATION_JSON)
		@Produces(MediaType.APPLICATION_JSON)
		public Response getRentabilidad(Rentabilidad rent, @javax.ws.rs.PathParam("id")Long idCompania ) {
			FestivAndesMaster tm = new FestivAndesMaster(getPath());
			ListaRentabilidad rentabilidades;
			try {
				
				//rentabilidades = tm.darRentabilidad(rent);
				rentabilidades = new ListaRentabilidad(tm.darRentabilidadCompania(rent, idCompania));
			} catch (Exception e) {
				return Response.status(500).entity(doErrorMessage(e)).build();
			}
			return Response.status(200).entity(rentabilidades).build();
		}
		
		@GET
		@Path("/siAsistieron")
		@Consumes(MediaType.APPLICATION_JSON)
		@Produces(MediaType.APPLICATION_JSON)
		public Response informeAsistencia(@PathParam("id") Long idCompania)
		{
			System.out.println(idCompania);
			FestivAndesMaster tm = new FestivAndesMaster(getPath());
			ListaUsuarios lista;
			try {
				lista = tm.siAsistieron(idCompania);
			} catch (Exception e) {
				return Response.status(500).entity(doErrorMessage(e)).build();
			}
			return Response.status(200).entity(lista).build();
		}
		
		@GET
		@Path("/noAsistieron")
		@Consumes(MediaType.APPLICATION_JSON)
		@Produces(MediaType.APPLICATION_JSON)
		public Response informeNoAsistencia(@PathParam("id") Long idCompania)
		{
			System.out.println(idCompania);
			FestivAndesMaster tm = new FestivAndesMaster(getPath());
			ListaUsuarios lista;
			try {
				lista = tm.noAsistieron(idCompania);
			} catch (Exception e) {
				return Response.status(500).entity(doErrorMessage(e)).build();
			}
			return Response.status(200).entity(lista).build();
		}

}
