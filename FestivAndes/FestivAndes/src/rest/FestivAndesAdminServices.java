package rest;


import java.util.ArrayList;

import javax.naming.NamingException;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.codehaus.jackson.map.ObjectMapper;

import jms.TwoPhaseCommit;
import tm.FestivAndesMaster;
import vos.VOAbonamiento;
import vos.BoletasCompradas;
import vos.Compania;
import vos.Espectaculo;
import vos.Funcion;
import vos.InformacionVentaLocalidad;
import vos.ListaBuenosClientes;
import vos.ListaFuncioneSitio;
import vos.ListaFuncionesCompania;
import vos.ListaInformacion;
import vos.ListaInformacionFuncion;

import vos.Localidad;
import vos.NotaDebito;
import vos.Rentabilidad;
import vos.ListaLocalidades;
import vos.ListaPopulares;
import vos.ListaRentabilidad;
import vos.ListaRespuestaAsistencia;
import vos.ListaUsuarios;
import vos.ReporteSitio;
import vos.Requerimiento;
import vos.Sitio;
import vos.Usuario;


/**
 * Clase que expone servicios REST con ruta base: http://"ip o nombre de host":8080/FestivAndes/rest/administrador/...
 * @author Camilo
 */
@Path("administrador")
public class FestivAndesAdminServices {


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
	
	private String doErrorMessage(String s){
		return "{ \"ERROR\": \""+ s + "\"}" ;
	}
	@GET
	@Path("sayHello")
	public String sayHello(@QueryParam("name")String name) {
		System.out.println("aasaaa");
		return "asa: "+name;

	}


	/**
	 * Método que expone servicio REST usando GET que da todos los videos de la base de datos.
	 * <b>URL: </b> http://"ip o nombre de host":8080/FestivAndes/rest/administrador/usuario
	 * @return Json con todos los videos de la base de datos O json con 
	 * el error que se produjo
	 */
	//	@GET
	//	@Produces({ MediaType.APPLICATION_JSON })
	//	public Response getUsuarios() {
	//		FestivAndesMaster tm = new FestivAndesMaster(getPath());
	//		ListaVideos videos;
	//		try {
	//			videos = tm.darVideos();
	//		} catch (Exception e) {
	//			return Response.status(500).entity(doErrorMessage(e)).build();
	//		}
	//		return Response.status(200).entity(videos).build();
	//	}


	/**
	 * Método que expone servicio REST usando GET que busca el video con el nombre que entra como parámetro
	 * <b>URL: </b> http://"ip o nombre de host":8080/VideoAndes/rest/administrador/id/"id para la busqueda"
	 * @param name - Nombre del video a buscar que entra en la URL como parámetro 
	 * @return Json con el/los videos encontrados con el nombre que entra como parámetro o json con 
	 * el error que se produjo
	 */
	@GET
	@Path("/id/{id}")
	@Produces({ MediaType.APPLICATION_JSON })
	public Response getUsuario(@javax.ws.rs.PathParam("id") Long id) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		Usuario usuario;
		try {
			if (id == null || id <= 0)
				throw new Exception("Nombre del video no valido");
			usuario = tm.buscarUsuarioPorID(id);
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).entity(usuario).build();
	}

	@GET
	@Path("/reporteFuncion/{id}")
	@Produces({ MediaType.APPLICATION_JSON })
	public Response darReporteFuncion(@javax.ws.rs.PathParam("id")Long idFuncion) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		String idFuncion1 = Long.toString(idFuncion);
		ListaInformacion lista;
		try {
			lista = tm.generarReporteDeUnaFuncion(idFuncion1);
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).entity(lista).build();
	}

	@GET
	@Path("/reporteCompania/{id}")
	@Produces({ MediaType.APPLICATION_JSON })
	public Response darReporteCompania(@javax.ws.rs.PathParam("id") Long idCompania)
	{
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		String idCompania1 = Long.toString(idCompania);
		ListaFuncionesCompania lista;
		try {
			lista = tm.generarReporteDeUnaCompania(idCompania1);
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).entity(lista).build();
	}

	@GET
	@Path("/reporteEspectaculo/{id}")
	@Produces({ MediaType.APPLICATION_JSON })
	public Response darReporteEspectaculo(@javax.ws.rs.PathParam("id")Long idEspectaculo) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		String idEspectaculo1 = Long.toString(idEspectaculo);
		ListaInformacionFuncion lista;
		try {
			lista = tm.generarReporteDeUnEspectaculo(idEspectaculo1);
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).entity(lista).build();
	}

	@GET
	@Path("/reporteSitio/{id}")
	@Produces({ MediaType.APPLICATION_JSON })
	public Response darReporteSitio(@javax.ws.rs.PathParam("id")Long idEspectaculo) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		String idSitio1 = Long.toString(idEspectaculo);

		ReporteSitio reporte;
		try {
			Sitio sitio = tm.darSitioConsulta(idSitio1);
			ListaLocalidades lista1 = tm.darLocalidadesSitio(idSitio1);
			ListaFuncioneSitio lista2 = tm.darFuncionesSitio(idSitio1);
			reporte = new ReporteSitio(sitio, lista1, lista2);

		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).entity(reporte).build();
	}

	/**
	 * Método que expone servicio REST usando GET que busca el video mas alquilado
	 * <b>URL: </b> http://"ip o nombre de host":8080/VideoAndes/rest/videos/MayorAlquilado
	 * @return Json con el/los videos encontrados con el nombre que entra como parámetro o json con 
	 * el error que se produjo
	 */
	//	@GET
	//	@Path("/MayorAlquilado")
	//	@Produces({ MediaType.APPLICATION_JSON })
	//	public Response getVideoMayorAlquilado() {
	//		VideoAndesMaster tm = new VideoAndesMaster(getPath());
	//		ListaVideos videos;
	//		try {
	//			videos = tm.videosMasAlquilados();
	//		} catch (Exception e) {
	//			return Response.status(500).entity(doErrorMessage(e)).build();
	//		}
	//		return Response.status(200).entity(videos).build();
	//	}


	/**
	 * Método que expone servicio REST usando PUT que agrega el usuario que recibe en Json
	 * <b>URL: </b> http://"ip o nombre de host":8080/VideoAndes/rest/administrado/usuario
	 * @param usuario - usuario a agregar
	 * @return Json con el usuario que agrego o Json con el error que se produjo
	 */
	@POST
	@Path("/usuario")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addUsuario(Usuario usuario) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		try {
			tm.addUsuario(usuario);
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).entity(usuario).build();
	}

	/**
	 * Método que expone servicio REST usando PUT que agrega el cliente que recibe en Json
	 * <b>URL: </b> http://"ip o nombre de host":8080/VideoAndes/rest/administrado/cliente
	 * @param cliente - cliente a agregar
	 * @return Json con el cliente que agrego o Json con el error que se produjo
	 */
	@POST
	@Path("/cliente")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addCliente(Usuario usuario) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		try {
			tm.addCliente(usuario);
			usuario.setRol((long) 2);
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).entity(usuario).build();
	}

	/**
	 * Método que expone servicio REST usando PUT que agrega la compa�ia que recibe en Json
	 * <b>URL: </b> http://"ip o nombre de host":8080/VideoAndes/rest/administrador/compania
	 * @param compania - compania a agregar
	 * @return Json con la compania que agrego o Json con el error que se produjo
	 */
	@POST
	@Path("/compania")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addCompania(Compania comp) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		try {
			tm.addCompania(comp);
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).entity(comp).build();
	}


	/**
	 * Método que expone servicio REST usando PUT que agrega el sitio que recibe en Json
	 * <b>URL: </b> http://"ip o nombre de host":8080/VideoAndes/rest/administrador/sitio
	 * @param sitio - sitio a agregar
	 * @return Json con el sitio que agrego o Json con el error que se produjo
	 */
	@POST
	@Path("/sitio")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addSitio(Sitio sitio) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		try {
			tm.addSitio(sitio);
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).entity(sitio).build();
	}





	/**
	 * Método que expone servicio REST usando PUT que agrega la funcion que recibe en Json
	 * <b>URL: </b> http://"ip o nombre de host":8080/VideoAndes/rest/administrador/funcion
	 * @param funcion - funcion a agregar
	 * @return Json con la funcion que agrego o Json con el error que se produjo
	 */
	@POST
	@Path("/funcion")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addFuncion(Funcion funcion) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		try {
			tm.addFuncion(funcion);
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).entity(funcion).build();
	}

	@POST
	@Path("/espectaculo")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addEspectaculo(Espectaculo espectaculo) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		try {
			tm.addEspectaculo(espectaculo);
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).entity(espectaculo).build();
	}


	@POST
	@Path("/rentabilidad")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response getRentabilidad(Rentabilidad rent) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		ListaRentabilidad rentabilidades;
		try {

			//rentabilidades = tm.darRentabilidad(rent);
			rentabilidades = new ListaRentabilidad(tm.darRentabilidad(rent));
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).entity(rentabilidades).build();
	}

	@POST
	@Path("/especPopulares")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response getEspecPopulares(Rentabilidad rent) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		ListaPopulares masPopuEspectaculo;
		try {

			//rentabilidades = tm.darRentabilidad(rent);
			masPopuEspectaculo = new ListaPopulares(tm.darMasPopuEspec(rent));
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).entity(masPopuEspectaculo).build();
	}

	@GET
	@Path("/asistenciaCliente/{id}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response reporteAsistenciaCliente(@PathParam("id") Long idCliente)
	{
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		String idCliente1 = Long.toString(idCliente);
		ListaRespuestaAsistencia lista;
		try {
			lista = tm.generarReporteAsistenciaCliente(idCliente1);
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).entity(lista).build();
	}

	@PUT
	@Path("/cancelarFuncion/{idFuncion}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response devolerAbonamiento(@PathParam("idFuncion") Long idFuncion, VOAbonamiento abonamiento)
	{
		ArrayList<NotaDebito> notasDebito = null;
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		try{
			notasDebito = (tm.devolverBoletasFunCancel(idFuncion, abonamiento.getFechaConsulta()));
			if(!tm.cancelarFuncion(idFuncion)){
				return Response.status(500).entity(doErrorMessage("Ocurrio un problema")).build();
			}
		} 
		catch(Exception e)
		{
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).type("text/plain").entity(notasDebito.toString()).build();
	}

	@GET
	@Path("/buenosClientes/{numBoletas}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response buenosClientes(@PathParam("numBoletas") Long numBoletas)
	{
		System.out.println(numBoletas);
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		ListaBuenosClientes lista;
		try {
			lista = tm.buenosClientes(numBoletas);
		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).entity(lista).build();
	}

	@PUT
	@Path("/consultaBoletas/fecha")
	@Produces({ MediaType.APPLICATION_JSON })
	public Response consultaBoletasFecha(VOAbonamiento fechas) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		ArrayList<BoletasCompradas> boletas = null;
		try {
		boletas = tm.consultaBoletasFecha(fechas.getFechaConsulta(), fechas.getFechaFinal());

		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).type("text/plain").entity(boletas.toString()).build();
	}
	
	@GET
	@Path("/consultaBoletas/requerimiento/{idReq}")
	@Produces({ MediaType.APPLICATION_JSON })
	public Response consultaBoletasRequerimiento(@PathParam("idReq") Long idReq) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		ArrayList<BoletasCompradas> boletas = null;
		try {
		boletas = tm.consultaBoletasRequerimiento(idReq);

		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).type("text/plain").entity(boletas.toString()).build();
	}
	
	@GET
	@Path("/consultaBoletas/localidad/{nomLocal}")
	@Produces({ MediaType.APPLICATION_JSON })
	public Response consultaBoletasLocalidad(@PathParam("nomLocal") String nomLocal) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		ArrayList<BoletasCompradas> boletas = null;
		try {
		boletas = tm.consultaBoletasLocalidad(nomLocal);

		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).type("text/plain").entity(boletas.toString()).build();
	}
	
	@GET
	@Path("/consultaBoletas/franjahoraria/{ini}/{fin}")
	@Produces({ MediaType.APPLICATION_JSON })
	public Response consultaBoletasFecha(@PathParam("ini") int ini,@PathParam("fin") int fin ) {
		FestivAndesMaster tm = new FestivAndesMaster(getPath());
		ArrayList<BoletasCompradas> boletas = null;
		try {
		boletas = tm.consultaBoletasFranjaHoraria(ini, fin);

		} catch (Exception e) {
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		return Response.status(200).type("text/plain").entity(boletas.toString()).build();
	}
	
	@GET
	@Path("/twoPrueba/{idCompania}")
	@Produces({ MediaType.APPLICATION_JSON })
	public Response twoPrueba(@PathParam("idCompania")Long idCompania)
	{
		TwoPhaseCommit two;
		try {
			two = new TwoPhaseCommit();
			two.RetirarCompania(idCompania);
		} catch (NamingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return Response.status(500).entity(doErrorMessage(e)).build();
		}
		
		
		return Response.status(200).build();
				
	}
}

