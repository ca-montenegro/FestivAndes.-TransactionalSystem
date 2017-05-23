package dao;

import java.awt.List;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Queue;
import vos.VOAbonamiento;
import vos.VOBoleta;
import vos.BoletasCompradas;
import vos.Cliente;
import vos.Compania;
import vos.Espectaculo;
import vos.Funcion;
import vos.FuncionCompania;
import vos.FuncionRespuestaCliente;
import vos.InformacionFuncionSitio;
import vos.InformacionVentaFuncion;
import vos.InformacionVentaLocalidad;
import vos.Localidad;
import vos.MasPopuEspectaculo;
import vos.NotaDebito;
import vos.Preferencia;
import vos.Rentabilidad;
import vos.Silla;
import vos.Sitio;
import vos.Usuario;

public class DAOTablaFestival {


	/**
	 * Arraylits de recursos que se usan para la ejecución de sentencias SQL
	 */
	private ArrayList<Object> recursos;

	/**
	 * Atributo que genera la conexión a la base de datos
	 */
	private Connection conn;

	/**
	 * Método constructor que crea DAOVideo
	 * <b>post: </b> Crea la instancia del DAO e inicializa el Arraylist de recursos
	 */
	public DAOTablaFestival() {
		recursos = new ArrayList<Object>();
	}

	/**
	 * Método que cierra todos los recursos que estan enel arreglo de recursos
	 * <b>post: </b> Todos los recurso del arreglo de recursos han sido cerrados
	 */
	public void cerrarRecursos() {
		for(Object ob : recursos){
			if(ob instanceof PreparedStatement)
				try {
					((PreparedStatement) ob).close();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
		}
	}

	/**
	 * Método que inicializa la connection del DAO a la base de datos con la conexión que entra como parámetro.
	 * @param con  - connection a la base de datos
	 */
	public void setConn(Connection con){
		this.conn = con;
	}

	/**
	 * Requerimiento 3 de consulta
	 * @param idFuncion
	 * @return
	 * @throws SQLException
	 */
	public ArrayList<InformacionVentaLocalidad> generarReporteDeUnaFuncion(String idFuncion) throws SQLException
	{
		ArrayList<InformacionVentaLocalidad> lista = new ArrayList<InformacionVentaLocalidad>();
		String sql =     "SELECT ID_LOCALIDAD,"+
				"COUNT(*) AS CANTIDAD_LOCALIDAD,"+
				"SUM(CASE WHEN ID_CLIENTE IS NOT NULL THEN 1 ELSE 0 END) AS CON_CLIENTE,"+
				"SUM(CASE WHEN ID_CLIENTE IS NULL THEN 1 ELSE 0 END) AS SIN_CLIENTE,"+
				"SUM(CASE WHEN ID_CLIENTE IS NULL THEN PRECIO_LOCALIDAD ELSE 0 END) AS PRODUCIDO_SIN_CLIENTE,"+
				"SUM(CASE WHEN ID_CLIENTE IS NOT NULL THEN PRECIO_LOCALIDAD ELSE 0 END) AS PRODUCIDO_CON_CLIENTE, "+
				"SUM(PRECIO_LOCALIDAD) AS PRODUCIDO_TOTAL " +
				" FROM (SELECT * FROM(((SELECT * FROM ISIS2304A241720.BOLETA WHERE ID_FUNCION =" + idFuncion + ") NATURAL JOIN"+ 
				"(SELECT ID_SILLA AS ID_SILLA, ID_LOCALIDAD AS ID_LOCALIDAD FROM ISIS2304A241720.SILLAS "+
				")) NATURAL JOIN "+
				"(SELECT ID_LOCALIDAD AS ID_LOCALIDAD, NOMBRE AS NOMBRE_LOCALIDAD, CAPACIDAD AS CAPACIDAD_LOCALIDAD, PRECIO AS PRECIO_LOCALIDAD "+
				" FROM ISIS2304A241720.LOCALIDAD))) "+
				"GROUP BY ID_LOCALIDAD "+
				"ORDER BY ID_LOCALIDAD ASC";
		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		while (rs.next()) {
			Long idLocalidad = Long.parseLong(rs.getString("ID_LOCALIDAD"));
			int cantidadTotalDeBoletas = Integer.parseInt(rs.getString("CANTIDAD_LOCALIDAD"));
			int numeroBoletasConCliente = Integer.parseInt(rs.getString("CON_CLIENTE"));
			int numeroBoletasSinCliente = Integer.parseInt(rs.getString("SIN_CLIENTE"));
			int producidoPorBoletasSinCliente = Integer.parseInt(rs.getString("PRODUCIDO_SIN_CLIENTE"));
			int producidoPorBoletasConCliente = Integer.parseInt(rs.getString("PRODUCIDO_CON_CLIENTE"));
			int producidoTotal = Integer.parseInt(rs.getString("PRODUCIDO_TOTAL"));
			lista.add(new InformacionVentaLocalidad(idLocalidad, cantidadTotalDeBoletas, numeroBoletasConCliente,
					numeroBoletasSinCliente, producidoPorBoletasConCliente, producidoPorBoletasSinCliente, producidoTotal));
		}

		return lista;
	}
	
	public ArrayList<ArrayList<FuncionRespuestaCliente>> generarReporteAsistenciaCliente(String idCliente) throws SQLException
	{
		DAOTablaUsuario daoUsuario = new DAOTablaUsuario();
		daoUsuario.setConn(conn);
		Long idclientex = Long.parseLong(idCliente);
		if(daoUsuario.darCliente(idclientex) == null)
		{
			throw new SQLException("El cliente con el id " + idCliente + " no esta registrado en el sistema");
		}
		ArrayList<ArrayList<FuncionRespuestaCliente>> resp = new ArrayList<ArrayList<FuncionRespuestaCliente>>();
		ArrayList<FuncionRespuestaCliente> activasRealizadas = new ArrayList<FuncionRespuestaCliente>();
		ArrayList<FuncionRespuestaCliente> devueltasRealizadas = new ArrayList<FuncionRespuestaCliente>();
		ArrayList<FuncionRespuestaCliente> activasNoRealizadas = new ArrayList<FuncionRespuestaCliente>();
		ArrayList<FuncionRespuestaCliente> devueltasNoRealizadas = new ArrayList<FuncionRespuestaCliente>();
		String sql = "WITH TABLA1 AS (SELECT ID_FUNCION, ESTADO, ID_CLIENTE, COUNT(*) AS CANT_FUNCION " +
				"FROM ISIS2304A241720.BOLETA WHERE ID_CLIENTE = " +  idCliente + " GROUP BY ID_FUNCION, ID_CLIENTE, ESTADO) "+
				"SELECT ID_FUNCION, ESTADO, ID_CLIENTE, REALIZADA, FECHA, ID_SITIO, ID_ESPECTACULO "+
				"FROM TABLA1 NATURAL JOIN(SELECT ID_FUNCION AS ID_FUNCION, REALIZADA AS REALIZADA, "+
				"FECHA AS FECHA, ID_SITIO AS ID_SITIO, ID_ESPECTACULO AS ID_ESPECTACULO "+
				"FROM ISIS2304A241720.FUNCION) ";
		System.out.println(sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			Long idFuncion = Long.parseLong(rs.getString("ID_FUNCION"));
			System.out.println(idFuncion);
			char[] cha = rs.getString("ESTADO").toCharArray();
			char estado = cha[0];
			System.out.println(estado);
			char[] cha2 = rs.getString("REALIZADA").toCharArray();
			char realizada = cha2[0];
			System.out.println(realizada);
			Long idSitio = Long.parseLong(rs.getString("ID_SITIO"));
			System.out.println(idSitio);
			Long idEspectaculo = Long.parseLong(rs.getString("ID_ESPECTACULO"));
			System.out.println(idEspectaculo);
			String fecha = rs.getString("FECHA");
			System.out.println(fecha);
			
			FuncionRespuestaCliente frc = new FuncionRespuestaCliente(idFuncion, fecha, idSitio, idEspectaculo);
			
			System.out.println(frc == null);
			if(estado == 'A' && realizada == 'N')
				activasNoRealizadas.add(frc);
				
			else if(estado == 'A' && realizada == 'S')
				activasRealizadas.add(frc);
			else if(estado == 'D' && realizada == 'N')
				devueltasNoRealizadas.add(frc);
			else if(estado == 'D' && realizada == 'S')
				devueltasRealizadas.add(frc);
		}
		
		if(activasRealizadas.isEmpty() && activasNoRealizadas.isEmpty() 
				&& devueltasNoRealizadas.isEmpty() && devueltasRealizadas.isEmpty())
		{
			throw new SQLException("El cliente no asiste aun a ninguna funcion");
		}
		
		resp.add(activasRealizadas);
		resp.add(activasNoRealizadas);
		resp.add(devueltasRealizadas);
		resp.add(devueltasNoRealizadas);
		daoUsuario.cerrarRecursos();
		return resp;
	}

	public ArrayList<InformacionVentaFuncion> generarReporteDeUnEspectaculo(String idEspectaculo) throws SQLException
	{
		ArrayList<InformacionVentaFuncion> lista = new ArrayList<InformacionVentaFuncion>();
		String sql = "SELECT ID_FUNCION, ID_SITIO, CANT_FUNCION, CON_CLIENTE, SIN_CLIENTE,"+
				"PRODUCIDO_CON_CLIENTE, PRODUCIDO_SIN_CLIENTE,  PRODUCIDO_TOTAL, "+
				"(CANT_FUNCION/CAPACIDAD_SITIO)*100 AS PORCENTAJE "+
				"FROM( "+
				"SELECT ID_FUNCION, COUNT(*) AS CANT_FUNCION, "+
				"SUM(CASE WHEN ID_CLIENTE IS NOT NULL THEN 1 ELSE 0 END) AS CON_CLIENTE, "+
				"SUM(CASE WHEN ID_CLIENTE IS NULL THEN 1 ELSE 0 END) AS SIN_CLIENTE, "+
				"SUM(CASE WHEN ID_CLIENTE IS NULL THEN PRECIO_LOCALIDAD ELSE 0 END) AS PRODUCIDO_SIN_CLIENTE, "+
				"SUM(CASE WHEN ID_CLIENTE IS NOT NULL THEN PRECIO_LOCALIDAD ELSE 0 END) AS PRODUCIDO_CON_CLIENTE, "+
				"SUM(PRECIO_LOCALIDAD) AS PRODUCIDO_TOTAL, "+
				"CAPACIDAD_SITIO, "+
				"ID_SITIO "+
				"FROM((( "+
				"((SELECT ID_FUNCION, ID_SITIO, ID_ESPECTACULO FROM ISIS2304A241720.FUNCION WHERE ID_ESPECTACULO = " + idEspectaculo + ") "+
				"NATURAL JOIN (SELECT ID_FUNCION AS ID_FUNCION, ID_CLIENTE AS ID_CLIENTE, ID_SILLA AS ID_SILLA, "+
				"ID_BOLETA AS ID_BOLETA FROM ISIS2304A241720.BOLETA)) "+
				"NATURAL JOIN(SELECT ID_SITIO AS ID_SITIO, CAPACIDAD AS CAPACIDAD_SITIO FROM ISIS2304A241720.SITIO)) "+
				"NATURAL JOIN (SELECT ID_SILLA AS ID_SILLA, ID_LOCALIDAD AS ID_LOCALIDAD FROM ISIS2304A241720.SILLAS)) "+
				"NATURAL JOIN (SELECT PRECIO AS PRECIO_LOCALIDAD, ID_LOCALIDAD AS ID_LOCALIDAD FROM ISIS2304A241720.LOCALIDAD)) "+
				"GROUP BY ID_FUNCION, CAPACIDAD_SITIO, ID_SITIO)";
		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		while (rs.next()) {
			Long idFuncion = Long.parseLong(rs.getString("ID_FUNCION"));
			Long idSitio = Long.parseLong(rs.getString("ID_SITIO"));
			int cantidadTotalDeBoletas = Integer.parseInt(rs.getString("CANT_FUNCION"));
			int numeroBoletasConCliente = Integer.parseInt(rs.getString("CON_CLIENTE"));
			int numeroBoletasSinCliente = Integer.parseInt(rs.getString("SIN_CLIENTE"));
			int producidoPorBoletasConCliente = Integer.parseInt(rs.getString("PRODUCIDO_CON_CLIENTE"));
			int producidoPorBoletasSinCliente = Integer.parseInt(rs.getString("PRODUCIDO_SIN_CLIENTE"));
			int producidoTotal = Integer.parseInt(rs.getString("PRODUCIDO_TOTAL"));
			double porcentajeOcupacionSitio = Double.parseDouble(rs.getString("PORCENTAJE"));
			lista.add(new InformacionVentaFuncion(idFuncion, idSitio, cantidadTotalDeBoletas, numeroBoletasConCliente, numeroBoletasSinCliente, producidoPorBoletasConCliente, producidoPorBoletasSinCliente, producidoTotal, porcentajeOcupacionSitio));
		}

		return lista;
	}

	public ArrayList darReporteCompania(String idCompania) throws SQLException
	{
		DAOTablaCompania daoCompania = new DAOTablaCompania();
		daoCompania.setConn(conn);
		ArrayList<FuncionCompania> funciones = new ArrayList<>();	
		
		if(daoCompania.buscarCompania(Long.parseLong(idCompania)) == null)
			throw new SQLException("La compania con id " + idCompania + " no existe");
		
		String sql = "WITH TABLA1 AS (SELECT ID_ESPEC, NOMBRE, ID_COMPANIA FROM "+
				"ISIS2304A241720.ESPECTACULO WHERE ID_COMPANIA = "+ idCompania +"), "+ 
				"TABLA2 AS( "+
				"SELECT ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO "+
				"FROM TABLA1 NATURAL JOIN (SELECT ID_FUNCION, ID_ESPECTACULO AS ID_ESPEC, ID_SITIO AS ID_SITIO FROM ISIS2304A241720.FUNCION)), "+
				"TABLA3 AS( "+
				"SELECT ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO, ID_SILLA "+
				"FROM TABLA2 NATURAL JOIN "+
				"(SELECT ID_FUNCION AS ID_FUNCION, ID_SILLA FROM ISIS2304A241720.BOLETA)), "+
				"TABLA4 AS(SELECT ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO, ID_LOCALIDAD  "+
				"FROM TABLA3 NATURAL JOIN (SELECT ID_SILLA AS ID_SILLA, ID_LOCALIDAD AS ID_LOCALIDAD FROM ISIS2304A241720.SILLAS)), "+
				"TABLA5 AS(SELECT ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO, COUNT(*) AS CANT_BOLETAS, PRECIO "+
				"FROM TABLA4 NATURAL JOIN(SELECT ID_LOCALIDAD AS ID_LOCALIDAD, PRECIO "+
				"FROM ISIS2304A241720.LOCALIDAD) GROUP BY ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO, PRECIO), "+
				"TABLA6 AS(SELECT ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO, CANT_BOLETAS, SUM(CANT_BOLETAS*PRECIO) AS RECOGIDO "+
				"FROM TABLA5 GROUP BY ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO, CANT_BOLETAS), "+
				"TABLA7 AS(SELECT ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO, SUM(CANT_BOLETAS) AS CANT_BOL_TOTAL, SUM(RECOGIDO) AS PRODUCIDO "+
				"FROM TABLA6 GROUP BY  ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO) "+
				"SELECT ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO, CANT_BOL_TOTAL, PRODUCIDO, (CANT_BOL_TOTAL/CAPACIDAD)*100 AS PORCENTAJE "+
				"FROM TABLA7 NATURAL JOIN(SELECT ID_SITIO AS ID_SITIO, CAPACIDAD FROM ISIS2304A241720.SITIO)";
		System.out.println("SQL stmt: " + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			Long idFuncion = Long.parseLong(rs.getString("ID_FUNCION"));
			Long idEspectaculo = Long.parseLong(rs.getString("ID_ESPEC"));
			String nombreEspectaculo = rs.getString("NOMBRE");
			Long idSitio = Long.parseLong(rs.getString("ID_SITIO"));
			int cantBoletas = Integer.parseInt(rs.getString("CANT_BOL_TOTAL"));
			int producido = Integer.parseInt(rs.getString("PRODUCIDO"));
			double porcentaje = Double.parseDouble(rs.getString("PORCENTAJE"));
			
			FuncionCompania fc = new FuncionCompania(idFuncion, idSitio, producido, porcentaje, cantBoletas);
			
			funciones.add(fc);
		}
		daoCompania.cerrarRecursos();
		return funciones;
	}
	
	
	public ArrayList darReporteCompaniaParaCliente(String idCompania, String idCliente) throws SQLException
	{
		DAOTablaCompania daoCompania = new DAOTablaCompania();
		daoCompania.setConn(conn);
		ArrayList<FuncionCompania> funciones = new ArrayList<>();
		
		if(daoCompania.buscarCompania(Long.parseLong(idCompania)) == null)
			throw new SQLException("La compania con id " + idCompania + " no existe");
		
		String sql = "WITH TABLA0 AS (SELECT ID_FUNCION FROM ISIS2304A241720.BOLETA WHERE ID_CLIENTE = "+idCliente+"), "+
						"TABLA1 AS (SELECT ID_ESPEC, NOMBRE, ID_COMPANIA FROM "+
						"ISIS2304A241720.ESPECTACULO WHERE ID_COMPANIA = "+idCompania + "), "+
						"TABLA2 AS(SELECT ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO "+
						"FROM TABLA1 NATURAL JOIN (SELECT ID_FUNCION, ID_ESPECTACULO AS ID_ESPEC, ID_SITIO AS ID_SITIO FROM ISIS2304A241720.FUNCION)), "+
						"TABLA3 AS(SELECT DISTINCT ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO "+ 
						"FROM TABLA2 NATURAL JOIN (SELECT ID_FUNCION AS ID_FUNCION FROM TABLA0)), "+
						"TABLA4 AS( "+
						"SELECT ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO, ID_SILLA "+
						"FROM TABLA3 NATURAL JOIN "+
						"(SELECT ID_FUNCION AS ID_FUNCION, ID_SILLA FROM ISIS2304A241720.BOLETA)), "+
						"TABLA5 AS(SELECT ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO, ID_LOCALIDAD "+ 
						"FROM TABLA4 NATURAL JOIN (SELECT ID_SILLA AS ID_SILLA, ID_LOCALIDAD AS ID_LOCALIDAD FROM ISIS2304A241720.SILLAS)), "+
						"TABLA6 AS(SELECT ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO, COUNT(*) AS CANT_BOLETAS, PRECIO "+
						"FROM TABLA5 NATURAL JOIN(SELECT ID_LOCALIDAD AS ID_LOCALIDAD, PRECIO  "+
						"FROM ISIS2304A241720.LOCALIDAD) GROUP BY ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO, PRECIO), "+
						"TABLA7 AS(SELECT ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO, CANT_BOLETAS, SUM(CANT_BOLETAS*PRECIO) AS RECOGIDO "+
						"FROM TABLA6 GROUP BY ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO, CANT_BOLETAS), "+
						"TABLA8 AS(SELECT ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO, SUM(CANT_BOLETAS) AS CANT_BOL_TOTAL, SUM(RECOGIDO) AS PRODUCIDO "+
						"FROM TABLA7 GROUP BY  ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO) "+
						"SELECT ID_FUNCION, ID_ESPEC, NOMBRE, ID_COMPANIA, ID_SITIO, CANT_BOL_TOTAL, PRODUCIDO, (CANT_BOL_TOTAL/CAPACIDAD)*100 AS PORCENTAJE "+
						"FROM TABLA8 NATURAL JOIN(SELECT ID_SITIO AS ID_SITIO, CAPACIDAD FROM ISIS2304A241720.SITIO) ";
		System.out.println("SQL stmt: " + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			Long idFuncion = Long.parseLong(rs.getString("ID_FUNCION"));
			Long idEspectaculo = Long.parseLong(rs.getString("ID_ESPEC"));
			String nombreEspectaculo = rs.getString("NOMBRE");
			Long idSitio = Long.parseLong(rs.getString("ID_SITIO"));
			int cantBoletas = Integer.parseInt(rs.getString("CANT_BOL_TOTAL"));
			int producido = Integer.parseInt(rs.getString("PRODUCIDO"));
			double porcentaje = Double.parseDouble(rs.getString("PORCENTAJE"));
			
			FuncionCompania fc = new FuncionCompania(idFuncion, idSitio, producido, porcentaje, cantBoletas);
			
			funciones.add(fc);
		}
		daoCompania.cerrarRecursos();
		return funciones;
	}
	
	public Sitio darSitioConsulta (String idSitio) throws SQLException
	{
		String sql = "SELECT * FROM ISIS2304A241720.SITIO WHERE ID_SITIO =" + idSitio;
		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			Long id = Long.parseLong(rs.getString("ID_SITIO"));
			char[] cha = rs.getString("TIPO").toCharArray();
			char tipo = cha[0];
			int capacidad = Integer.parseInt(rs.getString("CAPACIDAD"));
			String disponibilidad = rs.getString("HORARIO_DISPONIBILIDAD");
			char[] cha2 = rs.getString("PROTECCION_ATMOSFERICA").toCharArray();
			char proteAtmosfera = cha2[0];
			Sitio aux = new Sitio(id, tipo, capacidad, disponibilidad, proteAtmosfera);
			return aux;
		}
		return null;
	}

	public ArrayList<Localidad> darLocalidadesSitio (String idSitio) throws SQLException
	{
		ArrayList<Localidad> lista  = new ArrayList<Localidad>();
		String sql = "SELECT * FROM ISIS2304A241720.LOCALIDAD WHERE ID_SITIO =" + idSitio;
		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			Long id = Long.parseLong(rs.getString("ID_LOCALIDAD"));
			String nombre = rs.getString("NOMBRE");
			int capacidad = Integer.parseInt(rs.getString("CAPACIDAD"));
			boolean sillaNumerada = false;
			char sill = (rs.getString("SILLA_NUMERADA")).charAt(0);
			if(rs.getString("SILLA_NUMERADA").equals("S"))
				sillaNumerada = true;
			int precio = Integer.parseInt(rs.getString("PRECIO"));
			lista.add(new Localidad(id, Long.parseLong(idSitio), nombre,capacidad, sill, precio));
		}
		return lista;
	}

	public ArrayList<InformacionFuncionSitio> darFuncionesSitio(String idSitio) throws SQLException
	{
		ArrayList<InformacionFuncionSitio> lista  = new ArrayList<InformacionFuncionSitio>();
		String sql = "SELECT ID_FUNCION, FECHA , HORA, NOMBRE_ESPECTACULO, (CAPACIDAD - CANT_BOLETAS) AS BOLETAS_DISPONIBLES FROM("+
				"SELECT* FROM(SELECT * FROM(SELECT ID_FUNCION, FECHA, ID_SITIO, ID_ESPECTACULO, HORA, COUNT(*) AS CANT_BOLETAS FROM(SELECT* FROM((SELECT * FROM ISIS2304A241720.FUNCION WHERE ID_SITIO = 1)"+
				"NATURAL JOIN (SELECT ID_BOLETA AS ID_BOLETA, ID_FUNCION AS ID_FUNCION FROM ISIS2304A241720.BOLETA)))"+
				"GROUP BY ID_FUNCION, FECHA, ID_SITIO, ID_ESPECTACULO, HORA)NATURAL JOIN(SELECT ID_SITIO AS ID_SITIO, CAPACIDAD AS CAPACIDAD FROM ISIS2304A241720.SITIO)"+
				"NATURAL JOIN (SELECT ID_ESPEC AS ID_ESPECTACULO, NOMBRE AS NOMBRE_ESPECTACULO FROM ISIS2304A241720.ESPECTACULO))) ORDER BY ID_FUNCION";

		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			Long id = Long.parseLong(rs.getString("ID_FUNCION"));
			String fecha = rs.getString("FECHA");
			int hora = Integer.parseInt(rs.getString("HORA"));
			String nombreEspectaculo = rs.getString("NOMBRE_ESPECTACULO");
			int boletasDisponibles = Integer.parseInt(rs.getString("BOLETAS_DISPONIBLES"));
			lista.add(new InformacionFuncionSitio(id, fecha, hora, nombreEspectaculo, boletasDisponibles));
		}
		return lista;
	}

	public ArrayList<Rentabilidad> darRentabilidad(Rentabilidad rent) throws SQLException
	{
		ArrayList<Rentabilidad> listRent = new ArrayList<>();
		String sql = "with tabla1 as(select totalClientes, totalBoletas, fecha, id_sitio, tipo, totalClientes/capacidad as proporcion, id_espectaculo, sum(precio)as totalBoleta from "+ 
				"(select count(id_cliente)as totalClientes, count(id_cliente)as totalBoletas,id_funcion, fecha, id_espectaculo from funcion natural join boleta where fecha between" + "'" + rent.getFechaInicial()+"'"+" and " + "'" + rent.getFechaFinal() + "'"
				+" group by id_funcion, fecha, id_espectaculo) " +  
				"natural join (select precio, sitio.capacidad, tipo, sitio.id_sitio from sitio inner join localidad on sitio.ID_SITIO = localidad.ID_SITIO) group by totalClientes, totalBoletas, fecha, id_sitio, tipo, " +
				" totalClientes/capacidad, id_espectaculo),"
				+" tabla2 as( "
				+" select id_espectaculo, id_cat from(select * from categoria inner join ESPECTACULOCATEGORIA on id_cat = id_categoria)"
				+" inner join espectaculo on id_espec = id_espectaculo)"
				+" select * from tabla1 natural join tabla2 order by totalBoleta desc";

		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			String fechaInicial = rent.getFechaInicial();
			String fechaFinal = rent.getFechaFinal();
			Long idEspectaculo = Long.parseLong(rs.getString("id_espectaculo"));
			int totalClientes = Integer.parseInt(rs.getString("TOTALCLIENTES"));
			int totalBoletas = Integer.parseInt(rs.getString("TOTALBOLETAS"));
			Long idSitio = Long.parseLong(rs.getString("id_sitio"));
			char tipo = (rs.getString("TIPO")).charAt(0);
			double proporcion = Double.parseDouble(rs.getString("PROPORCION"));
			int precio = Integer.parseInt(rs.getString("TOTALBOLETA"));
			Long idCat = Long.parseLong(rs.getString("ID_CAT"));
			String fecha = rs.getString("FECHA");
			listRent.add(new Rentabilidad(fechaInicial, fechaFinal, idEspectaculo, totalClientes,
					totalBoletas, idSitio, tipo, proporcion, precio,idCat,fecha));
		}

		return listRent;
	}

	public ArrayList<Rentabilidad> darRentabilidadCompania(Rentabilidad rent, Long idCompania) throws SQLException
	{
		ArrayList<Rentabilidad> listRent = new ArrayList<>();
		String sql = "with tabla1 as(select totalClientes, totalBoletas, fecha, id_sitio, tipo, totalClientes/capacidad as proporcion, id_espectaculo, sum(precio)as totalBoleta from "+ 
				"(select count(id_cliente)as totalClientes, count(id_cliente)as totalBoletas,id_funcion, fecha, id_espectaculo from funcion natural join boleta where fecha between" + "'" + rent.getFechaInicial()+"'"+" and " + "'" + rent.getFechaFinal() + "'"
				+" group by id_funcion, fecha, id_espectaculo) " +  
				"natural join (select precio, sitio.capacidad, tipo, sitio.id_sitio from sitio inner join localidad on sitio.ID_SITIO = localidad.ID_SITIO) group by totalClientes, totalBoletas, fecha, id_sitio, tipo, " +
				" totalClientes/capacidad, id_espectaculo),"
				+" tabla2 as( "
				+" select id_espectaculo, id_cat from(select * from categoria inner join ESPECTACULOCATEGORIA on id_cat = id_categoria)"
				+" inner join espectaculo on id_espec = id_espectaculo where id_espectaculo = "+idCompania+")"
				+" select * from tabla1 natural join tabla2 order by totalBoleta desc";

		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			String fechaInicial = rent.getFechaInicial();
			String fechaFinal = rent.getFechaFinal();
			Long idEspectaculo = Long.parseLong(rs.getString("id_espectaculo"));
			int totalClientes = Integer.parseInt(rs.getString("TOTALCLIENTES"));
			int totalBoletas = Integer.parseInt(rs.getString("TOTALBOLETAS"));
			Long idSitio = Long.parseLong(rs.getString("id_sitio"));
			char tipo = (rs.getString("TIPO")).charAt(0);
			double proporcion = Double.parseDouble(rs.getString("PROPORCION"));
			int precio = Integer.parseInt(rs.getString("TOTALBOLETA"));
			Long idCat = Long.parseLong(rs.getString("ID_CAT"));
			String fecha = rs.getString("FECHA");
			listRent.add(new Rentabilidad(fechaInicial, fechaFinal, idEspectaculo, totalClientes,
					totalBoletas, idSitio, tipo, proporcion, precio,idCat,fecha));
		}

		return listRent;
	}

	public ArrayList<MasPopuEspectaculo> darMasPopuEspec(Rentabilidad rent) throws SQLException
	{
		ArrayList<MasPopuEspectaculo> listaMasPopu = new  ArrayList<>();
		String sql = "with tabla1 as (select count(id_Cliente) as asistentes, id_funcion, funcion.ID_ESPECTACULO as id_espec, fecha from boleta "
				+"natural join" 
				+" funcion where fecha between "+"'"+rent.getFechaInicial()+"'"+ "and" + "'" +rent.getFechaFinal() + "'" + 
				" group by id_funcion, funcion.ID_ESPECTACULO, fecha)"
				+" select sum(asistentes) as asistentes_funciones, id_espec, nombre, duracion, idioma, costo, descripcion, servicio_tradu, participacion,"
				+" id_compania, id_operario from tabla1 natural join espectaculo group by id_espec, nombre, duracion, idioma, costo, "
				+" descripcion, servicio_tradu, participacion,id_compania,id_operario";

		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			Long idEspec = Long.parseLong(rs.getString("id_espec"));
			String nombre = rs.getString("nombre");
			int duracion = Integer.parseInt(rs.getString("DURACION"));
			String idioma = rs.getString("IDIOMA");
			int costo = Integer.parseInt(rs.getString("COSTO"));
			String descripcion = rs.getString("DESCRIPCION");

			char servicioTraduccion = (rs.getString("servicio_tradu")).charAt(0);
			char participacion = (rs.getString("participacion")).charAt(0);
			Long idCompania = Long.parseLong(rs.getString("id_compania"));
			Long idOperario = Long.parseLong(rs.getString("id_operario"));
			int totalAsistentes = Integer.parseInt(rs.getString("asistentes_funciones"));
			listaMasPopu.add(new MasPopuEspectaculo(idEspec, nombre, duracion, idioma, costo, descripcion, servicioTraduccion, participacion, idCompania, idOperario, totalAsistentes));
		}
		return listaMasPopu;
	}

	public NotaDebito actualizarDevBoleta(Long idBoleta, Long idUsuario, String fecha) throws SQLException, ParseException
	{
		DAOTablaBoleta daoBoleta = new DAOTablaBoleta();
		daoBoleta.setConn(conn);
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		VOBoleta boleta = daoBoleta.buscarBoleta(idBoleta);
		if(boleta==null)
			throw new SQLException("La boleta con el id: " + idBoleta+ "no se encuentra en el sistema");
		if(!boleta.getIdCliente().equals(idUsuario))
			throw new SQLException("La boleta con el id: " + idBoleta + " no pertenece al cliente.");
		java.util.Date dateFuncion = formatter.parse(boleta.getFuncion().getFecha());
		java.util.Date dateDev = formatter.parse(fecha);

		Long diff = Math.round((dateFuncion.getTime() - dateDev.getTime()) / (double) 86400000);

		if(diff<5)
			throw new SQLException("Para generar la devolución de la boleta: "+idBoleta + " debe ser con 5 días previos al evento");
		String sql = "update boleta set estado = 'D' where id_boleta = "+idBoleta;
		System.out.println("SQL stmt: "+ sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		prepStmt.executeUpdate();
		String sql1 = "insert into devolucion values((devolucion_seq2.nextval), "  + idBoleta + "," + idUsuario +")";
		String key[] = {"ID_DEVOLUCION"};
		PreparedStatement prepStmt2 = conn.prepareStatement(sql1,key);
		recursos.add(prepStmt2);
		prepStmt2.executeUpdate();
		System.out.println("SQL stmt:" + sql1);
		ResultSet rsInsert = prepStmt2.getGeneratedKeys();
		Long idDevolucion = null;
		if (rsInsert.next()) {
			idDevolucion = rsInsert.getLong(1);
			System.out.println(idDevolucion);
		}
		return new NotaDebito(idDevolucion, idBoleta, dateDev, dateFuncion, idUsuario, boleta.getIdFuncion(), boleta.getFuncion());

	}

	public Long verificarFecha(String fecha) throws SQLException, ParseException
	{
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		Date dateCons = formatter.parse(fecha);
		String sql = "Select * from festival";
		System.out.println("SQL stmt: "+ sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		ResultSet rs = prepStmt.executeQuery();
		String fechaFestival = "";
		if(rs.next())
			fechaFestival = rs.getString("FECHA_INICIAL_FESTIVAL");
		String sql2 = "select abonamiento_seq2.nextval from dual";
		System.out.println("SQL stmt: "+ sql2);
		PreparedStatement prepStmt2 = conn.prepareStatement(sql2);
		ResultSet rs2 = prepStmt2.executeQuery();
		Long idAbonamiento = (long)0;
		if(rs2.next())
			idAbonamiento = Long.parseLong(rs2.getString("NEXTVAL"));
		Date dateFest = formatter.parse(fechaFestival);
		Long diff = Math.round((dateFest.getTime()-dateCons.getTime())/(double) 604800000);
		if(diff>3)
			return idAbonamiento;
		return (long)0;
	}

	public ArrayList<Long> verificarSitioLocalidad(VOAbonamiento abonamiento) throws SQLException
	{
		ArrayList<Long> funciones =abonamiento.getIdsFunciones();
		ArrayList<String> localidades = abonamiento.getIdsLocalidades();
		ArrayList<Long> listIdSillas = new ArrayList<>();

		for(int i = 0; i<funciones.size() && i<localidades.size(); i++)
		{
			Long idFuncion = funciones.get(i);
			String localidadNombre = localidades.get(i);
			String sql = "select * from funcion natural join "
					+ "(select l1.id_localidad, l1.nombre, l1.capacidad, l1.id_sitio, l1.precio "
					+ "from localidad l1 left join localidad l2 "
					+ "on l1.nombre = l2.nombre group by l1.id_localidad, l1.nombre, l1.capacidad, l1.id_sitio, l1.precio) "
					+ "where id_funcion = " + idFuncion + " AND nombre =  '" + localidadNombre + "'";
			System.out.println("SQL stmt: "+ sql);
			PreparedStatement prepStmt = conn.prepareStatement(sql);
			ResultSet rs = prepStmt.executeQuery();
			if(!rs.next())
				throw new SQLException("La localidad número: "+ (i+1) +" no existe para la función "+(i+1)+ " seleccionada.");
			Long idLocalidad = Long.parseLong(rs.getString("ID_LOCALIDAD"));
			String sql2 = "with tabla1 as(select * from sillas where id_localidad = "+ idLocalidad + "), "
					+"tabla2 as( select Sillas.id_silla as id_silla, id_funcion, numero, id_localidad,id_abonamiento, "
					+ "id_boleta from sillas inner join boleta on sillas.ID_SILLA = boleta.ID_SILLA where id_localidad = " + idLocalidad
					+ " and id_funcion = " + idFuncion + ")"
					+"select tabla1.id_silla, tabla1.numero, tabla1.id_localidad from tabla1 left join tabla2 on tabla1.id_Silla = tabla2.id_Silla where id_boleta is null order by numero";
			System.out.println("SQL stmt: "+ sql2);
			PreparedStatement prepStmt2 = conn.prepareStatement(sql2);
			ResultSet rs2 = prepStmt2.executeQuery();
			if(!rs2.next())
				throw new SQLException("No hay asientos disponibles para la localidad " + idLocalidad+ "y funcion: "+ idFuncion);
			Long idSilla = Long.parseLong(rs2.getString("ID_SILLA"));
			listIdSillas.add(idSilla);
		}
		return listIdSillas;

	}
	
	public ArrayList<Long> obtenerIdBoletaAbonados(Long idAbonamiento) throws SQLException
	{
		ArrayList<Long> idsBoletas = new ArrayList<>();
		String sql = "select * from boleta where id_abonamiento = " + idAbonamiento + " order by id_boleta";
		System.out.println("SQL stmt: " + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			Long idBoleta = Long.parseLong(rs.getString("ID_BOLETA"));
			idsBoletas.add(idBoleta);			
		}
		return idsBoletas;
	}
	
//	public void borrarBoletasDevueltas() throws SQLException
//	{
//		String sql = "delete from boleta where ESTADO = 'D'";
//		System.out.println("SQL stmt: " + sql);
//		PreparedStatement prepStmt = conn.prepareStatement(sql);
//		if(!prepStmt.executeQuery().next())
//			throw new SQLException("No se pudieron eliminar las boletas");
//		
//	}
	
	public ArrayList<String> obtenerIdBoletaFunCancelada(Long idFuncion) throws SQLException
	{
		ArrayList<String> idsBoletas = new ArrayList<>();
		
		String sql = "select * from boleta where id_funcion = " + idFuncion + " order by id_boleta";
		System.out.println("SQL stmt: " + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			Long idBoleta = Long.parseLong(rs.getString("ID_BOLETA"));
			Long idCliente = Long.parseLong(rs.getString("ID_CLIENTE"));
			idsBoletas.add(idBoleta + ","+idCliente);	
		}
		return idsBoletas;
	}
	
	public ArrayList<Usuario> buenosClientes(Long numBoletas) throws SQLException
	{
		ArrayList<Usuario> usuarios = new ArrayList<>();
		String numBol = String.valueOf(numBoletas);
		String sql = "with tabla1 as (select id_boleta, id_silla, id_cliente, id_localidad from ((select * from ISIS2304A241720.boleta)"
					 +" natural join (select * from ISIS2304A241720.sillas))),"
					 +" tabla2 as (select id_boleta, id_cliente, nombre from (tabla1 natural join ISIS2304A241720.localidad)),"
					 +" tabla3 as(select id_boleta, id_cliente, nombre from tabla2 where nombre != 'VIP'),"
					 +" tabla4 as(select id_cliente from tabla2 minus select id_cliente from tabla3),"
					 +" tabla5 as(select * from tabla4 natural join tabla2),"
					 +" tabla6 as (select distinct id_cliente, count(*) as cantidad from tabla5 group by id_cliente)"
					 +" select * from ( (select id_cliente as id_user from tabla6 where cantidad >= " + numBol + ") natural join ISIS2304A241720.usuario)";
		System.out.println("SQL stmt: " + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			Long idUsuario = Long.parseLong(rs.getString("ID_USER"));
			String nombre = rs.getString("NOMBRE_USER");
			String correo = rs.getString("CORREO_USER");
			Long rol = Long.parseLong(rs.getString("ROL_USER"));
			Usuario aux = new Usuario(idUsuario, nombre, correo, rol);
			usuarios.add(aux);
		}
		return usuarios;
	}

	public ArrayList<BoletasCompradas> consultaBoletasFecha(String fechaInicial, String fechaFinal) throws SQLException 
	{	
		ArrayList<BoletasCompradas> boletasCompradas = new ArrayList<>();
		String sql = "with tabla1 as (select sillas.ID_SILLA, "
				+" sillas.NUMERO as numeroSilla, "
				+" ID_LOCALIDAD, "
				+" localidad.NOMBRE as nombreLocalidad, "
				+" localidad.CAPACIDAD, "
				+" localidad.ID_SITIO as idSitio ,"
				+" localidad.PRECIO, "
				+" localidad.SILLA_NUMERADA from sillas natural join localidad), "
				+" tabla2 as (select * from boleta natural join funcion), "
				+" tabla3 as (select *from tabla1 inner join tabla2 on tabla1.id_silla = tabla2.id_silla), "
				+" tabla4 as (select distinct * from tabla3 inner join espectaculo on tabla3.id_espectaculo = espectaculo.id_espec), "
				+" tabla5 as (select * from tabla4 inner join requerimientoespectaculo on tabla4.id_espec = requerimientoespectaculo.ID_ESPECTACULO) "
				+" select * from tabla5 inner join REQUERIMIENTOS on REQUERIMIENTOS.ID_REQ = tabla5.id_requerimiento where fecha between '"+ fechaInicial +"' and '" + fechaFinal + "'";
		System.out.println("SQL stmt:  " + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			Long id_boleta = Long.parseLong(rs.getString("id_boleta"));
			Long id_localidad = Long.parseLong(rs.getString("id_localidad"));
			int numeroSilla = Integer.parseInt(rs.getString("NUMEROSILLA"));
			String nombreLocalidad = rs.getString("nombreLocalidad");
			Long idSitio = Long.parseLong(rs.getString("idSitio"));
			double precio = Double.parseDouble(rs.getString("PRECIO"));
			char silla_numerada = (rs.getString("SILLA_NUMERADA")).charAt(0);
			Long id_funcion = Long.parseLong(rs.getString("ID_FUNCION"));
			Long id_cliente = Long.parseLong(rs.getString("ID_CLIENTE"));
			Long id_abonamiento = Long.parseLong(rs.getString("ID_ABONAMIENTO"));
			char estado = (rs.getString("ESTADO")).charAt(0);
			String fecha = rs.getString("FECHA");
			int hora = Integer.parseInt(rs.getString("HORA"));
			char realizada = (rs.getString("REALIZADA")).charAt(0);
			Long id_espec = Long.parseLong(rs.getString("ID_ESPEC"));
			String nombre = rs.getString("NOMBRE");
			double duracion = Double.parseDouble(rs.getString("DURACION"));
			String idioma = rs.getString("IDIOMA");
			double costo =  Double.parseDouble(rs.getString("COSTO"));
			String descripcion = rs.getString("DESCRIPCION");
			char servicio_tradu = (rs.getString("SERVICIO_TRADU")).charAt(0);
			String publico_objetivo = rs.getString("PUBLICO_OBJETIVO");
			Long id_compania = Long.parseLong(rs.getString("ID_COMPANIA"));
			Long id_operario = Long.parseLong(rs.getString("ID_OPERARIO"));
			Long id_req = Long.parseLong(rs.getString("ID_REQ"));
//			String descripcion_1 = rs.getString("descripcion_1");
			String descripcion_1 = "";
			BoletasCompradas boletaCompra = new BoletasCompradas(id_boleta, id_localidad, numeroSilla, nombreLocalidad, idSitio, precio, silla_numerada, id_funcion, id_cliente, id_abonamiento, estado, fecha, hora, realizada, id_espec, nombre, duracion, idioma, costo, descripcion, servicio_tradu, publico_objetivo, id_compania, id_operario, id_req, descripcion_1); 
			boletasCompradas.add(boletaCompra);
		}
		
		return boletasCompradas;
		
	}

	public ArrayList<BoletasCompradas> consultaRequerimiento(Long id) throws SQLException {
		// TODO Auto-generated method stub
		ArrayList<BoletasCompradas> boletasCompradas = new ArrayList<>();
		String sql = "with tabla1 as (select sillas.ID_SILLA, "
				+" sillas.NUMERO as numeroSilla, "
				+" ID_LOCALIDAD, "
				+" localidad.NOMBRE as nombreLocalidad, "
				+" localidad.CAPACIDAD, "
				+" localidad.ID_SITIO as idSitio ,"
				+" localidad.PRECIO, "
				+" localidad.SILLA_NUMERADA from sillas natural join localidad), "
				+" tabla2 as (select * from boleta natural join funcion), "
				+" tabla3 as (select *from tabla1 inner join tabla2 on tabla1.id_silla = tabla2.id_silla), "
				+" tabla4 as (select distinct * from tabla3 inner join espectaculo on tabla3.id_espectaculo = espectaculo.id_espec), "
				+" tabla5 as (select * from tabla4 inner join requerimientoespectaculo on tabla4.id_espec = requerimientoespectaculo.ID_ESPECTACULO) "
				+" select * from tabla5 inner join REQUERIMIENTOS on REQUERIMIENTOS.ID_REQ = tabla5.id_requerimiento where id_req = "+ id;
		System.out.println("SQL stmt:  " + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			Long id_boleta = Long.parseLong(rs.getString("id_boleta"));
			Long id_localidad = Long.parseLong(rs.getString("id_localidad"));
			int numeroSilla = Integer.parseInt(rs.getString("NUMEROSILLA"));
			String nombreLocalidad = rs.getString("nombreLocalidad");
			Long idSitio = Long.parseLong(rs.getString("idSitio"));
			double precio = Double.parseDouble(rs.getString("PRECIO"));
			char silla_numerada = (rs.getString("SILLA_NUMERADA")).charAt(0);
			Long id_funcion = Long.parseLong(rs.getString("ID_FUNCION"));
			Long id_cliente = Long.parseLong(rs.getString("ID_CLIENTE"));
			Long id_abonamiento = Long.parseLong(rs.getString("ID_ABONAMIENTO"));
			char estado = (rs.getString("ESTADO")).charAt(0);
			String fecha = rs.getString("FECHA");
			int hora = Integer.parseInt(rs.getString("HORA"));
			char realizada = (rs.getString("REALIZADA")).charAt(0);
			Long id_espec = Long.parseLong(rs.getString("ID_ESPEC"));
			String nombre = rs.getString("NOMBRE");
			double duracion = Double.parseDouble(rs.getString("DURACION"));
			String idioma = rs.getString("IDIOMA");
			double costo =  Double.parseDouble(rs.getString("COSTO"));
			String descripcion = rs.getString("DESCRIPCION");
			char servicio_tradu = (rs.getString("SERVICIO_TRADU")).charAt(0);
			String publico_objetivo = rs.getString("PUBLICO_OBJETIVO");
			Long id_compania = Long.parseLong(rs.getString("ID_COMPANIA"));
			Long id_operario = Long.parseLong(rs.getString("ID_OPERARIO"));
			Long id_req = Long.parseLong(rs.getString("ID_REQ"));
//			String descripcion_1 = rs.getString("descripcion_1");
			String descripcion_1 = "";
			BoletasCompradas boletaCompra = new BoletasCompradas(id_boleta, id_localidad, numeroSilla, nombreLocalidad, idSitio, precio, silla_numerada, id_funcion, id_cliente, id_abonamiento, estado, fecha, hora, realizada, id_espec, nombre, duracion, idioma, costo, descripcion, servicio_tradu, publico_objetivo, id_compania, id_operario, id_req, descripcion_1); 
			boletasCompradas.add(boletaCompra);
		}
		
		return boletasCompradas;
	}

	public ArrayList<BoletasCompradas> consultaLocalidad(String nombreLocal) throws SQLException {
		// TODO Auto-generated method stub
		ArrayList<BoletasCompradas> boletasCompradas = new ArrayList<>();
		String sql = "with tabla1 as (select sillas.ID_SILLA, "
				+" sillas.NUMERO as numeroSilla, "
				+" ID_LOCALIDAD, "
				+" localidad.NOMBRE as nombreLocalidad, "
				+" localidad.CAPACIDAD, "
				+" localidad.ID_SITIO as idSitio ,"
				+" localidad.PRECIO, "
				+" localidad.SILLA_NUMERADA from sillas natural join localidad), "
				+" tabla2 as (select * from boleta natural join funcion), "
				+" tabla3 as (select *from tabla1 inner join tabla2 on tabla1.id_silla = tabla2.id_silla), "
				+" tabla4 as (select distinct * from tabla3 inner join espectaculo on tabla3.id_espectaculo = espectaculo.id_espec), "
				+" tabla5 as (select * from tabla4 inner join requerimientoespectaculo on tabla4.id_espec = requerimientoespectaculo.ID_ESPECTACULO) "
				+" select * from tabla5 inner join REQUERIMIENTOS on REQUERIMIENTOS.ID_REQ = tabla5.id_requerimiento where nombrelocalidad = '"+ nombreLocal+ "'";
		System.out.println("SQL stmt:  " + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			Long id_boleta = Long.parseLong(rs.getString("id_boleta"));
			Long id_localidad = Long.parseLong(rs.getString("id_localidad"));
			int numeroSilla = Integer.parseInt(rs.getString("NUMEROSILLA"));
			String nombreLocalidad = rs.getString("nombreLocalidad");
			Long idSitio = Long.parseLong(rs.getString("idSitio"));
			double precio = Double.parseDouble(rs.getString("PRECIO"));
			char silla_numerada = (rs.getString("SILLA_NUMERADA")).charAt(0);
			Long id_funcion = Long.parseLong(rs.getString("ID_FUNCION"));
			Long id_cliente = Long.parseLong(rs.getString("ID_CLIENTE"));
			Long id_abonamiento = Long.parseLong(rs.getString("ID_ABONAMIENTO"));
			char estado = (rs.getString("ESTADO")).charAt(0);
			String fecha = rs.getString("FECHA");
			int hora = Integer.parseInt(rs.getString("HORA"));
			char realizada = (rs.getString("REALIZADA")).charAt(0);
			Long id_espec = Long.parseLong(rs.getString("ID_ESPEC"));
			String nombre = rs.getString("NOMBRE");
			double duracion = Double.parseDouble(rs.getString("DURACION"));
			String idioma = rs.getString("IDIOMA");
			double costo =  Double.parseDouble(rs.getString("COSTO"));
			String descripcion = rs.getString("DESCRIPCION");
			char servicio_tradu = (rs.getString("SERVICIO_TRADU")).charAt(0);
			String publico_objetivo = rs.getString("PUBLICO_OBJETIVO");
			Long id_compania = Long.parseLong(rs.getString("ID_COMPANIA"));
			Long id_operario = Long.parseLong(rs.getString("ID_OPERARIO"));
			Long id_req = Long.parseLong(rs.getString("ID_REQ"));
//			String descripcion_1 = rs.getString("descripcion_1");
			String descripcion_1 = "";
			BoletasCompradas boletaCompra = new BoletasCompradas(id_boleta, id_localidad, numeroSilla, nombreLocalidad, idSitio, precio, silla_numerada, id_funcion, id_cliente, id_abonamiento, estado, fecha, hora, realizada, id_espec, nombre, duracion, idioma, costo, descripcion, servicio_tradu, publico_objetivo, id_compania, id_operario, id_req, descripcion_1); 
			boletasCompradas.add(boletaCompra);
		}
		
		return boletasCompradas;
	}

	public ArrayList<BoletasCompradas> consultaFranjaHoraria(int ini, int fin) throws SQLException {
		// TODO Auto-generated method stub
		ArrayList<BoletasCompradas> boletasCompradas = new ArrayList<>();
		String sql = "with tabla1 as (select sillas.ID_SILLA, "
				+" sillas.NUMERO as numeroSilla, "
				+" ID_LOCALIDAD, "
				+" localidad.NOMBRE as nombreLocalidad, "
				+" localidad.CAPACIDAD, "
				+" localidad.ID_SITIO as idSitio ,"
				+" localidad.PRECIO, "
				+" localidad.SILLA_NUMERADA from sillas natural join localidad), "
				+" tabla2 as (select * from boleta natural join funcion), "
				+" tabla3 as (select *from tabla1 inner join tabla2 on tabla1.id_silla = tabla2.id_silla), "
				+" tabla4 as (select distinct * from tabla3 inner join espectaculo on tabla3.id_espectaculo = espectaculo.id_espec), "
				+" tabla5 as (select * from tabla4 inner join requerimientoespectaculo on tabla4.id_espec = requerimientoespectaculo.ID_ESPECTACULO) "
				+" select * from tabla5 inner join REQUERIMIENTOS on REQUERIMIENTOS.ID_REQ = tabla5.id_requerimiento where hora between " + ini + " and " + fin;
		System.out.println("SQL stmt:  " + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			Long id_boleta = Long.parseLong(rs.getString("id_boleta"));
			Long id_localidad = Long.parseLong(rs.getString("id_localidad"));
			int numeroSilla = Integer.parseInt(rs.getString("NUMEROSILLA"));
			String nombreLocalidad = rs.getString("nombreLocalidad");
			Long idSitio = Long.parseLong(rs.getString("idSitio"));
			double precio = Double.parseDouble(rs.getString("PRECIO"));
			char silla_numerada = (rs.getString("SILLA_NUMERADA")).charAt(0);
			Long id_funcion = Long.parseLong(rs.getString("ID_FUNCION"));
			Long id_cliente = Long.parseLong(rs.getString("ID_CLIENTE"));
			Long id_abonamiento = Long.parseLong(rs.getString("ID_ABONAMIENTO"));
			char estado = (rs.getString("ESTADO")).charAt(0);
			String fecha = rs.getString("FECHA");
			int hora = Integer.parseInt(rs.getString("HORA"));
			char realizada = (rs.getString("REALIZADA")).charAt(0);
			Long id_espec = Long.parseLong(rs.getString("ID_ESPEC"));
			String nombre = rs.getString("NOMBRE");
			double duracion = Double.parseDouble(rs.getString("DURACION"));
			String idioma = rs.getString("IDIOMA");
			double costo =  Double.parseDouble(rs.getString("COSTO"));
			String descripcion = rs.getString("DESCRIPCION");
			char servicio_tradu = (rs.getString("SERVICIO_TRADU")).charAt(0);
			String publico_objetivo = rs.getString("PUBLICO_OBJETIVO");
			Long id_compania = Long.parseLong(rs.getString("ID_COMPANIA"));
			Long id_operario = Long.parseLong(rs.getString("ID_OPERARIO"));
			Long id_req = Long.parseLong(rs.getString("ID_REQ"));
//			String descripcion_1 = rs.getString("descripcion_1");
			String descripcion_1 = "";
			BoletasCompradas boletaCompra = new BoletasCompradas(id_boleta, id_localidad, numeroSilla, nombreLocalidad, idSitio, precio, silla_numerada, id_funcion, id_cliente, id_abonamiento, estado, fecha, hora, realizada, id_espec, nombre, duracion, idioma, costo, descripcion, servicio_tradu, publico_objetivo, id_compania, id_operario, id_req, descripcion_1); 
			boletasCompradas.add(boletaCompra);
		}
		
		return boletasCompradas;
	}

	


}

