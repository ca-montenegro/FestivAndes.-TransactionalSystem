package dao;

import java.awt.List;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Queue;

import com.sun.javafx.collections.ArrayListenerHelper;

import sun.security.jca.GetInstance;
import vos.Abonamiento;
import vos.Boleta;
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

	public ArrayList<Usuario> darUsuarios() throws SQLException{
		ArrayList<Usuario> usuarios = new ArrayList<Usuario>();

		String sql = "SELECT * FROM ISIS2304A241720.USUARIO";

		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();

		while (rs.next()) {
			Long id = Long.parseLong(rs.getString("ID_USER"));
			String nombre = (rs.getString("NOMBRE_USER"));
			String correo = rs.getString("CORREO_USER");
			Long rol = Long.parseLong(rs.getString("ROL_USER"));
			usuarios.add(new Usuario(id, nombre, correo, rol));

		}
		return usuarios;
	}

	public Usuario darUsuario(Long id) throws SQLException{
		Usuario user = null;
		String sql = "SELECT * FROM USUARIO WHERE ID_USER = " + id;
		System.out.println("SQL stmt:" + sql);

		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			Long id1 = Long.parseLong(rs.getString("ID_USER"));
			String nombre = (rs.getString("NOMBRE_USER"));
			String correo = rs.getString("CORREO_USER");
			Long rol = Long.parseLong(rs.getString("ROL_USER"));
			user = new Usuario(id1, nombre, correo, rol);
		}
		return user;

	}



	public void registrarUsuario(Usuario user) throws SQLException 
	{
		String sql = "INSERT INTO ISIS2304A241720.USUARIO VALUES (";
		sql += user.getId() + ",'";
		sql += user.getNombre() + "','";
		sql += user.getCorreo() + "',";
		sql += user.getRol()+ ")";

		System.out.println("SQL stmt:" + sql);

		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		prepStmt.executeQuery();
	}

	public void registrarCliente(Usuario user) throws SQLException
	{
		String sql = "INSERT INTO ISIS2304A241720.USUARIO VALUES (";
		sql += user.getId() + ",'";
		sql += user.getNombre() + "','";
		sql += user.getCorreo() + "',";
		sql += 2+ ")";

		System.out.println("SQL stmt:" + sql);

		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		prepStmt.executeQuery();
	}

	public void registrarCompania(Compania comp) throws SQLException
	{
		String sql = "INSERT INTO ISIS2304A241720.COMPANIA VALUES(";
		sql+= comp.getId()+ ",'";
		sql+=comp.getNombre() + "','";
		sql += comp.getRepresentante() + "','";
		sql += comp.getPaisOrigen() + "','";
		sql += comp.getPaginaWeb() + "','";
		sql += comp.getFechaLlegada() + "','";
		sql += comp.getFechaSalida() + "',";
		sql += comp.getIdFestival()+ ",";
		sql += comp.getIdOrganizador()+")";

		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		prepStmt.executeQuery();
	}

	public void registrarSitio(Sitio sitio) throws SQLException
	{
		String sql = "INSERT INTO ISIS2304A241720.SITIO VALUES(";
		sql+= sitio.getId() + ",'";
		sql+=sitio.getTipo() +"',";
		sql+=sitio.getCapacidad()+",'";
		sql+=sitio.getDisponibilidad() + "','";
		sql+=sitio.getProteAtmosfera() +"')";

		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		prepStmt.executeQuery();
	}

	public void registrarLocalidad(Localidad local) throws SQLException
	{
		String sql = "INSERT INTO ISIS2304A241720.LOCALIDAD VALUES( ";
		sql+= local.getId() + ",'";
		sql+= local.getNombre() + "',";
		sql+=local.getCapacidad() + ",";
		sql+=local.getIdSitio() + ",";
		sql+=local.getPrecio() +",'";
		sql+=local.getSillaNumerada()+"'";

		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		prepStmt.executeQuery();
	}

	public void registrarEspectaculo(Espectaculo espec) throws SQLException
	{
		String sql = "INSERT INTO ISIS2304A241720.ESPECTACULO VALUES( ";
		sql+= espec.getId() + ",'";
		sql+= espec.getNombre() + "',";
		sql+=espec.getDuracion() + ",'";
		sql+=espec.getIdioma() + "',";
		sql+=espec.getCosto() +",'";
		sql+=espec.getDescripcion()+"','";
		sql+=espec.getServicioTraduccion()+"','";
		sql+=espec.getParticipacion()+"','";
		sql+=espec.getPublicoObjetivo()+"',";
		sql+=espec.getIdCompania() + ",";
		sql+=espec.getIdOperario()+")";

		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		prepStmt.executeQuery();
	}

	public void programarFuncion(Funcion funcion) throws SQLException
	{
		String sql = "INSERT INTO ISIS2304A241720.FUNCION VALUES(";
		sql+=funcion.getId() + ",'";
		sql+=funcion.getFecha() + "',";
		sql+=funcion.getIdSitio() + ",";
		sql+=funcion.getIdEspectaculo() + ",'";
		sql+=funcion.getHora()+"','";
		sql+=funcion.getRealizada()+"')";

		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		prepStmt.executeQuery();

		funcion.setSitio(buscarSitio(funcion.getIdSitio()));
		funcion.setEspectaculo(buscarEspectaculo(funcion.getIdEspectaculo()));

	}

	public Sitio buscarSitio(Long id) throws SQLException
	{

		String sql = "SELECT * FROM ISIS2304A241720.SITIO WHERE ID_SITIO = " + id;
		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		Sitio st = null;
		if(rs.next())
		{
			Long idS = Long.parseLong(rs.getString("ID_SITIO"));
			char tipo = (rs.getString("TIPO")).charAt(0);
			int capacidad = Integer.parseInt(rs.getString("CAPACIDAD"));
			String horario = rs.getString("HORARIO_DISPONIBILIDAD");
			char protec = (rs.getString("PROTECCION_ATMOSFERICA")).charAt(0);
			st = new Sitio(idS, tipo, capacidad, horario, protec);
		}
		return st;
	}

	public Espectaculo buscarEspectaculo(Long id) throws SQLException
	{

		String sql = "SELECT * FROM ISIS2304A241720.ESPECTACULO WHERE ID_ESPEC = " + id;
		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		Espectaculo espect = null;
		if(rs.next())
		{
			Long idS = Long.parseLong(rs.getString("ID_ESPEC"));
			String nombre = (rs.getString("NOMBRE"));
			int duracion = Integer.parseInt(rs.getString("DURACION"));
			String idioma = (rs.getString("IDIOMA"));
			int costo = Integer.parseInt(rs.getString("COSTO"));
			String descripcion = rs.getString("DESCRIPCION");
			char tradu = (rs.getString("SERVICIO_TRADU")).charAt(0);
			boolean traduc = false;
			if(tradu=='S')
				traduc=true;
			boolean parti = false;
			char participacion = (rs.getString("SERVICIO_TRADU")).charAt(0);
			if(participacion=='S')
				parti = true;
			String publicoObjetivo = rs.getString("PUBLICO_OBJETIVO");
			Long idCom = Long.parseLong(rs.getString("ID_COMPANIA"));
			Long idOper = Long.parseLong(rs.getString("ID_OPERARIO"));
			espect  = new Espectaculo(idS, nombre, duracion, idioma, costo, descripcion, tradu, participacion, publicoObjetivo, idCom, idOper);
		}
		return espect;
	}

	public Boleta buscarBoleta(Long id) throws SQLException
	{

		String sql = "SELECT * FROM ISIS2304A241720.BOLETA WHERE ID_BOLETA = " + id;
		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		Boleta boleta = null;
		if(rs.next())
		{
			Long idFuncion = Long.parseLong(rs.getString("ID_FUNCION"));
			Long idSilla = Long.parseLong(rs.getString("ID_SILLA"));
			char estado = (rs.getString("ESTADO")).charAt(0);
			Long idCliente = Long.parseLong(rs.getString("ID_CLIENTE"));
			Long idAbonamiento = Long.parseLong(rs.getString("ID_ABONAMIENTO"));

			boleta = new Boleta(id, idFuncion, idSilla, 0, estado);
			boleta.setFuncion(buscarFuncion(idFuncion));
			boleta.setSilla(buscarSilla(idSilla));
			boleta.setLocalidad(buscarLocalidad(boleta.getSilla().getIdLocalidad()));
			boleta.setIdCliente(idCliente);
			boleta.setPrecio(boleta.getLocalidad().getPrecio());
			boleta.setIdAbonamiento(idAbonamiento);
			boleta.setFuncion(buscarFuncion(idFuncion));
		}
		return boleta;
	}


	public void agregarPreferencia(Preferencia prefe, Long id) throws SQLException
	{
		String sql2 ="SELECT * FROM ISIS2304A241720.USUARIO WHERE ISIS2304A241720.USUARIO.ID_USER=" + id;
		System.out.println("SQL stmt:" + sql2);
		PreparedStatement prepStmt2 = conn.prepareStatement(sql2);
		recursos.add(prepStmt2);
		ResultSet er = prepStmt2.executeQuery();
		if(er.next()){
			String sql = "INSERT INTO ISIS2304A241720.PREFERENCIA VALUES(";
			sql+=prefe.getId() + ",'";
			sql+=prefe.getNombre()+"','";
			sql+=prefe.getDescripcion()+"')";

			System.out.println("SQL stmt:" + sql);
			PreparedStatement prepStmt = conn.prepareStatement(sql);
			recursos.add(prepStmt);
			prepStmt.executeQuery();
			Long idPref = prefe.getId();

			String sql3 = "INSERT INTO ISIS2304A241720.CLIENTE_PREFERENCIA VALUES(";
			sql3+= idPref +",";
			sql3+= id+")";
			System.out.println("SQL stmt:" + sql3);
			PreparedStatement prepStmt3 = conn.prepareStatement(sql3);
			recursos.add(prepStmt3);
			prepStmt3.executeQuery();

		}

	}

	public void deletePreferencia(Preferencia prefe, Long id) throws SQLException
	{
		String sql = "DELETE FROM ISIS2304A241720.CLIENTE_PREFERENCIA WHERE ISIS2304A241720.CLIENTE_PREFERENCIA.ID_PREFERENCIA=" + 
				prefe.getId() + " AND ISIS2304A241720.CLIENTE_PREFERENCIA.ID_CLIENTE = " + id;
		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet er = prepStmt.executeQuery();
		if(er.next())
		{
			String sql2 = "DELETE FROM ISIS2304A241720.PREFERENCIA WHERE ISIS2304A241720.PREFERENCIA.ID_PREFERENCIA =" + 
					prefe.getId();
			System.out.println("SQL stmt:" + sql2);
			PreparedStatement prepStmt2 = conn.prepareStatement(sql2);
			recursos.add(prepStmt2);
			prepStmt2.executeQuery();
		}
	}

	public void updatePreferencia(Preferencia prefe) throws SQLException
	{
		String sql = "UPDATE ISIS2304A241720.PREFERENCIA SET ";
		sql+="ISIS2304A241720.PREFERENCIA.TIPO_PREFERENCIA ="+"'"+ prefe.getNombre()+"'";
		sql+=", ISIS2304A241720.PREFERENCIA.DESCRIPCION="+"'"+ prefe.getDescripcion()+"'";
		sql+=" WHERE ISIS2304A241720.PREFERENCIA.ID_PREFERENCIA = " + prefe.getId(); 
		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		prepStmt.executeQuery();
	}

	public Preferencia buscarPreferencia(Long id) throws SQLException
	{
		String sql = "SELECT * FROM ISIS2304A241720.PREFERENCIA WHERE ID_PREFERENCIA = " + id;
		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet er = prepStmt.executeQuery();
		Preferencia prefe = null;
		if(er.next())
		{
			String tipo = er.getString("TIPO_PREFERENCIA");
			String descripcion = er.getString("DESCRIPCION");
			prefe = new Preferencia(id, tipo, descripcion);
		}
		return prefe;
	}


	public Boleta venderBoleta(Long idFuncion, Long idSilla, Long idCliente, Long idAbonamiento) throws SQLException
	{
		Silla silla = buscarSilla(idSilla);
		if(silla==null)
			throw new SQLException("No se encontró la silla");
		Localidad local = buscarLocalidad(silla.getIdLocalidad());
		if(local==null)
			throw new SQLException("No se encontró la localidad");
		silla.setLocalidad(local);
		local.setSitio(buscarSitio(local.getIdSitio()));
		Funcion funcion = buscarFuncion(idFuncion);
		if(funcion==null)
			throw new SQLException("No se encontró la función. Por favor selecciona una función existente.");
		funcion.setSitio(buscarSitio(funcion.getIdSitio()));
		funcion.setEspectaculo(buscarEspectaculo(funcion.getIdEspectaculo()));
		String sql = "SELECT * FROM ISIS2304A241720.SILLAS NATURAL JOIN ISIS2304A241720.LOCALIDAD WHERE NUMERO =" + silla.getNumero()
		+ " AND " + "LOCALIDAD.NOMBRE= '" + silla.getLocalidad().getNombre()+"'";	
		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			//Verificar que una boleta no se haya vendido
			String sql1 = "SELECT * FROM ISIS2304A241720.BOLETA WHERE ID_SILLA="+silla.getId() + "and ID_FUNCION = " + idFuncion;
			System.out.println("SQL stmt:" + sql1);
			PreparedStatement prepStmt1 = conn.prepareStatement(sql1);
			recursos.add(prepStmt1);
			ResultSet rsBoleta = prepStmt1.executeQuery();
			if(rsBoleta.next()){
				throw new SQLException("Ya hay una boleta para la silla: "+idSilla + " y la funci�n: " +idFuncion);
			}
			else{


				String sql2="INSERT INTO ISIS2304A241720.BOLETA VALUES( sequence_boleta.NEXTVAL,"+ idFuncion
						+ "," + idSilla+ "," + idCliente + "," + idAbonamiento + ","  + "'A'" + ")";
				String key[] = {"ID_BOLETA"};
				PreparedStatement prepStmt2 = conn.prepareStatement(sql2,key);
				recursos.add(prepStmt2);
				prepStmt2.executeUpdate();
				System.out.println("SQL stmt:" + sql2);
				ResultSet rsInsert = prepStmt2.getGeneratedKeys();
				Long id = null;
				if (rsInsert.next()) {
					id = rsInsert.getLong(1);
					System.out.println(id);
				}
				Boleta boletaVendida = new Boleta(id,funcion.getId(), silla.getId(),0,'A');
				boletaVendida.setIdCliente(idCliente);
				boletaVendida.setId(id);
				boletaVendida.setFuncion(funcion);
				boletaVendida.setSilla(silla);
				boletaVendida.setLocalidad(local);
				boletaVendida.setCliente(darCliente(idCliente));
				boletaVendida.setPrecio(local.getPrecio());

				return boletaVendida;
			}

		}
		return null;
	}

	public Funcion marcarRealizada(Long idFuncion) throws SQLException
	{
		Funcion fun = buscarFuncion(idFuncion);
		if(fun==null)
			throw new SQLException("No existe la función");
		if(fun.getRealizada()=='N')
		{
			String sql = "UPDATE FUNCION SET REALIZADA = 'S' WHERE ID_FUNCION = " + idFuncion;
			PreparedStatement st = conn.prepareStatement(sql);
			recursos.add(st);
			st.executeUpdate();
			fun = buscarFuncion(idFuncion);
			fun.setEspectaculo(buscarEspectaculo(fun.getIdEspectaculo()));
			fun.setSitio(buscarSitio(fun.getIdSitio()));
		}
		return fun;
	}



	public Silla buscarSilla(Long id) throws SQLException
	{
		String sql = "SELECT * FROM ISIS2304A241720.SILLAS WHERE ID_SILLA = " + id;
		System.out.println("SQL stmt: "+ sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		if(rs.next())
		{
			Long idS = Long.parseLong(rs.getString("ID_SILLA"));
			int numero = Integer.parseInt(rs.getString("NUMERO"));
			Long idL = Long.parseLong(rs.getString("ID_LOCALIDAD"));
			return new Silla(idS, idL, numero);
		}
		return null;
	}

	public Localidad buscarLocalidad(Long id) throws SQLException
	{
		String sql = "SELECT * FROM ISIS2304A241720.LOCALIDAD WHERE ID_LOCALIDAD = " + id;
		System.out.println("SQL stmt: "+ sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		if(rs.next())
		{
			Long idL = Long.parseLong(rs.getString("ID_LOCALIDAD"));
			String nombre = rs.getString("NOMBRE");
			int capacidad = Integer.parseInt(rs.getString("CAPACIDAD"));
			Long idS = Long.parseLong(rs.getString("ID_SITIO"));
			int precio = Integer.parseInt(rs.getString("PRECIO"));
			boolean sillaNumerada = false;
			char sillaNum = (rs.getString("SILLA_NUMERADA")).charAt(0);
			if(sillaNum=='S')
			{
				sillaNumerada=true;
			}

			return new Localidad(idL, idS, nombre, capacidad,sillaNum, precio);
		}
		return null;
	}

	public Funcion buscarFuncion(Long id)throws SQLException
	{
		String sql = "SELECT * FROM ISIS2304A241720.FUNCION WHERE ID_FUNCION = " + id;
		System.out.println("SQL stmt: "+ sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		if(rs.next())
		{
			Long idF = Long.parseLong(rs.getString("ID_FUNCION"));
			String fecha = rs.getString("FECHA");
			Long idS = Long.parseLong(rs.getString("ID_SITIO"));
			Long idE = Long.parseLong(rs.getString("ID_ESPECTACULO"));
			int hora = Integer.parseInt(rs.getString("HORA"));
			char realizada = (rs.getString("REALIZADA")).charAt(0);
			Funcion func = new Funcion(idF, fecha, idS, idE, hora, realizada);
			func.setEspectaculo(buscarEspectaculo(idE));

			return func;
		}
		return null;
	}

	public Cliente darCliente(Long id) throws SQLException{
		Cliente user = null;
		String sql = "SELECT * FROM USUARIO WHERE ID_USER = " + id;
		System.out.println("SQL stmt:" + sql);

		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			Long id1 = Long.parseLong(rs.getString("ID_USER"));
			String nombre = (rs.getString("NOMBRE_USER"));
			String correo = rs.getString("CORREO_USER");
			//Long rol = Long.parseLong(rs.getString("ROL_USER"));
			user = new Cliente(id1, nombre, correo);
		}
		return user;

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
			char[] cha2 = rs.getString("ESTADO").toCharArray();
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
		
		resp.add(activasRealizadas);
		resp.add(activasNoRealizadas);
		resp.add(devueltasRealizadas);
		resp.add(devueltasNoRealizadas);
		
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
		ArrayList<FuncionCompania> funciones = new ArrayList<>();
		
		String sql = "WITH TABLA1 AS (SELECT ID_ESPEC, NOMBRE, ID_COMPANIA FROM "+
				"ISIS2304A241720.ESPECTACULO WHERE ID_COMPANIA = 19), "+ 
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
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		Boleta boleta = buscarBoleta(idBoleta);
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

	public ArrayList<Long> verificarSitioLocalidad(Abonamiento abonamiento) throws SQLException
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
	
	public void borrarBoletasDevueltas() throws SQLException
	{
		String sql = "delete from boleta where ESTADO = 'D'";
		System.out.println("SQL stmt: " + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		if(!prepStmt.executeQuery().next())
			throw new SQLException("No se pudieron eliminar las boletas");
		
	}
	
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

	public void cancelarFuncion(Long idFuncion) throws SQLException {
		
		String sql = "delete from funcion where id_funcion = " + idFuncion;
		System.out.println("SQL stmt: " + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		ResultSet rs = prepStmt.executeQuery();
		if(!rs.next())
			throw new SQLException("No se pudo eliminar la funcion con id: " + idFuncion);
		
	}
	


}

