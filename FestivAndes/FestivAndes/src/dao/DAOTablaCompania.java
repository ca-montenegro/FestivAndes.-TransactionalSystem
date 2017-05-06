package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import vos.Compania;
import vos.Usuario;

public class DAOTablaCompania {

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
	public DAOTablaCompania() {
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
	
	public Compania buscarCompania(Long id) throws SQLException
	{
		String sql = "SELECT * FROM ISIS2304A241720.COMPANIA WHERE ID_COMPA = " + id;
		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		Compania st = null;
		if(rs.next())
		{
			Long idComp = Long.parseLong(rs.getString("ID_COMPA"));
			String nombre = rs.getString("NOMBRE_COMPA");
			String repre = rs.getString("REPRESEN_COMPA");
			String pais = rs.getString("PAIS_ORIGEN_COMPA");
			String pagWeb = rs.getString("PAG_WEB_COMPA");
			String llegada = rs.getString("FECHA_LLEGADA_COMPA");
			String salida = rs.getString("FECHA_SALIDA_COMPA");
			Long idFest = Long.parseLong(rs.getString("ID_FESTIVAL"));
			Long idOrga = Long.parseLong(rs.getString("ID_ORGANI"));
			st = new Compania(idComp, nombre, repre, pais, pagWeb, llegada, salida, idFest, idOrga);
		}
		return st;
	}
	
	public ArrayList<Usuario> siAsistieron(Long id) throws SQLException
	{
		ArrayList<Usuario> usuarios = new ArrayList<>();
		String idcompa = String.valueOf(id);
		String sql = "with tabla1 as (select id_funcion as id_fun from ((select id_espec from ISIS2304A241720.espectaculo where id_compania =" + idcompa + ")"
				+ " inner join ISIS2304A241720.funcion on id_espec = id_espectaculo)),"
				+ " tabla2 as (select distinct id_cliente from tabla1 inner join (select * from ISIS2304A241720.boleta where estado = 'A')"
				+ " on id_fun = id_funcion)"
				+ " select * from (tabla2 inner join ISIS2304A241720.usuario on id_cliente = id_user)";
		System.out.println("SQL stmt: " + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			Long idUsuario = Long.parseLong(rs.getString("ID_CLIENTE"));
			String nombre = rs.getString("NOMBRE_USER");
			String correo = rs.getString("CORREO_USER");
			Long rol = Long.parseLong(rs.getString("ROL_USER"));
			Usuario aux = new Usuario(idUsuario, nombre, correo, rol);
			usuarios.add(aux);
		}
		return usuarios;
	}
	
	public ArrayList<Usuario> noAsistieron(Long id) throws SQLException
	{
		ArrayList<Usuario> usuarios = new ArrayList<>();
		String idcompa = String.valueOf(id);
		String sql = "with tabla1 as (select id_funcion as id_fun from ((select id_espec from ISIS2304A241720.espectaculo where id_compania =" + idcompa + ")"
				+ " inner join ISIS2304A241720.funcion on id_espec = id_espectaculo)),"
				+ " tabla2 as (select distinct id_cliente from tabla1 inner join (select * from ISIS2304A241720.boleta where estado = 'D')"
				+ " on id_fun = id_funcion)"
				+ " select * from (tabla2 inner join ISIS2304A241720.usuario on id_cliente = id_user)";
		System.out.println("SQL stmt: " + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		ResultSet rs = prepStmt.executeQuery();
		while(rs.next())
		{
			Long idUsuario = Long.parseLong(rs.getString("ID_CLIENTE"));
			String nombre = rs.getString("NOMBRE_USER");
			String correo = rs.getString("CORREO_USER");
			Long rol = Long.parseLong(rs.getString("ROL_USER"));
			Usuario aux = new Usuario(idUsuario, nombre, correo, rol);
			usuarios.add(aux);
		}
		return usuarios;
	}
}
