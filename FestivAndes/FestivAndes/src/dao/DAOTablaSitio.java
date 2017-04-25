package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import vos.Sitio;

public class DAOTablaSitio {

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
	public DAOTablaSitio() {
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
}
