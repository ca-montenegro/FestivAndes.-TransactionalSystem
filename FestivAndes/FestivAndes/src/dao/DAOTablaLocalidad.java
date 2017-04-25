package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import vos.Localidad;

public class DAOTablaLocalidad {

	
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
	public DAOTablaLocalidad() {
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
}
