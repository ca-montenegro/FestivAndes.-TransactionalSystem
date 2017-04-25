package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import vos.Preferencia;

public class DAOTablaPreferencia {

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
	public DAOTablaPreferencia() {
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

}
