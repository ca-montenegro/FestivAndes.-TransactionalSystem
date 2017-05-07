package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import vos.BoletasCompradas;
import vos.Cliente;
import vos.Usuario;

public class DAOTablaUsuario {

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
	public DAOTablaUsuario() {
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

	
}
