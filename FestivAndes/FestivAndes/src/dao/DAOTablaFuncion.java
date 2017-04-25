package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import vos.Funcion;

public class DAOTablaFuncion {

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
	public DAOTablaFuncion() {
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



	
	public Funcion buscarFuncion(Long id)throws SQLException
	{
		DAOTablaEspectaculo daoEspectaculo = new DAOTablaEspectaculo();
		daoEspectaculo.setConn(conn);
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
			func.setEspectaculo(daoEspectaculo.buscarEspectaculo(idE));
			daoEspectaculo.cerrarRecursos();
			return func;
		}
		
		return null;
	}
	
	public void programarFuncion(Funcion funcion) throws SQLException
	{
		DAOTablaSitio daoSitio = new DAOTablaSitio();
		DAOTablaEspectaculo daoEspectaculo = new DAOTablaEspectaculo();
		daoSitio.setConn(conn);
		daoEspectaculo.setConn(conn);
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

		funcion.setSitio(daoSitio.buscarSitio(funcion.getIdSitio()));
		funcion.setEspectaculo(daoEspectaculo.buscarEspectaculo(funcion.getIdEspectaculo()));
		daoSitio.cerrarRecursos();
		daoEspectaculo.cerrarRecursos();
	}
	
	public Funcion marcarRealizada(Long idFuncion) throws SQLException
	{
		DAOTablaSitio daoSitio = new DAOTablaSitio();
		DAOTablaEspectaculo daoEspectaculo = new DAOTablaEspectaculo();
		daoSitio.setConn(conn);
		daoEspectaculo.setConn(conn);
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
			fun.setEspectaculo(daoEspectaculo.buscarEspectaculo(fun.getIdEspectaculo()));
			fun.setSitio(daoSitio.buscarSitio(fun.getIdSitio()));
		}
		daoSitio.cerrarRecursos();
		daoEspectaculo.cerrarRecursos();
		return fun;
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
