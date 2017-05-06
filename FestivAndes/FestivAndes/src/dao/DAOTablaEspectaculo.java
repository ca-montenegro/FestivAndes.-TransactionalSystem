package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import vos.Espectaculo;

public class DAOTablaEspectaculo {

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
	public DAOTablaEspectaculo() {
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

	public Boolean cancelarFuncion(Long idFuncion) throws SQLException {
		
		String sql = "delete from funcion where id_funcion = " + idFuncion;
		System.out.println("SQL stmt: " + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		if(rs.next())
				return false;
		return true;
	}
}
