package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import vos.VOBoleta;
import vos.Funcion;
import vos.Localidad;
import vos.Silla;

public class DAOTablaBoleta {

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
	public DAOTablaBoleta() {
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
	



	public VOBoleta buscarBoleta(Long id) throws SQLException
	{
		DAOTablaFuncion daoFuncion = new DAOTablaFuncion();
		DAOTablaSilla daoSilla = new DAOTablaSilla();
		DAOTablaLocalidad daoLocalidad = new DAOTablaLocalidad();
		daoFuncion.setConn(conn);
		daoSilla.setConn(conn);
		daoLocalidad.setConn(conn);
		String sql = "SELECT * FROM ISIS2304A241720.BOLETA WHERE ID_BOLETA = " + id;
		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		VOBoleta boleta = null;
		if(rs.next())
		{
			Long idFuncion = Long.parseLong(rs.getString("ID_FUNCION"));
			Long idSilla = Long.parseLong(rs.getString("ID_SILLA"));
			char estado = (rs.getString("ESTADO")).charAt(0);
			Long idCliente = Long.parseLong(rs.getString("ID_CLIENTE"));
			Long idAbonamiento = Long.parseLong(rs.getString("ID_ABONAMIENTO"));

			boleta = new VOBoleta(id, idFuncion, idSilla, 0, estado);
			boleta.setFuncion(daoFuncion.buscarFuncion(idFuncion));
			boleta.setSilla(daoSilla.buscarSilla(idSilla));
			boleta.setLocalidad(daoLocalidad.buscarLocalidad(boleta.getSilla().getIdLocalidad()));
			boleta.setIdCliente(idCliente);
			boleta.setPrecio(boleta.getLocalidad().getPrecio());
			boleta.setIdAbonamiento(idAbonamiento);
//			boleta.setFuncion(daoFuncion.buscarFuncion(idFuncion));
			
		}
		daoFuncion.cerrarRecursos();
		daoSilla.cerrarRecursos();
		daoLocalidad.cerrarRecursos();
		return boleta;
	}
	
	public VOBoleta venderBoleta(Long idFuncion, Long idSilla, Long idCliente, Long idAbonamiento) throws SQLException
	{
		DAOTablaFuncion daoFuncion = new DAOTablaFuncion();
		DAOTablaSilla daoSilla = new DAOTablaSilla();
		DAOTablaLocalidad daoLocalidad = new DAOTablaLocalidad();
		DAOTablaSitio daoSitio = new DAOTablaSitio();
		DAOTablaEspectaculo daoEspectaculo = new DAOTablaEspectaculo();
		DAOTablaUsuario daoUsuario = new DAOTablaUsuario();
		daoFuncion.setConn(conn);
		daoSilla.setConn(conn);
		daoLocalidad.setConn(conn);
		daoSitio.setConn(conn);
		daoEspectaculo.setConn(conn);
		daoUsuario.setConn(conn);
		Silla silla = daoSilla.buscarSilla(idSilla);
		if(silla==null)
			throw new SQLException("No se encontró la silla");
		Localidad local = daoLocalidad.buscarLocalidad(silla.getIdLocalidad());
		if(local==null)
			throw new SQLException("No se encontró la localidad");
		silla.setLocalidad(local);
		local.setSitio(daoSitio.buscarSitio(local.getIdSitio()));
		Funcion funcion = daoFuncion.buscarFuncion(idFuncion);
		if(funcion==null)
			throw new SQLException("No se encontró la función. Por favor selecciona una función existente.");
		funcion.setSitio(daoSitio.buscarSitio(funcion.getIdSitio()));
		funcion.setEspectaculo(daoEspectaculo.buscarEspectaculo(funcion.getIdEspectaculo()));
		String sql = "SELECT * FROM ISIS2304A241720.SILLAS NATURAL JOIN ISIS2304A241720.LOCALIDAD WHERE NUMERO =" + silla.getNumero()
		+ " AND " + "LOCALIDAD.NOMBRE= '" + silla.getLocalidad().getNombre()+"'";	
		System.out.println("SQL stmt:" + sql);
		PreparedStatement prepStmt = conn.prepareStatement(sql);
		recursos.add(prepStmt);
		ResultSet rs = prepStmt.executeQuery();
		VOBoleta returnBoleta = null;
		while(rs.next())
		{
			//Verificar que una boleta no se haya vendido
			String sql1 = "SELECT * FROM ISIS2304A241720.BOLETA WHERE ID_SILLA="+silla.getId() + "and ID_FUNCION = " + idFuncion+ " and ESTADO = 'A'";
			System.out.println("SQL stmt:" + sql1);
			PreparedStatement prepStmt1 = conn.prepareStatement(sql1);
			recursos.add(prepStmt1);
			ResultSet rsBoleta = prepStmt1.executeQuery();
			if(rsBoleta.next()){
				throw new SQLException("Ya hay una boleta para la silla: "+idSilla + " y la funci�n: " +idFuncion);
			}
			else{


				String sql2="INSERT INTO ISIS2304A241720.BOLETA VALUES( sequence_boleta.NEXTVAL + 12452352,"+ idFuncion
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
				VOBoleta boletaVendida = new VOBoleta(id,funcion.getId(), silla.getId(),0,'A');
				boletaVendida.setIdCliente(idCliente);
				boletaVendida.setId(id);
				boletaVendida.setFuncion(funcion);
				boletaVendida.setSilla(silla);
				boletaVendida.setLocalidad(local);
				boletaVendida.setCliente(daoUsuario.darCliente(idCliente));
				boletaVendida.setPrecio(local.getPrecio());
				
				return boletaVendida;
			}

		}
		daoFuncion.cerrarRecursos();
		daoSilla.cerrarRecursos();
		daoLocalidad.cerrarRecursos();
		daoSitio.cerrarRecursos();
		daoEspectaculo.cerrarRecursos();
		daoUsuario.cerrarRecursos();
		
		return returnBoleta;
	}
}
