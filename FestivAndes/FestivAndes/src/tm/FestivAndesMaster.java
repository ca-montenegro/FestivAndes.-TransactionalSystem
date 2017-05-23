package tm;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import dao.DAOTablaBoleta;
import dao.DAOTablaCompania;
import dao.DAOTablaEspectaculo;
import dao.DAOTablaFestival;
import dao.DAOTablaFuncion;
import dao.DAOTablaLocalidad;
import dao.DAOTablaPreferencia;
import dao.DAOTablaSilla;
import dao.DAOTablaSitio;
import dao.DAOTablaUsuario;
import dtm.FestivAndesDistributed;
import jms.NonReplyException;
import vos.VOAbonamiento;
import vos.VOBoleta;
import vos.BoletasCompradas;
import vos.Compania;
import vos.Espectaculo;
import vos.InformacionVentaFuncion;
import vos.InformacionVentaLocalidad;
import vos.ListaBuenosClientes;
import vos.ListaFuncioneSitio;
import vos.ListaFuncionesCompania;
import vos.ListaInformacion;
import vos.ListaInformacionFuncion;
import vos.ListaLocalidades;
import vos.ListaPorEstado;
import vos.ListaPorRealizacion;
import vos.ListaRespuestaAsistencia;
import vos.ListaUsuarios;
import vos.Localidad;
import vos.MasPopuEspectaculo;
import vos.NotaDebito;
import vos.Funcion;
import vos.FuncionCompania;
import vos.FuncionRespuestaCliente;
import vos.InformacionFuncionSitio;
import vos.Preferencia;
import vos.Rentabilidad;
import vos.Silla;
import vos.Sitio;
import vos.Usuario;



public class FestivAndesMaster {



	/**
	 * Atributo estático que contiene el path relativo del archivo que tiene los datos de la conexión
	 */
	private static final String CONNECTION_DATA_FILE_NAME_REMOTE = "/conexion.properties";

	/**
	 * Atributo estático que contiene el path absoluto del archivo que tiene los datos de la conexión
	 */
	private  String connectionDataPath;

	/**
	 * Atributo que guarda el usuario que se va a usar para conectarse a la base de datos.
	 */
	private String user;

	/**
	 * Atributo que guarda la clave que se va a usar para conectarse a la base de datos.
	 */
	private String password;

	/**
	 * Atributo que guarda el URL que se va a usar para conectarse a la base de datos.
	 */
	private String url;

	/**
	 * Atributo que guarda el driver que se va a usar para conectarse a la base de datos.
	 */
	private String driver;

	/**
	 * Conexión a la base de datos
	 */
	private Connection conn;
	
	private FestivAndesDistributed dtm;

	/**
	 * Método constructor de la clase VideoAndesMaster, esta clase modela y contiene cada una de las 
	 * transacciones y la logia de negocios que estas conllevan.
	 * <b>post: </b> Se crea el objeto VideoAndesMaster, se inicializa el path absoluto de el archivo de conexión y se
	 * inicializa los atributos que se usan par la conexión a la base de datos.
	 * @param contextPathP - path absoluto en el servidor del contexto del deploy actual
	 */
	public FestivAndesMaster(String contextPathP) {
		connectionDataPath = contextPathP + CONNECTION_DATA_FILE_NAME_REMOTE;
		initConnectionData();
		System.out.println("Instancing DTM...");
		dtm = FestivAndesDistributed.getInstance(this);
		System.out.println("Done!");
	}

	public void commit() throws SQLException
	{
		conn.commit();
	}

	/*
	 * Método que  inicializa los atributos que se usan para la conexion a la base de datos.
	 * <b>post: </b> Se han inicializado los atributos que se usan par la conexión a la base de datos.
	 */
	private void initConnectionData() {
		try {
			File arch = new File(this.connectionDataPath);
			Properties prop = new Properties();
			FileInputStream in = new FileInputStream(arch);
			prop.load(in);
			in.close();
			this.url = prop.getProperty("url");
			this.user = prop.getProperty("usuario");
			this.password = prop.getProperty("clave");
			this.driver = prop.getProperty("driver");
			Class.forName(driver);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Método que  retorna la conexión a la base de datos
	 * @return Connection - la conexión a la base de datos
	 * @throws SQLException - Cualquier error que se genere durante la conexión a la base de datos
	 */
	private Connection darConexion() throws SQLException {
		System.out.println("Connecting to: " + url + " With user: " + user);
		return DriverManager.getConnection(url, user, password);
	}

	////////////////////////////////////////
	///////Transacciones////////////////////
	////////////////////////////////////////

	/**
	 * Método que modela la transacción que retorna todos los videos de la base de datos.
	 * @return ListaVideos - objeto que modela  un arreglo de videos. este arreglo contiene el resultado de la búsqueda
	 * @throws Exception -  cualquier error que se genere durante la transacción
	 */
	public ArrayList<Usuario> darUsuarios() throws Exception {
		ArrayList<Usuario> usuarios;
		DAOTablaUsuario daoFestival = new DAOTablaUsuario();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			usuarios = daoFestival.darUsuarios();
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return usuarios;

	}

	public Usuario buscarUsuarioPorID(Long id) throws Exception {

		DAOTablaUsuario daoFestival = new DAOTablaUsuario();
		Usuario usuario = null;
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			usuario = daoFestival.darUsuario(id);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return usuario;
	}

	/**
	 * Método que modela la transacción que agrega un solo usuario a la base de datos.
	 * <b> post: </b> se ha agregado el usuario que entra como parámetro
	 * @param usuario - el usuario a agregar. video != null
	 * @throws Exception - cualquier error que se genera agregando el usuario
	 */
	public void addUsuario(Usuario usuario) throws Exception {
		DAOTablaUsuario daoFestival = new DAOTablaUsuario();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			daoFestival.registrarUsuario(usuario);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
	}

	/**
	 * Método que modela la transacción que agrega un solo un cliente a la base de datos.
	 * <b> post: </b> se ha agregado el usuario que entra como parámetro
	 * @param usuario - el usuario a agregar. video != null
	 * @throws Exception - cualquier error que se genera agregando el cliente
	 */
	public void addCliente(Usuario usuario) throws Exception {
		DAOTablaUsuario daoFestival = new DAOTablaUsuario();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			daoFestival.registrarCliente(usuario);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
	}

	/**
	 * Método que modela la transacción que agrega una sola compa�ia a la base de datos.
	 * <b> post: </b> se ha agregado compa�ia que entra como parámetro
	 * @param usuario -la compa�ia a agregar. video != null
	 * @throws Exception - cualquier error que se genera agregando la compa�ia
	 */
	public void addCompania(Compania compania) throws Exception {
		DAOTablaCompania daoFestival = new DAOTablaCompania();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			daoFestival.registrarCompania(compania);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
	}

	/**
	 * Método que modela la transacción que agrega un solo sitio a la base de datos.
	 * <b> post: </b> se ha agregado sitio que entra como parámetro
	 * @param usuario -sitio a agregar. video != null
	 * @throws Exception - cualquier error que se genera agregando el sitio
	 */
	public void addSitio(Sitio sitio) throws Exception {
		DAOTablaSitio daoFestival = new DAOTablaSitio();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			daoFestival.registrarSitio(sitio);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
	}

	public void addLocalidad(Localidad local) throws Exception {
		DAOTablaLocalidad daoFestival = new DAOTablaLocalidad();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			daoFestival.registrarLocalidad(local);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
	}

	/**
	 * Método que modela la transacción que agrega una sola función a la base de datos.
	 * <b> post: </b> se ha agregado una función que entra como parámetro
	 * @param funcion -funcion a agregar. funcion != null
	 * @throws Exception - cualquier error que se genera agregando la funcion
	 */
	public void addFuncion(Funcion funcion) throws Exception {
		DAOTablaFuncion daoFestival = new DAOTablaFuncion();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			daoFestival.programarFuncion(funcion);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
	}

	/**
	 * Método que modela la transacción que agrega una sola función a la base de datos.
	 * <b> post: </b> se ha agregado una función que entra como parámetro
	 * @param funcion -funcion a agregar. funcion != null
	 * @throws Exception - cualquier error que se genera agregando la funcion
	 */
	public void addPreferencia(Preferencia preferencia, Long id) throws Exception {
		DAOTablaPreferencia daoFestival = new DAOTablaPreferencia();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			daoFestival.agregarPreferencia(preferencia, id);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
	}

	/**
	 * Método que modela la transacción que agrega una sola función a la base de datos.
	 * <b> post: </b> se ha agregado una función que entra como parámetro
	 * @param funcion -funcion a agregar. funcion != null
	 * @throws Exception - cualquier error que se genera agregando la funcion
	 */
	public Preferencia deletePreferencia(Long id, Long idP) throws Exception {
		DAOTablaPreferencia daoFestival = new DAOTablaPreferencia();
		Preferencia prefe = null;
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			prefe = daoFestival.buscarPreferencia(idP);
			daoFestival.deletePreferencia(prefe, id);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return prefe;
	}

	/**
	 * Método que modela la transacción que agrega una sola función a la base de datos.
	 * <b> post: </b> se ha agregado una función que entra como parámetro
	 * @param funcion -funcion a agregar. funcion != null
	 * @throws Exception - cualquier error que se genera agregando la funcion
	 */
	public void updatePreferencia(Preferencia preferencia, Long id) throws Exception {
		DAOTablaPreferencia daoFestival = new DAOTablaPreferencia();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			daoFestival.updatePreferencia(preferencia);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
	}



	public ListaInformacion generarReporteDeUnaFuncion (String idFuncion) throws SQLException
	{
		ArrayList<InformacionVentaLocalidad> informacion;
		DAOTablaFestival daoFestival = new DAOTablaFestival();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			informacion = daoFestival.generarReporteDeUnaFuncion(idFuncion);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return new ListaInformacion(informacion);
	}

	public ListaFuncionesCompania generarReporteDeUnaCompania (String idCompania) throws SQLException
	{
		ArrayList<FuncionCompania> funciones;
		DAOTablaFestival daoFestival = new DAOTablaFestival();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			funciones = daoFestival.darReporteCompania(idCompania);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return new ListaFuncionesCompania(Long.parseLong(idCompania), funciones);
	}

	public ListaFuncionesCompania generarReporteDeUnaCompaniaParaCliente (String idCompania, String idCliente) throws SQLException
	{
		ArrayList<FuncionCompania> funciones;
		DAOTablaFestival daoFestival = new DAOTablaFestival();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			funciones = daoFestival.darReporteCompaniaParaCliente(idCompania,idCliente);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return new ListaFuncionesCompania(Long.parseLong(idCompania), funciones);
	}


	public ListaRespuestaAsistencia generarReporteAsistenciaCliente(String idCliente) throws SQLException
	{
		ListaPorRealizacion realizadas;
		ListaPorRealizacion noRealizadas;
		ListaPorEstado activasRealizadas;
		ListaPorEstado activasNoRealizadas;
		ListaPorEstado devueltasRealizadas;
		ListaPorEstado devueltasNoRealizadas;
		ArrayList<ArrayList<FuncionRespuestaCliente>> resp;

		DAOTablaFestival daoFestival = new DAOTablaFestival();

		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			resp = daoFestival.generarReporteAsistenciaCliente(idCliente);
			activasRealizadas = new ListaPorEstado(resp.get(0));
			System.out.println("ar" + resp.get(0).isEmpty());
			activasNoRealizadas = new ListaPorEstado(resp.get(1));
			System.out.println("anr" + resp.get(1).isEmpty());
			devueltasRealizadas = new ListaPorEstado(resp.get(2));
			System.out.println("dr" + resp.get(2).isEmpty());
			devueltasNoRealizadas = new ListaPorEstado(resp.get(3));
			System.out.println("dnr" + resp.get(3).isEmpty());
			realizadas = new ListaPorRealizacion(activasRealizadas, devueltasRealizadas);
			noRealizadas = new ListaPorRealizacion(activasNoRealizadas, devueltasNoRealizadas);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return new ListaRespuestaAsistencia(realizadas, noRealizadas);
	}


	/**
	 * Método que modela la transacción que agrega una sola función a la base de datos.
	 * <b> post: </b> se ha agregado una función que entra como parámetro
	 * @param funcion -funcion a agregar. funcion != null
	 * @throws Exception - cualquier error que se genera agregando la funcion
	 */


	public ListaInformacionFuncion generarReporteDeUnEspectaculo(String idEspectaculo) throws SQLException
	{
		ArrayList<InformacionVentaFuncion> informacion;
		DAOTablaFestival daoFestival = new DAOTablaFestival();

		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			informacion = daoFestival.generarReporteDeUnEspectaculo(idEspectaculo);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return new ListaInformacionFuncion(informacion);

	}


	public VOBoleta inicVenderBoleta(Long idFuncion, Long idSilla, Long idCliente, Long idAbonamiento) throws Exception {

		VOBoleta boleta = null;
		try{

			this.conn = darConexion();
			boleta = venderBoleta(idFuncion, idSilla, idCliente, idAbonamiento);
			conn.commit();
		}
		catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {

				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return boleta;


	}

	public VOBoleta venderBoleta(Long idFuncion, Long	 idSilla, Long idCliente, Long idAbonamiento) throws Exception {
		DAOTablaBoleta daoFestival = new DAOTablaBoleta();
		VOBoleta boleta = null;
		try 
		{
			//////Transacción
			//this.conn = darConexion();
			daoFestival.setConn(conn);
			boleta = daoFestival.venderBoleta(idFuncion, idSilla, idCliente, idAbonamiento);
			//conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			//			try {
			daoFestival.cerrarRecursos();
			//				//if(this.conn!=null)
			//					//this.conn.close();
			//			} catch (SQLException exception) {
			//				System.err.println("SQLException closing resources:" + exception.getMessage());
			//				exception.printStackTrace();
			//				throw exception;
			//			}
		}
		return boleta;
	}

	public VOBoleta verificarLocalidadSillas( Long idSilla, int cantidad) throws Exception {
		DAOTablaSilla daoFestival = new DAOTablaSilla();
		VOBoleta boleta = null;
		try 
		{
			//////Transacción
			int cont = 0;
			this.conn = darConexion();
			daoFestival.setConn(conn);
			Silla primera = daoFestival.buscarSilla(idSilla);
			Long localidad = primera.getIdLocalidad();	
			for(int i = 1; i<=cantidad; i++){
				Silla temp = daoFestival.buscarSilla(idSilla+i);
				if(temp.getIdLocalidad()!=localidad)
					throw new SQLException("Las Sillas no son de la misma localidad");
			}
			conn.commit();
			//conn.setSavepoint(cont+"");
			cont++;

		} catch (SQLException e) {
			//conn.rollback();
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return boleta;
	}
	public ArrayList<VOBoleta> venderMultiplesBoleta(Long idFuncion, Long idSilla, Long idCliente, int cantidad) throws Exception {
		DAOTablaFestival daoFestival = new DAOTablaFestival();
		ArrayList<VOBoleta> listaBoleta = new ArrayList<>();
		try 
		{
			//////Transacción

			this.conn = darConexion();
			conn.setAutoCommit(false);
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			daoFestival.setConn(conn);
			conn.setSavepoint();
			for(int i = 0; i<cantidad;i++){
				listaBoleta.add(venderBoletasVarias(idFuncion, idSilla+i, idCliente));

			}
			conn.commit();
			conn.setAutoCommit(true);

		} catch (SQLException e) {
			conn.rollback();
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return listaBoleta;
	}

	public VOBoleta venderBoletasVarias(Long idFuncion, Long idSilla, Long idCliente) throws SQLException
	{
		DAOTablaBoleta daoFestival = new DAOTablaBoleta();
		VOBoleta boleta = null;
		try 
		{
			//////Transacción
			//this.conn = darConexion();
			daoFestival.setConn(conn);
			boleta = daoFestival.venderBoleta(idFuncion, idSilla, idCliente,(long)0);

		} catch (SQLException e) {
			//conn.rollback();
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		return boleta;
	}

	/**
	 * Método que modela la transacción que agrega una sola función a la base de datos.
	 * <b> post: </b> se ha agregado una función que entra como parámetro
	 * @param funcion -funcion a agregar. funcion != null
	 * @throws Exception - cualquier error que se genera agregando la funcion
	 */
	public Funcion marcarRealizadaFuncion(Long idFuncion) throws Exception {
		DAOTablaFuncion daoFuncion = new DAOTablaFuncion();
		Funcion funcion = null;
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFuncion.setConn(conn);

			funcion = daoFuncion.marcarRealizada(idFuncion);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFuncion.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return funcion;
	}


	public void addEspectaculo(Espectaculo espec) throws Exception {
		DAOTablaEspectaculo daoFestival = new DAOTablaEspectaculo();
		Funcion funcion = null;
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);

			daoFestival.registrarEspectaculo(espec);
			conn.commit();
		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
	}

	public Sitio darSitioConsulta(String idSitio) throws SQLException
	{
		Sitio aux;
		DAOTablaFestival daoFestival = new DAOTablaFestival();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);

			aux = daoFestival.darSitioConsulta(idSitio);

			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return aux;
	}

	public ListaLocalidades darLocalidadesSitio( String idSitio) throws SQLException
	{
		ArrayList<Localidad> localidades;
		DAOTablaFestival daoFestival = new DAOTablaFestival();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			localidades = daoFestival.darLocalidadesSitio(idSitio);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return new ListaLocalidades(localidades);
	}

	public ListaFuncioneSitio darFuncionesSitio( String idSitio) throws SQLException
	{
		ArrayList<InformacionFuncionSitio> funciones;
		DAOTablaFestival daoFestival = new DAOTablaFestival();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			funciones = daoFestival.darFuncionesSitio(idSitio);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return new ListaFuncioneSitio(funciones);

	}

	public List<Rentabilidad> darRentabilidad( Rentabilidad rent) throws SQLException
	{
		ArrayList<Rentabilidad> rentabilidad;
		DAOTablaFestival daoFestival = new DAOTablaFestival();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			rentabilidad = daoFestival.darRentabilidad(rent);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return rentabilidad;

	}

	public List<Rentabilidad> darRentabilidadCompania( Rentabilidad rent, Long idCompania) throws SQLException
	{
		ArrayList<Rentabilidad> rentabilidad;
		DAOTablaFestival daoFestival = new DAOTablaFestival();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			rentabilidad = daoFestival.darRentabilidadCompania(rent,idCompania);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return rentabilidad;

	}


	public List<MasPopuEspectaculo> darMasPopuEspec( Rentabilidad rent) throws SQLException
	{
		ArrayList<MasPopuEspectaculo> masPopuEspectaculo;
		DAOTablaFestival daoFestival = new DAOTablaFestival();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			masPopuEspectaculo = daoFestival.darMasPopuEspec(rent);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return masPopuEspectaculo;

	}

	public NotaDebito inicActualizarDevBoleta(Long idUsuario, Long idBoleta, String fecha) throws SQLException{

		NotaDebito notaDebito =null;
		try {
			this.conn = darConexion();
			notaDebito = actualizarDevBoleta(idUsuario, idBoleta, fecha);

			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			try {
				throw e;
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}finally {
			try {
				if(this.conn!=null);
				this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return notaDebito;

	}

	public NotaDebito actualizarDevBoleta(Long idUsuario, Long idBoleta, String fecha) throws SQLException
	{
		DAOTablaFestival daoFestival = new DAOTablaFestival();
		NotaDebito notaDebito =null;
		try 
		{
			//////Transacción
			//this.conn = darConexion();
			daoFestival.setConn(conn);
			notaDebito= daoFestival.actualizarDevBoleta(idBoleta, idUsuario, fecha);
			conn.commit();
			//			daoFestival.borrarBoletasDevueltas();
			//conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			try {
				throw e;
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} finally {
			//			try {
			daoFestival.cerrarRecursos();
			//				if(this.conn!=null);
			//					//this.conn.close();
			//			} catch (SQLException exception) {
			//				System.err.println("SQLException closing resources:" + exception.getMessage());
			//				exception.printStackTrace();
			//				throw exception;
			//			}
		}
		return notaDebito;

	}

	public ArrayList<VOBoleta> crearAbonamiento(Long idUsuario, VOAbonamiento abonamiento) {
		// TODO Auto-generated method stub
		DAOTablaFestival daoFestival = new DAOTablaFestival();
		ArrayList<VOBoleta> abonamientoList = new ArrayList<>();
		try 
		{
			//////Transacción
			this.conn = darConexion();
			conn.setAutoCommit(false);
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			daoFestival.setConn(conn);
			conn.setSavepoint();
			Long idAbonamiento = daoFestival.verificarFecha(abonamiento.getFechaConsulta());
			if(idAbonamiento==0)
				throw new SQLException("La fecha no es antes de 3 semanas al inicio del festival");

			ArrayList<Long> idFunciones = abonamiento.getIdsFunciones();
			int i = 0;
			for(Long idSillas: daoFestival.verificarSitioLocalidad(abonamiento)){
				VOBoleta boleta = venderBoleta(idFunciones.get(i), idSillas, idUsuario, idAbonamiento);
				boleta.setPrecio((int)(boleta.getPrecio()*0.8));
				boleta.setIdAbonamiento(idAbonamiento);
				abonamientoList.add(boleta);
				i++;
			}
			conn.setAutoCommit(true);
			conn.commit();

		} catch (SQLException e) {

			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			try {
				conn.rollback();
				throw e;
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			try {
				throw e;
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} finally {
			try {
				conn.setAutoCommit(true);
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				try {
					throw exception;
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return abonamientoList;
	}

	public ArrayList<NotaDebito> devolverAbonamiento(Long idUsuario, Long idAbonamiento, String fecha) {
		// TODO Auto-generated method stub
		DAOTablaFestival daoFestival = new DAOTablaFestival();
		ArrayList<Long> idsBoletas;
		ArrayList<NotaDebito> notasDebito = new ArrayList<>();
		try {
			this.conn = darConexion();
			conn.setAutoCommit(false);
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			daoFestival.setConn(conn);
			conn.setSavepoint();
			idsBoletas = daoFestival.obtenerIdBoletaAbonados(idAbonamiento);
			for(Long idBoleta: idsBoletas)
			{
				notasDebito.add(actualizarDevBoleta(idUsuario, idBoleta, fecha));

			}
			conn.setAutoCommit(true);
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			try {
				conn.rollback();
				throw e;
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			try {
				throw e;
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} finally {
			try {
				conn.setAutoCommit(true);
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				try {
					throw exception;
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		return notasDebito;
	}


	public ArrayList<NotaDebito> devolverBoletasFunCancel(Long idFuncion, String fecha) {
		// TODO Auto-generated method stub
		DAOTablaFestival daoFestival = new DAOTablaFestival();
		DAOTablaFuncion daoFuncion = new DAOTablaFuncion();
		daoFuncion.setConn(conn);
		ArrayList<String> idsBoletas;
		ArrayList<NotaDebito> notasDebito = new ArrayList<>();
		try {
			this.conn = darConexion();
			conn.setAutoCommit(false);
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			daoFestival.setConn(conn);
			conn.setSavepoint();
			idsBoletas = daoFestival.obtenerIdBoletaFunCancelada(idFuncion);
			for(String idBoletaCliente: idsBoletas)
			{
				Long idBoleta = Long.parseLong(idBoletaCliente.split(",")[0]);
				Long idUsuario = Long.parseLong(idBoletaCliente.split(",")[1]);
				notasDebito.add(actualizarDevBoleta(idUsuario, idBoleta, fecha));

			}
			daoFuncion.cancelarFuncion(idFuncion);
			conn.setAutoCommit(true);
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			try {
				conn.rollback();
				throw e;
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			try {
				throw e;
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} finally {
			try {
				conn.setAutoCommit(true);
				daoFuncion.cerrarRecursos();
				daoFestival.cerrarRecursos();

				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				try {
					throw exception;
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		return notasDebito;
	}

	public boolean cancelarFuncion(Long idFuncion) throws SQLException
	{
		DAOTablaEspectaculo daoEspectaculo = new DAOTablaEspectaculo();
		Boolean borro;
		try{

			this.conn = darConexion();
			daoEspectaculo.setConn(conn);
			borro = daoEspectaculo.cancelarFuncion(idFuncion);
		}
		catch(SQLException e)
		{
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoEspectaculo.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return borro;
	}



	public ListaUsuarios siAsistieron(Long idCompania) throws SQLException
	{
		ArrayList<Usuario> resp;

		DAOTablaCompania daoCompania = new DAOTablaCompania();

		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoCompania.setConn(conn);
			resp = daoCompania.siAsistieron(idCompania);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoCompania.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return new ListaUsuarios(idCompania, resp);
	}

	public ListaUsuarios noAsistieron(Long idCompania) throws SQLException
	{
		ArrayList<Usuario> resp;

		DAOTablaCompania daoCompania = new DAOTablaCompania();

		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoCompania.setConn(conn);
			resp = daoCompania.noAsistieron(idCompania);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoCompania.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return new ListaUsuarios(idCompania, resp);
	}

	public ListaBuenosClientes buenosClientes(Long numBoletas) throws SQLException
	{
		ArrayList<Usuario> resp;

		DAOTablaFestival daoFestival = new DAOTablaFestival();

		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			resp = daoFestival.buenosClientes(numBoletas);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return new ListaBuenosClientes(numBoletas, resp);
	}

	public ArrayList<BoletasCompradas> consultaBoletasFecha(String fechaInicial, String fechaFinal) throws SQLException {
		DAOTablaFestival daoFestival = new DAOTablaFestival();
		ArrayList<BoletasCompradas> boletasCompradas;
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			boletasCompradas = daoFestival.consultaBoletasFecha(fechaInicial, fechaFinal);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return boletasCompradas;
	}

	public ArrayList<BoletasCompradas> consultaBoletasRequerimiento(Long id) throws SQLException {
		// TODO Auto-generated method stub
		DAOTablaFestival daoFestival = new DAOTablaFestival();
		ArrayList<BoletasCompradas> boletasCompradas;
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			boletasCompradas = daoFestival.consultaRequerimiento(id);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return boletasCompradas;
	}

	public ArrayList<BoletasCompradas> consultaBoletasLocalidad(String nombre) throws SQLException {
		// TODO Auto-generated method stub
		DAOTablaFestival daoFestival = new DAOTablaFestival();
		ArrayList<BoletasCompradas> boletasCompradas;
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			boletasCompradas = daoFestival.consultaLocalidad(nombre);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return boletasCompradas;
	}

	public ArrayList<BoletasCompradas> consultaBoletasFranjaHoraria(int ini, int fin) throws SQLException {
		// TODO Auto-generated method stub
		DAOTablaFestival daoFestival = new DAOTablaFestival();
		ArrayList<BoletasCompradas> boletasCompradas;
		try 
		{
			//////Transacción
			this.conn = darConexion();
			daoFestival.setConn(conn);
			boletasCompradas = daoFestival.consultaFranjaHoraria(ini, fin);
			conn.commit();

		} catch (SQLException e) {
			System.err.println("SQLException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			System.err.println("GeneralException:" + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			try {
				daoFestival.cerrarRecursos();
				if(this.conn!=null)
					this.conn.close();
			} catch (SQLException exception) {
				System.err.println("SQLException closing resources:" + exception.getMessage());
				exception.printStackTrace();
				throw exception;
			}
		}
		return boletasCompradas;
	}

	public ArrayList<VOBoleta> crearAbonamientoGlobal(Long idUsuario, VOAbonamiento abonamiento) throws NonReplyException {
		// TODO Auto-generated method stub
		//LOCAL
		ArrayList<VOBoleta> abonamientos = crearAbonamiento(idUsuario, abonamiento);
		abonamiento.setIdUsuario(idUsuario);
		//GLOBAL
		ArrayList<VOBoleta> abonamientosGlobal = dtm.getAbonamientosGlobal(abonamiento);
		abonamientos.addAll(abonamientosGlobal);
		return abonamientos;
	}

}
