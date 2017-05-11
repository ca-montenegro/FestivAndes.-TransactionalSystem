package vos;

public class BoletasCompradas {

	private Long id_boleta;
	
	private Long id_localidad;
	
	private int numerosilla;
	
	private String nombreLocalidad;
	
	private Long idSitio;
	
	private double precio;
	
	private char silla_numerada;
	
	private Long id_funcion;
	
	private Long id_cliente;
	
	private Long id_abonamiento;
	
	private char estado;
	
	private String fecha;
	
	private int hora;
	
	private char realizada;
	
	private long id_espec;
	
	private String nombre;
	
	private double duracion;
	
	private String idioma;
	
	private double costo;
	
	private String descripcion;
	
	private char servicio_tradu;
	
	private String publico_objetivo;
	
	private long id_compania;
	
	private long id_operario;
	
	private long id_req;
	
	private String descripcion_1;

	public BoletasCompradas(Long id_boleta, Long id_localidad, int numerosilla, String nombreLocalidad, Long idSitio,
			double precio, char silla_numerada, Long id_funcion, Long id_cliente, Long id_abonamiento, char estado,
			String fecha, int hora, char realizada, long id_espec, String nombre, double duracion, String idioma,
			double costo, String descripcion, char servicio_tradu, String publico_objetivo, long id_compania,
			long id_operario, long id_req, String descripcion_1) {
		this.id_boleta = id_boleta;
		this.id_localidad = id_localidad;
		this.numerosilla = numerosilla;
		this.nombreLocalidad = nombreLocalidad;
		this.idSitio = idSitio;
		this.precio = precio;
		this.silla_numerada = silla_numerada;
		this.id_funcion = id_funcion;
		this.id_cliente = id_cliente;
		this.id_abonamiento = id_abonamiento;
		this.estado = estado;
		this.fecha = fecha;
		this.hora = hora;
		this.realizada = realizada;
		this.id_espec = id_espec;
		this.nombre = nombre;
		this.duracion = duracion;
		this.idioma = idioma;
		this.costo = costo;
		this.descripcion = descripcion;
		this.servicio_tradu = servicio_tradu;
		this.publico_objetivo = publico_objetivo;
		this.id_compania = id_compania;
		this.id_operario = id_operario;
		this.id_req = id_req;
		this.descripcion_1 = descripcion_1;
	}

	public Long getId_boleta() {
		return id_boleta;
	}

	public void setId_boleta(Long id_boleta) {
		this.id_boleta = id_boleta;
	}

	public Long getId_localidad() {
		return id_localidad;
	}

	public void setId_localidad(Long id_localidad) {
		this.id_localidad = id_localidad;
	}

	public int getNumerosilla() {
		return numerosilla;
	}

	public void setNumerosilla(int numerosilla) {
		this.numerosilla = numerosilla;
	}

	public String getNombreLocalidad() {
		return nombreLocalidad;
	}

	public void setNombreLocalidad(String nombreLocalidad) {
		this.nombreLocalidad = nombreLocalidad;
	}

	public Long getIdSitio() {
		return idSitio;
	}

	public void setIdSitio(Long idSitio) {
		this.idSitio = idSitio;
	}

	public double getPrecio() {
		return precio;
	}

	public void setPrecio(double precio) {
		this.precio = precio;
	}

	public char getSilla_numerada() {
		return silla_numerada;
	}

	public void setSilla_numerada(char silla_numerada) {
		this.silla_numerada = silla_numerada;
	}

	public Long getId_funcion() {
		return id_funcion;
	}

	public void setId_funcion(Long id_funcion) {
		this.id_funcion = id_funcion;
	}

	public Long getId_cliente() {
		return id_cliente;
	}

	public void setId_cliente(Long id_cliente) {
		this.id_cliente = id_cliente;
	}

	public Long getId_abonamiento() {
		return id_abonamiento;
	}

	public void setId_abonamiento(Long id_abonamiento) {
		this.id_abonamiento = id_abonamiento;
	}

	public char getEstado() {
		return estado;
	}

	public void setEstado(char estado) {
		this.estado = estado;
	}

	public String getFecha() {
		return fecha;
	}

	public void setFecha(String fecha) {
		this.fecha = fecha;
	}

	public int getHora() {
		return hora;
	}

	public void setHora(int hora) {
		this.hora = hora;
	}

	public char getRealizada() {
		return realizada;
	}

	public void setRealizada(char realizada) {
		this.realizada = realizada;
	}

	public long getId_espec() {
		return id_espec;
	}

	public void setId_espec(long id_espec) {
		this.id_espec = id_espec;
	}

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre;
	}

	public double getDuracion() {
		return duracion;
	}

	public void setDuracion(double duracion) {
		this.duracion = duracion;
	}

	public String getIdioma() {
		return idioma;
	}

	public void setIdioma(String idioma) {
		this.idioma = idioma;
	}

	public double getCosto() {
		return costo;
	}

	public void setCosto(double costo) {
		this.costo = costo;
	}

	public String getDescripcion() {
		return descripcion;
	}

	public void setDescripcion(String descripcion) {
		this.descripcion = descripcion;
	}

	public char getServicio_tradu() {
		return servicio_tradu;
	}

	public void setServicio_tradu(char servicio_tradu) {
		this.servicio_tradu = servicio_tradu;
	}

	public String getPublico_objetivo() {
		return publico_objetivo;
	}

	public void setPublico_objetivo(String publico_objetivo) {
		this.publico_objetivo = publico_objetivo;
	}

	public long getId_compania() {
		return id_compania;
	}

	public void setId_compania(long id_compania) {
		this.id_compania = id_compania;
	}

	public long getId_operario() {
		return id_operario;
	}

	public void setId_operario(long id_operario) {
		this.id_operario = id_operario;
	}

	public long getId_req() {
		return id_req;
	}

	public void setId_req(long id_req) {
		this.id_req = id_req;
	}

	public String getDescripcion_1() {
		return descripcion_1;
	}

	public void setDescripcion_1(String descripcion_1) {
		this.descripcion_1 = descripcion_1;
	}
	
	public String toString()
	{
		return "nombre: " + nombre + ". "
				+ "fecha funcion: " + fecha+ ". "
						+ "sitio función: " + idSitio +". "
								+ "franja horaria: " + hora + ". "
										+ "localidad: " + nombreLocalidad + ". "
								+ "conteo: " + id_espec + ". \n";
		
	}
	
	
}
