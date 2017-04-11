package vos;

import org.codehaus.jackson.annotate.JsonProperty;

public class Compania {

	@JsonProperty(value = "id")
	private Long id;
	
	@JsonProperty(value = "nombre")
	private String nombre;
	
	@JsonProperty(value = "representante")
	private String representante;
	
	@JsonProperty(value = "paisOrigen")
	private String paisOrigen;
	
	@JsonProperty(value = "paginaWeb")
	private String paginaWeb;
	
	@JsonProperty(value = "fechaLlegada")
	private String fechaLlegada;
	
	@JsonProperty(value = "fechaSalida")
	private String fechaSalida;
	
	@JsonProperty(value = "idFestival")
	private Long idFestival;
	
	@JsonProperty(value = "idOrganizador")
    private Long idOrganizador;
	
	private Festival festival;
	
	private Organizador organizador;
	
	public Compania(@JsonProperty(value="id")Long id, 
			@JsonProperty(value="nombre")String nombre,
			@JsonProperty(value="representante") String representante, 
			@JsonProperty(value = "paisOrigen") String paisOrigen,
			@JsonProperty(value = "paginaWeb") String paginaWeb, 
			@JsonProperty(value = "fechaLlegada") String fechaLlegada,
			@JsonProperty(value = "fechaSalida") String fechaSalida, 
			@JsonProperty(value = "idFestival") Long idFestival,
			@JsonProperty(value = "idOrganizador") Long idOrganizador)
	{
		this.id=id;
		this.nombre = nombre;
		this.representante = representante;
		this.paisOrigen = paisOrigen;
		this.paginaWeb = paginaWeb;
		this.fechaLlegada = fechaLlegada;
		this.fechaSalida = fechaSalida;
		this.idFestival = idFestival;
		this.idOrganizador = idOrganizador;
	}

	public Festival getFestival() {
		return festival;
	}

	public void setFestival(Festival festival) {
		this.festival = festival;
	}

	public Organizador getOrganizador() {
		return organizador;
	}

	public void setOrganizador(Organizador organizador) {
		this.organizador = organizador;
	}

	public Long getId() {
		return id;
	}

	public Long getIdFestival() {
		return idFestival;
	}

	public void setIdFestival(Long idFestival) {
		this.idFestival = idFestival;
	}

	public Long getIdOrganizador() {
		return idOrganizador;
	}

	public void setIdOrganizador(Long idOrganizador) {
		this.idOrganizador = idOrganizador;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre;
	}

	public String getRepresentante() {
		return representante;
	}

	public void setRepresentante(String representante) {
		this.representante = representante;
	}

	public String getPaisOrigen() {
		return paisOrigen;
	}

	public void setPaisOrigen(String paisOrigen) {
		this.paisOrigen = paisOrigen;
	}

	public String getPaginaWeb() {
		return paginaWeb;
	}

	public void setPaginaWeb(String paginaWeb) {
		this.paginaWeb = paginaWeb;
	}

	public String getFechaLlegada() {
		return fechaLlegada;
	}

	public void setFechaLlegada(String fechaLlegada) {
		this.fechaLlegada = fechaLlegada;
	}

	public String getFechaSalida() {
		return fechaSalida;
	}

	public void setFechaSalida(String fechaSalida) {
		this.fechaSalida = fechaSalida;
	}
	
	
}
