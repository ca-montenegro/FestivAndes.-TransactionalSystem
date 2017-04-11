package vos;

import org.codehaus.jackson.annotate.JsonProperty;

public class MasPopuEspectaculo {
	
	@JsonProperty(value="idEspec")
	public Long idEspec;
	
	@JsonProperty(value="nombre")
	private String nombre;
	
	@JsonProperty(value="duracion")
	private int duracion;
	
	@JsonProperty(value="idioma")
	private String idioma;
	
	@JsonProperty(value="costo")
	private int costo;
	
	@JsonProperty(value="descripcion")
	private String descripcion;
	
	@JsonProperty(value="servicioTraduccion")
	private char servicioTraduccion;
	
	@JsonProperty(value="participacion")
	private char participacion;
	

	@JsonProperty(value="idCompania")
	private Long idCompania;
	
	@JsonProperty(value="idOperario")
	private Long idOperario;
	
	@JsonProperty(value="totalAsistentes")
	private int totalAsistentes;
	
	public MasPopuEspectaculo(@JsonProperty(value="idEspec")Long idEspec, 
			@JsonProperty(value="nombre")String nombre, 
			@JsonProperty(value="duracion")int duracion, 
			@JsonProperty(value="idioma")String idioma, 
			@JsonProperty(value="costo")int costo, 
			@JsonProperty(value="descripcion")String descripcion,
			@JsonProperty(value="servicioTraduccion")char servicioTraduccion, 
			@JsonProperty(value="participacion") char participacion, 
			@JsonProperty(value="idCompania")Long idCompania,
			@JsonProperty(value="idOperario")Long idOperario,
			@JsonProperty(value="totalAsistentes") int totalAsistentes)
			
	{
		this.idEspec = idEspec;
		this.nombre = nombre;
		this.duracion = duracion;
		this.idioma = idioma;
		this.costo = costo;
		this.descripcion = descripcion;
		this.servicioTraduccion = servicioTraduccion;
		this.participacion = participacion;
		this.idCompania = idCompania;
		this.idOperario = idOperario;
		this.totalAsistentes = totalAsistentes;
	}

	public Long getIdEspec() {
		return idEspec;
	}

	public void setIdEspec(Long id) {
		this.idEspec = id;
	}

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre;
	}

	public int getDuracion() {
		return duracion;
	}

	public void setDuracion(int duracion) {
		this.duracion = duracion;
	}

	public String getIdioma() {
		return idioma;
	}

	public void setIdioma(String idioma) {
		this.idioma = idioma;
	}

	public int getCosto() {
		return costo;
	}

	public void setCosto(int costo) {
		this.costo = costo;
	}

	public String getDescripcion() {
		return descripcion;
	}

	public void setDescripcion(String descripcion) {
		this.descripcion = descripcion;
	}

	public char getServicioTraduccion() {
		return servicioTraduccion;
	}

	public void setServicioTraduccion(char servicioTraduccion) {
		this.servicioTraduccion = servicioTraduccion;
	}

	public char getParticipacion() {
		return participacion;
	}

	public void setParticipacion(char participacion) {
		this.participacion = participacion;
	}

	public Long getIdCompania() {
		return idCompania;
	}

	public void setIdCompania(Long idCompania) {
		this.idCompania = idCompania;
	}

	public Long getIdOperario() {
		return idOperario;
	}

	public void setIdOperario(Long idOperario) {
		this.idOperario = idOperario;
	}

	public int getTotalAsistentes() {
		return totalAsistentes;
	}

	public void setTotalAsistentes(int totalAsistentes) {
		this.totalAsistentes = totalAsistentes;
	}
	
	
	
}
