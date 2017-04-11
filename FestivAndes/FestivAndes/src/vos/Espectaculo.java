package vos;

import java.util.ArrayList;

import org.codehaus.jackson.annotate.JsonProperty;

public class Espectaculo {
	
	@JsonProperty(value="id")
	public Long id;
	
	@JsonProperty(value="nombre")
	private String nombre;
	
	@JsonProperty(value="duracion")
	private int duracion;
	
	@JsonProperty(value="idioma")
	private String idioma;
	
	@JsonProperty(value="value")
	private int costo;
	
	@JsonProperty(value="descripcion")
	private String descripcion;
	
	@JsonProperty(value="servicioTraduccion")
	private char servicioTraduccion;
	
	@JsonProperty(value="participacion")
	private char participacion;
	
	private ArrayList<Elemento> elementos;

	@JsonProperty(value="idCompania")
	private Long idCompania;
	
	@JsonProperty(value="idOperario")
	private Long idOperario;
	
	private Compania compania;
	
	private Operario operario;
	
	private Categoria categoria;
	
	private ArrayList<Requerimiento> requerimientos;
	
	@JsonProperty(value="publicoObjetivo")
	private String publicoObjetivo;

	public Espectaculo(@JsonProperty(value="id")Long id, 
			@JsonProperty(value="nombre")String nombre, 
			@JsonProperty(value="duracion")int duracion, 
			@JsonProperty(value="idioma")String idioma, 
			@JsonProperty(value="costo")int costo, 
			@JsonProperty(value="descripcion")String descripcion,
			@JsonProperty(value="servicioTraduccion")char servicioTraduccion, 
			@JsonProperty(value="participacion") char participacion, 
			@JsonProperty(value="publicoObjetivo")String publicoObjetivo, 
			@JsonProperty(value="idCompania")Long idCompania,
			@JsonProperty(value="idOperario")Long idOperario)
			
	{
		this.id = id;
		this.nombre = nombre;
		this.duracion = duracion;
		this.idioma = idioma;
		this.costo = costo;
		this.descripcion = descripcion;
		this.servicioTraduccion = servicioTraduccion;
		this.participacion = participacion;
		this.idCompania = idCompania;
		this.idOperario = idOperario;
		this.publicoObjetivo = publicoObjetivo;
		elementos = new ArrayList<Elemento>();
		requerimientos = new ArrayList<Requerimiento>();	
	}

	public Long getId() {
		return id;
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

	public ArrayList<Elemento> getElementos() {
		return elementos;
	}

	public void setElementos(ArrayList<Elemento> elementos) {
		this.elementos = elementos;
	}

	public Compania getCompania() {
		return compania;
	}

	public void setCompania(Compania compania) {
		this.compania = compania;
	}

	public Operario getOperario() {
		return operario;
	}

	public void setOperario(Operario operario) {
		this.operario = operario;
	}

	public Categoria getCategoria() {
		return categoria;
	}

	public void setCategoria(Categoria categoria) {
		this.categoria = categoria;
	}

	public ArrayList<Requerimiento> getRequerimientos() {
		return requerimientos;
	}

	public void setRequerimientos(ArrayList<Requerimiento> requerimientos) {
		this.requerimientos = requerimientos;
	}

	public String getPublicoObjetivo() {
		return publicoObjetivo;
	}

	public void setPublicoObjetivo(String publicoObjetivo) {
		this.publicoObjetivo = publicoObjetivo;
	}
	
	public void addElemento( Elemento elemento)
	{
		elementos.add(elemento);
	}
	
	public void addRequerimiento(Requerimiento requerimiento)
	{
		requerimientos.add(requerimiento);
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
	
}
