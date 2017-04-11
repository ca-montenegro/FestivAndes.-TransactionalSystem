package vos;

import java.util.ArrayList;

import org.codehaus.jackson.annotate.JsonProperty;

public class Requerimiento {
	
	@JsonProperty(value="id")
	private Long id;
	
	@JsonProperty(value="nombre")
	private String nombre;
	
	/**
	 * Espectaculos que tienen este requerimiento
	 */
	private ArrayList<Espectaculo> espectaculos;
	
	
	public Requerimiento (@JsonProperty(value="id")Long id, @JsonProperty(value="nombre")String nombre)
	{
		this.id = id;
		this.nombre = nombre;
		espectaculos = new ArrayList<Espectaculo>();
	}

	
	public void addEspectaculo(Espectaculo espectaculo)
	{
		espectaculos.add(espectaculo);
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
	

}
