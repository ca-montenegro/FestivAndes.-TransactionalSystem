package vos;

import org.codehaus.jackson.annotate.JsonProperty;

public class Sitio {

	@JsonProperty(value="id")
	private Long id;
	
	@JsonProperty(value="tipo")
	private char tipo;
	
	@JsonProperty(value="capacidad")
	private int capacidad;
	
	@JsonProperty(value="disponibilidad")
	private String disponibilidad;
	
	@JsonProperty(value="proteAtmosfera")
	private char proteAtmosfera;
	
	

	public Sitio(@JsonProperty(value="id") Long id, 
			@JsonProperty(value="tipo") char tipo,
			@JsonProperty(value="capacidad") int capacidad,
			@JsonProperty(value="disponibilidad") String disponibilidad,  
			@JsonProperty(value="proteAtmosfera") char proteAtmosfera) 
	{
		this.id = id;
		this.tipo = tipo;
		this.capacidad = capacidad;
		this.disponibilidad = disponibilidad;
		this.proteAtmosfera = proteAtmosfera;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public char getTipo() {
		return tipo;
	}

	public void setTipo(char tipo) {
		this.tipo = tipo;
	}

	public int getCapacidad() {
		return capacidad;
	}

	public void setCapacidad(int capacidad) {
		this.capacidad = capacidad;
	}

	public String getDisponibilidad() {
		return disponibilidad;
	}

	public void setDisponibilidad(String disponibilidad) {
		this.disponibilidad = disponibilidad;
	}

	public char getProteAtmosfera() {
		return proteAtmosfera;
	}

	public void setProteAtmosfera(char proteAtmosfera) {
		this.proteAtmosfera = proteAtmosfera;
	}
	
	
}
