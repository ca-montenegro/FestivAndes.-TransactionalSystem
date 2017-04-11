package vos;

import java.util.ArrayList;

import org.codehaus.jackson.annotate.JsonProperty;

public class Silla {
	
	@JsonProperty(value="id")
	private Long id;
	
	
	private Localidad localidad;
	
	@JsonProperty(value="numero")
	private int numero;
	
	@JsonProperty(value="idLocalidad")
	private Long idLocalidad;
	
	public Silla( @JsonProperty(value="id")Long id, @JsonProperty(value="idLocalidad")Long idLocalidad, @JsonProperty(value="numero")int numero)
	{
		this.id = id;
		this.idLocalidad = idLocalidad;
		this.numero = numero;
	}

	
	public Long getIdLocalidad() {
		return idLocalidad;
	}


	public void setIdLocalidad(Long idLocalidad) {
		this.idLocalidad = idLocalidad;
	}


	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}


	public Localidad getLocalidad() {
		return localidad;
	}

	public void setLocalidad(Localidad localidad) {
		this.localidad = localidad;
	}

	public int getNumero() {
		return numero;
	}

	public void setNumero(int numero) {
		this.numero = numero;
	}
	

}
