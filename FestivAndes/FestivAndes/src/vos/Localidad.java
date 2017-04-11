package vos;

import java.util.ArrayList;

import org.codehaus.jackson.annotate.JsonProperty;

public class Localidad {
	
	@JsonProperty(value="id")
	private Long id;
	
	private ArrayList<Silla> sillas;
	
	private Sitio sitio;

	@JsonProperty(value="sillaNumerada")
	private char sillaNumerada;
	
	@JsonProperty(value="nombre")
	private String nombre;

	@JsonProperty(value="precio")
	private int precio;
	
	@JsonProperty(value="idSitio")
	private Long idSitio;
	
	@JsonProperty(value="capacidad")
	private int capacidad;
	
	public Localidad (@JsonProperty(value="id")Long id, 
			@JsonProperty(value="idSitio") Long idSitio, 
			@JsonProperty(value="nombre")String nombre, 
			@JsonProperty(value="capacidad")int capacidad,
			@JsonProperty(value="sillaNumerada")char sillaNumerada, 
			@JsonProperty(value="precio")int precio)
	{
		this.id = id;
		this.nombre = nombre;
		this.idSitio = idSitio;
		this.sillaNumerada = sillaNumerada;
		this.precio = precio;
		sillas = new ArrayList<Silla>();
	}

	
	public int getCapacidad() {
		return capacidad;
	}


	public void setCapacidad(int capacidad) {
		this.capacidad = capacidad;
	}


	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public ArrayList<Silla> getSillas() {
		return sillas;
	}

	public void setSillas(ArrayList<Silla> sillas) {
		this.sillas = sillas;
	}

	public Sitio getSitio() {
		return sitio;
	}

	public void setSitio(Sitio sitio) {
		this.sitio = sitio;
	}

	public char getSillaNumerada() {
		return sillaNumerada;
	}

	public void setSillaNumerada(char sillaNumerada) {
		this.sillaNumerada = sillaNumerada;
	}

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre;
	}
	
	public int getPrecio() {
		return precio;
	}

	public void setPrecio(int precio) {
		this.precio = precio;
	}

//	public void addSilla(Long id, int numero)
//	{
//		Silla sillax = null;
//		if(!sillaNumerada)
//			sillax = new Silla(id, this.id , 0);
//		else
//			sillax = new Silla(id, this.id , numero);
//		sillas.add(sillax);
//	}

	public Long getIdSitio() {
		return idSitio;
	}

	public void setIdSitio(Long idSitio) {
		this.idSitio = idSitio;
	}
	
	

}
