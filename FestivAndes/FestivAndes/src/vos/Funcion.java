package vos;

//import javax.persistence.GeneratedValue;
//import javax.persistence.GenerationType;
//import javax.persistence.Id;

import org.codehaus.jackson.annotate.JsonProperty;

public class Funcion {

	@JsonProperty(value="id")
//	@Id
//	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;
	
	@JsonProperty(value="fecha")
	private String fecha;
	
	@JsonProperty(value="idSitio")
	private Long idSitio;
	
	@JsonProperty(value="realizada")
	private char realizada;
	
	@JsonProperty(value="hora")
	private int hora;

	public Funcion(@JsonProperty(value="id") Long id, 
			@JsonProperty(value="fecha") String fecha, 
			@JsonProperty(value="idSitio")  Long idSitio, 
			@JsonProperty(value="hora") int hora,
			@JsonProperty(value="realizada") char realizada) 
	{
		this.id = id;
		this.fecha = fecha;
		this.idSitio = idSitio;
		this.hora = hora;
		this.realizada = realizada;
	}
	
	public char getRealizada() {
		return realizada;
	}

	public void setRealizada(char realizada) {
		this.realizada = realizada;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getFecha() {
		return fecha;
	}

	public void setFecha(String fecha) {
		this.fecha = fecha;
	}

	public Long getIdSitio() {
		return idSitio;
	}

	public void setIdSitio(Long idSitio) {
		this.idSitio = idSitio;
	}
	
	public int getHora() {
		return (hora);
	}

	public void setHora(int hora) {
		this.hora = hora;
	}
}
