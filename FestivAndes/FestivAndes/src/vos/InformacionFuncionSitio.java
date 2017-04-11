package vos;

import org.codehaus.jackson.annotate.JsonProperty;

public class InformacionFuncionSitio {

	@JsonProperty(value="idFuncion")
	private Long id;
	
	@JsonProperty(value="fecha")
	private String fecha;
	
	@JsonProperty(value="hora")
	private int hora;
	
	@JsonProperty(value="nombreEspectaculo")
	private String nombreEspectaculo;
	
	@JsonProperty(value="boletasDisponibles")
	private int boletasDisponibles;
	
	

	public InformacionFuncionSitio(	@JsonProperty(value="idFuncion")Long id, 
			@JsonProperty(value="fecha")String fecha, 
			@JsonProperty(value="hora")int hora, 
			@JsonProperty(value="nombreEspectaculo")String nombreEspectaculo,
			@JsonProperty(value="boletasDisponibles")int boletasDisponibles) {
		this.id = id;
		this.fecha = fecha;
		this.hora = hora;
		this.nombreEspectaculo = nombreEspectaculo;
		this.boletasDisponibles = boletasDisponibles;
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

	public int getHora() {
		return hora;
	}

	public void setHora(int hora) {
		this.hora = hora;
	}

	public String getNombreEspectaculo() {
		return nombreEspectaculo;
	}

	public void setNombreEspectaculo(String nombreEspectaculo) {
		this.nombreEspectaculo = nombreEspectaculo;
	}

	public int getBoletasDisponibles() {
		return boletasDisponibles;
	}

	public void setBoletasDisponibles(int boletasDisponibles) {
		this.boletasDisponibles = boletasDisponibles;
	}
	
	
}
