package vos;

import org.codehaus.jackson.annotate.JsonProperty;

public class FuncionRespuestaCliente {
	
	@JsonProperty (value = "idFuncion")
	private Long idFuncion;
	
	@JsonProperty (value = "fecha")
	private String fecha;
	
	@JsonProperty (value = "idSitio")
	private Long idSitio;
	
	@JsonProperty (value = "idEspectaculo")
	private Long idEspectaculo;
	
	public FuncionRespuestaCliente(@JsonProperty (value = "idFuncion") Long idFuncion,
			@JsonProperty (value = "fecha") String fecha,
			@JsonProperty (value = "idSitio") Long idSitio,
			@JsonProperty (value = "idEspectaculo") Long idEspectaculo)
	{
		this.idFuncion = idFuncion;
		this.fecha = fecha;
		this.idSitio = idSitio;
		this.idEspectaculo = idEspectaculo;
	}

	public Long getIdFuncion() {
		return idFuncion;
	}

	public void setIdFuncion(Long idFuncion) {
		this.idFuncion = idFuncion;
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

	public Long getIdEspectaculo() {
		return idEspectaculo;
	}

	public void setIdEspectaculo(Long idEspectaculo) {
		this.idEspectaculo = idEspectaculo;
	}
	
	
	
}
