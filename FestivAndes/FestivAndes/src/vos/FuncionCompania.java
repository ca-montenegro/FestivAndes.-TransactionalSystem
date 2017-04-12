package vos;

import org.codehaus.jackson.annotate.JsonProperty;

public class FuncionCompania {
	
	@JsonProperty(value ="idFuncion")
	private Long idFuncion;
	
	@JsonProperty(value="idSitio")
	private Long idSitio;
	
	@JsonProperty(value="producido")
	private int producido;
	
	@JsonProperty(value="porcentajeOcupacion")
	private double porcentajeOcupacion;
	
	@JsonProperty(value="cantidadBoletasClientes")
	private int cantidadBoletasClientes;
	
	public FuncionCompania(@JsonProperty(value ="idFuncion")Long idFuncion,
	@JsonProperty(value="idSitio")Long idSitio,	
	@JsonProperty(value="producido")int producido,
	@JsonProperty(value="porcentajeOcupacion")double porcentajeOcupacion,
	@JsonProperty(value="cantidadBoletasClientes")int cantidadBoletasClientes)
	{
		this.cantidadBoletasClientes = cantidadBoletasClientes;
		this.porcentajeOcupacion = porcentajeOcupacion;
		this.producido = producido;
		this.idFuncion = idFuncion;
		this.idSitio = idSitio;
	}

	public Long getIdFuncion() {
		return idFuncion;
	}

	public void setIdFuncion(Long idFuncion) {
		this.idFuncion = idFuncion;
	}

	public Long getIdSitio() {
		return idSitio;
	}

	public void setIdSitio(Long idSitio) {
		this.idSitio = idSitio;
	}

	public int getProducido() {
		return producido;
	}

	public void setProducido(int producido) {
		this.producido = producido;
	}

	public double getPorcentajeOcupacion() {
		return porcentajeOcupacion;
	}

	public void setPorcentajeOcupacion(double porcentajeOcupacion) {
		this.porcentajeOcupacion = porcentajeOcupacion;
	}

	public int getCantidadBoletasClientes() {
		return cantidadBoletasClientes;
	}

	public void setCantidadBoletasClientes(int cantidadBoletasClientes) {
		this.cantidadBoletasClientes = cantidadBoletasClientes;
	}

	
	
}
