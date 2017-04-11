package vos;

import org.codehaus.jackson.annotate.JsonProperty;

public class InformacionVentaFuncion {

	@JsonProperty(value="idFuncion")
	private Long idFuncion;
	
	@JsonProperty(value="idSitio")
	private Long idSitio;
	
	@JsonProperty(value="cantidadTotalDeBoletas")
	private int cantidadTotalDeBoletas;
	
	@JsonProperty(value="numeroBoletasConCliente")
	private int numeroBoletasConCliente;
	
	@JsonProperty(value="numeroBoletasSinCliente")
	private int numeroBoletasSinCliente;
	
	@JsonProperty(value="producidoPorBoletasConCliente")
	private int producidoPorBoletasConCliente;
	
	@JsonProperty(value="producidoPorBoletasSinCliente")
	private int producidoPorBoletasSinCliente;
	
	@JsonProperty(value="producidoTotal")
	private int producidoTotal;
	
	@JsonProperty(value="porcentajeOcupacionSitio")
	private double porcentajeOcupacionSitio;

	public InformacionVentaFuncion(@JsonProperty(value="idFuncion")Long idFuncion, 
			@JsonProperty(value="idSitio")Long idSitio, 
			@JsonProperty(value="cantidadTotalDeBoletas")int cantidadTotalDeBoletas,
			@JsonProperty(value="numeroBoletasConCliente")int numeroBoletasConCliente, 
			@JsonProperty(value="numeroBoletasSinCliente")int numeroBoletasSinCliente, 
			@JsonProperty(value="producidoPorBoletasConCliente")int producidoPorBoletasConCliente,
			@JsonProperty(value="producidoPorBoletasSinCliente")int producidoPorBoletasSinCliente,
			@JsonProperty(value="producidoTotal")int producidoTotal,
			@JsonProperty(value="porcentajeOcupacionSitio")double porcentajeOcupacionSitio) 
	{
		this.idFuncion = idFuncion;
		this.idSitio = idSitio;
		this.cantidadTotalDeBoletas = cantidadTotalDeBoletas;
		this.numeroBoletasConCliente = numeroBoletasConCliente;
		this.numeroBoletasSinCliente = numeroBoletasSinCliente;
		this.producidoPorBoletasConCliente = producidoPorBoletasConCliente;
		this.producidoPorBoletasSinCliente = producidoPorBoletasSinCliente;
		this.producidoTotal = producidoTotal;
		this.porcentajeOcupacionSitio = porcentajeOcupacionSitio;
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

	public int getCantidadTotalDeBoletas() {
		return cantidadTotalDeBoletas;
	}

	public void setCantidadTotalDeBoletas(int cantidadTotalDeBoletas) {
		this.cantidadTotalDeBoletas = cantidadTotalDeBoletas;
	}

	public int getNumeroBoletasConCliente() {
		return numeroBoletasConCliente;
	}

	public void setNumeroBoletasConCliente(int numeroBoletasConCliente) {
		this.numeroBoletasConCliente = numeroBoletasConCliente;
	}

	public int getNumeroBoletasSinCliente() {
		return numeroBoletasSinCliente;
	}

	public void setNumeroBoletasSinCliente(int numeroBoletasSinCliente) {
		this.numeroBoletasSinCliente = numeroBoletasSinCliente;
	}

	public int getProducidoPorBoletasConCliente() {
		return producidoPorBoletasConCliente;
	}

	public void setProducidoPorBoletasConCliente(int producidoPorBoletasConCliente) {
		this.producidoPorBoletasConCliente = producidoPorBoletasConCliente;
	}

	public int getProducidoPorBoletasSinCliente() {
		return producidoPorBoletasSinCliente;
	}

	public void setProducidoPorBoletasSinCliente(int producidoPorBoletasSinCliente) {
		this.producidoPorBoletasSinCliente = producidoPorBoletasSinCliente;
	}

	public int getProducidoTotal() {
		return producidoTotal;
	}

	public void setProducidoTotal(int producidoTotal) {
		this.producidoTotal = producidoTotal;
	}

	public double getPorcentajeOcupacionSitio() {
		return porcentajeOcupacionSitio;
	}

	public void setPorcentajeOcupacionSitio(double porcentajeOcupacionSitio) {
		this.porcentajeOcupacionSitio = porcentajeOcupacionSitio;
	}
	
	
	
}
