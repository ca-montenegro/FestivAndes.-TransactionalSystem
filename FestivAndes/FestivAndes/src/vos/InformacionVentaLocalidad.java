package vos;

import org.codehaus.jackson.annotate.JsonProperty;

public class InformacionVentaLocalidad {
	
	@JsonProperty(value="idLocalidad")
	private Long idLocalidad;
	
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
	
	public InformacionVentaLocalidad (@JsonProperty(value="idLocalidad")Long idLocalidad,
			@JsonProperty(value="cantidadTotalDeBoletas")int cantidadTotalDeBoletas, 
			@JsonProperty(value="numeroBoletasConCliente")int numeroBoletasConCliente, 
			@JsonProperty(value="numeroBoletasSinCliente")int numeroBoletasSinCliente, 
			@JsonProperty(value="producidoPorBoletasConCliente")int producidoPorBoletasConCliente, 
			@JsonProperty(value="producidoPorBoletasSinCliente")int producidoPorBoletasSinCliente,
			@JsonProperty(value="producidoTotal")int producidoTotal)
	{
		this.idLocalidad = idLocalidad;
		this.cantidadTotalDeBoletas = cantidadTotalDeBoletas;
		this.numeroBoletasConCliente = numeroBoletasConCliente;
		this.numeroBoletasSinCliente = numeroBoletasSinCliente;
		this.producidoPorBoletasConCliente = producidoPorBoletasConCliente;
		this.producidoPorBoletasSinCliente = producidoPorBoletasSinCliente;
		this.producidoTotal = producidoTotal;
	}
	
	public int getProducidoTotal() {
		return producidoTotal;
	}

	public void setProducidoTotal(int producidoTotal) {
		this.producidoTotal = producidoTotal;
	}

	public Long getIdLocalidad() {
		return idLocalidad;
	}

	public void setIdLocalidad(Long idLocalidad) {
		this.idLocalidad = idLocalidad;
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

	
	

}
