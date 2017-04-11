package vos;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

public class ListaInformacion {
	
	/**
	 * List con los informacion
	 */
	@JsonProperty(value="informacion")
	private List<InformacionVentaLocalidad> informacion;
	
	@JsonProperty(value="cantidadTotalBoletas")
	private int cantidadTotalBoletas;

	@JsonProperty(value="producidoTotalFuncion")
	private int producidoTotalFuncion;
	
	/**
	 * Constructor de la clase ListaInformacion
	 * @param informacion - informacion para agregar al arreglo de la clase
	 */
	public ListaInformacion( @JsonProperty(value="informacion")List<InformacionVentaLocalidad> informacion){
		this.informacion = informacion;
		int prod = 0;
		for(InformacionVentaLocalidad temp: informacion)
		{
			prod += temp.getProducidoTotal();
		}
		this.producidoTotalFuncion = prod;
		
		int cant = 0;
		for(InformacionVentaLocalidad temp: informacion)
		{
			cant += temp.getCantidadTotalDeBoletas();
		}
		this.cantidadTotalBoletas = cant;
		
	}
	
	public int getCantidadTotalBoletas() {
		return cantidadTotalBoletas;
	}

	public void setCantidadTotalBoletas(int cantidadTotalBoletas) {
		this.cantidadTotalBoletas = cantidadTotalBoletas;
	}



	public int getProducidoTotalFuncion() {
		return producidoTotalFuncion;
	}



	public void setProducidoTotalFuncion(int producidoTotalFuncion) {
		this.producidoTotalFuncion = producidoTotalFuncion;
	}



	public List<InformacionVentaLocalidad> getInformacion() {
		return informacion;
	}

	public void setInformacion(List<InformacionVentaLocalidad> informacion) {
		this.informacion = informacion;
	}

	

}
