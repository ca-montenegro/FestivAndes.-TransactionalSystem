package vos;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

public class ListaInformacionFuncion {
	
	/**
	 * List con los informacion
	 */
	@JsonProperty(value="informacion")
	private List<InformacionVentaFuncion> informacion;
	
	@JsonProperty(value="cantidadTotalBoletas")
	private int cantidadTotalBoletas;

	@JsonProperty(value="producidoTotalEspectaculo")
	private int producidoTotalEspectaculo;
	
	/**
	 * Constructor de la clase ListaInformacion
	 * @param informacion - informacion para agregar al arreglo de la clase
	 */
	public ListaInformacionFuncion( @JsonProperty(value="informacion")List<InformacionVentaFuncion> informacion){
		this.informacion = informacion;
		int prod = 0;
		for(InformacionVentaFuncion temp: informacion)
		{
			prod += temp.getProducidoTotal();
		}
		this.producidoTotalEspectaculo = prod;
		
		int cant = 0;
		for(InformacionVentaFuncion temp: informacion)
		{
			cant += temp.getCantidadTotalDeBoletas();
		}
		this.cantidadTotalBoletas = cant;
		
	}
}
