package vos;

import org.codehaus.jackson.annotate.JsonProperty;

public class ListaRespuestaAsistencia {
	
	@JsonProperty(value = "realizadas")
	private ListaPorRealizacion realizadas;
	
	@JsonProperty(value = "noRealizadas")
	private ListaPorRealizacion noRealizadas;
	
	public ListaRespuestaAsistencia( 
			@JsonProperty(value = "realizadas")ListaPorRealizacion realizadas, 
			@JsonProperty(value = "noRealizadas")ListaPorRealizacion noRealizadas)
	{
		this.realizadas = realizadas;
		this.noRealizadas = noRealizadas;
	}

	public ListaPorRealizacion getRealizadas() {
		return realizadas;
	}

	public void setRealizadas(ListaPorRealizacion realizadas) {
		this.realizadas = realizadas;
	}

	public ListaPorRealizacion getNoRealizadas() {
		return noRealizadas;
	}

	public void setNoRealizadas(ListaPorRealizacion noRealizadas) {
		this.noRealizadas = noRealizadas;
	}

	
	
}
