package vos;

import org.codehaus.jackson.annotate.JsonProperty;

public class ListaPorRealizacion {
	
	@JsonProperty(value = "boletasActivas")
	private ListaPorEstado boletasActivas;
	
	@JsonProperty(value = "boletasDevueltas")
	private ListaPorEstado boletasDevueltas;
	
	public ListaPorRealizacion(@JsonProperty(value ="boletasActivas") ListaPorEstado boletasActivas,
			@JsonProperty(value = "boletasDevueltas") ListaPorEstado boletasDevueltas)
	{
		this.boletasActivas = boletasActivas;
		this.boletasDevueltas = boletasDevueltas;
	}
	
	public ListaPorEstado getBoletasActivas() {
		return boletasActivas;
	}

	public void setBoletasActivas(ListaPorEstado boletasActivas) {
		this.boletasActivas = boletasActivas;
	}

	public ListaPorEstado getBoletasDevueltas() {
		return boletasDevueltas;
	}

	public void setBoletasDevueltas(ListaPorEstado boletasDevueltas) {
		this.boletasDevueltas = boletasDevueltas;
	}

	
	
}
