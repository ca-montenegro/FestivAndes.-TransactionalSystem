package vos;

import org.codehaus.jackson.annotate.JsonProperty;

public class ReporteSitio {
	
	@JsonProperty(value="Sitio")
	private Sitio sitio;
	
	@JsonProperty(value="Localidades")
	private ListaLocalidades localidades;
	
	@JsonProperty(value="Funciones")
	private ListaFuncioneSitio funciones;

	public ReporteSitio(@JsonProperty(value="Sitio")Sitio sitio, 
			@JsonProperty(value="Localidades")ListaLocalidades localidades, 
			@JsonProperty(value="Funciones")ListaFuncioneSitio funciones) {
		this.sitio = sitio;
		this.localidades = localidades;
		this.funciones = funciones;
	}

	public Sitio getSitio() {
		return sitio;
	}

	public void setSitio(Sitio sitio) {
		this.sitio = sitio;
	}

	public ListaLocalidades getLocalidades() {
		return localidades;
	}

	public void setLocalidades(ListaLocalidades localidades) {
		this.localidades = localidades;
	}

	public ListaFuncioneSitio getFunciones() {
		return funciones;
	}

	public void setFunciones(ListaFuncioneSitio funciones) {
		this.funciones = funciones;
	}
	
	
	
	
	

}
