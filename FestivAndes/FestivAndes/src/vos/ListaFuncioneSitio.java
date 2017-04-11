package vos;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

public class ListaFuncioneSitio {
	
	@JsonProperty(value="Funciones")
	private List<InformacionFuncionSitio> funciones;
	
	public ListaFuncioneSitio(@JsonProperty(value="Funciones")List<InformacionFuncionSitio> funciones) {
		this.funciones = funciones;
	}

	public List<InformacionFuncionSitio> getFunciones() {
		return funciones;
	}

	public void setFunciones(List<InformacionFuncionSitio> funciones) {
		this.funciones = funciones;
	}
	
	

}
