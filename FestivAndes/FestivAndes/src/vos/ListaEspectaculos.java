package vos;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

public class ListaEspectaculos {
	
	@JsonProperty(value="Funciones")
	private List<ListaFuncionesEspectaculos> funciones;
	
	public ListaEspectaculos(@JsonProperty(value="Funciones")List<ListaFuncionesEspectaculos> funciones) {
		this.funciones = funciones;
	}

	public List<ListaFuncionesEspectaculos> getFunciones() {
		return funciones;
	}

	public void setFunciones(List<ListaFuncionesEspectaculos> funciones) {
		this.funciones = funciones;
	}
	
	

}
