package vos;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

public class ListaPorEstado {
	
	@JsonProperty(value = "funciones")
	private List<FuncionRespuestaCliente> funciones;
	
	public ListaPorEstado(@JsonProperty(value="funciones")List<FuncionRespuestaCliente> funciones)
	{
		this.funciones = funciones;
	}
	
	public List<FuncionRespuestaCliente> getFunciones() {
		return funciones;
	}

	public void setFunciones(List<FuncionRespuestaCliente> funciones) {
		this.funciones = funciones;
	}
}
