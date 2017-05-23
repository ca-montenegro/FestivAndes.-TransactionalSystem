package vos;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

public class ListaFuncionesEspectaculos {
	
	@JsonProperty(value="idEspectaculo")
	private Long idEspectaculo;
	
	@JsonProperty(value="funciones")
	private List<Funcion> funciones;
	
	public ListaFuncionesEspectaculos(@JsonProperty(value="idEspectaculo")Long idEspectaculo,
	@JsonProperty(value="funciones")List<Funcion> funciones)
	{
		this.idEspectaculo = idEspectaculo;
		this.funciones = funciones;
	}

	public Long getIdEspectaculo() {
		return idEspectaculo;
	}

	public void setIdEspectaculo(Long idEspectaculo) {
		this.idEspectaculo = idEspectaculo;
	}

	public List<Funcion> getFunciones() {
		return funciones;
	}

	public void setFunciones(List<Funcion> funciones) {
		this.funciones = funciones;
	}

}
