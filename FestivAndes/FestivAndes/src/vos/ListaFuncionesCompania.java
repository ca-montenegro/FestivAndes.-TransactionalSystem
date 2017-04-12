package vos;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

public class ListaFuncionesCompania {
	
	@JsonProperty(value="idCompania")
	private Long idCompania;
	
	@JsonProperty(value="funciones")
	private List<FuncionCompania> funciones;
	
	public ListaFuncionesCompania(@JsonProperty(value="idCompania")Long idCompania,
	@JsonProperty(value="funciones")List<FuncionCompania> funciones)
	{
		this.idCompania = idCompania;
		this.funciones = funciones;
	}

	public Long getIdCompania() {
		return idCompania;
	}

	public void setIdCompania(Long idCompania) {
		this.idCompania = idCompania;
	}

	public List<FuncionCompania> getFunciones() {
		return funciones;
	}

	public void setFunciones(List<FuncionCompania> funciones) {
		this.funciones = funciones;
	}
	
	
	

}
