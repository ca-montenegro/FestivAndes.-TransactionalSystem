package vos;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

public class ListaBuenosClientes {
	
	@JsonProperty(value="numBoletas")
	private Long numBoletas;
	
	@JsonProperty(value="usuarios")
	private List<Usuario> usuarios;
	
	public ListaBuenosClientes(@JsonProperty(value="numBoletas")Long numBoletas,
	@JsonProperty(value="usuarios")List<Usuario> usuarios)
	{
		this.numBoletas = numBoletas;
		this.usuarios = usuarios;
	}
	
	public Long getNumBoletas() {
		return numBoletas;
	}


	public void setNumBoletas(Long numBoletas) {
		this.numBoletas = numBoletas;
	}



	public List<Usuario> getUsuarios() {
		return usuarios;
	}

	public void setUsuarios(List<Usuario> usuarios) {
		this.usuarios = usuarios;
	}
	
	
}
