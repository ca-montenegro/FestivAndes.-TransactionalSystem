package vos;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

public class ListaUsuarios {
	
	@JsonProperty(value="idCompania")
	private Long idCompania;
	
	@JsonProperty(value="usuarios")
	private List<Usuario> usuarios;
	
	public ListaUsuarios(@JsonProperty(value="idCompania")Long idCompania,
	@JsonProperty(value="usuarios")List<Usuario> usuarios)
	{
		this.idCompania = idCompania;
		this.usuarios = usuarios;
	}

	public Long getIdCompania() {
		return idCompania;
	}

	public void setIdCompania(Long idCompania) {
		this.idCompania = idCompania;
	}

	public List<Usuario> getUsuarios() {
		return usuarios;
	}

	public void setUsuarios(List<Usuario> usuarios) {
		this.usuarios = usuarios;
	}
	
	
}
