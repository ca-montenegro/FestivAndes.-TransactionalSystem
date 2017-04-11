package vos;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

public class ListaLocalidades {
	
	@JsonProperty(value="Localidades")
	private List<Localidad> localidades;
	
	public ListaLocalidades(@JsonProperty(value="Localidades")List<Localidad> localidades) {
		this.localidades = localidades;
	}

	public List<Localidad> getLocalidades() {
		return localidades;
	}

	public void setLocalidades(List<Localidad> localidades) {
		this.localidades = localidades;
	}
	

}
