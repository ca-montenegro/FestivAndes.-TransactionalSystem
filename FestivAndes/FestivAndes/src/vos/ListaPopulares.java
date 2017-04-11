package vos;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

public class ListaPopulares {

	@JsonProperty(value="masPopuEspectaculo")
	private List<MasPopuEspectaculo> masPopuEspectaculo;
	
	public ListaPopulares(@JsonProperty(value="masPopuEspectaculo") List<MasPopuEspectaculo> masPopuEspectaculo)
	{
		this.masPopuEspectaculo = masPopuEspectaculo;
	}

	public List<MasPopuEspectaculo> getMasPopuEspectaculo() {
		return masPopuEspectaculo;
	}

	public void setMasPopuEspectaculo(List<MasPopuEspectaculo> masPopuEspectaculo) {
		this.masPopuEspectaculo = masPopuEspectaculo;
	}


	
}
