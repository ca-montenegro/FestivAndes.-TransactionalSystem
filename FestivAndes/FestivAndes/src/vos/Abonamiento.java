package vos;

import java.util.ArrayList;

import org.codehaus.jackson.annotate.JsonProperty;

public class Abonamiento {

	@JsonProperty(value="idsFunciones")
	private ArrayList<Long> idsFunciones;
	
	@JsonProperty(value="idsLocalidades")
	private ArrayList<String> localidades;
	
	@JsonProperty(value="fechaConsulta")
	private String fechaConsulta;
	
	private ArrayList<Boleta> abonamientoList;



	public Abonamiento(@JsonProperty(value="idsFunciones") ArrayList<Long> idsFunciones,
			@JsonProperty(value="idsLocalidades") ArrayList<String> localidades, 
			@JsonProperty(value="fechaConsulta") String fechaConsulta) {
		this.idsFunciones = idsFunciones;
		this.localidades = localidades;
		this.fechaConsulta = fechaConsulta;
		
	}
	

	public ArrayList<Long> getIdsFunciones() {
		return idsFunciones;
	}

	public void setIdsFunciones(ArrayList<Long> idsFunciones) {
		this.idsFunciones = idsFunciones;
	}

	public ArrayList<String> getIdsLocalidades() {
		return localidades;
	}

	public void setIdsLocalidades(ArrayList<String> localidades) {
		this.localidades = localidades;
	}


	public String getFechaConsulta() {
		return fechaConsulta;
	}


	public void setFechaConsulta(String fechaConsulta) {
		this.fechaConsulta = fechaConsulta;
	}
	
	public ArrayList<Boleta> getAbonamientoList() {
		return abonamientoList;
	}


	public void setAbonamientoList(ArrayList<Boleta> abonamientoList) {
		this.abonamientoList = abonamientoList;
	}
	
	public String toString(){
		
		return "";
	}
	
}
