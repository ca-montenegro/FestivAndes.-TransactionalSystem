package vos;

import java.util.ArrayList;

import org.codehaus.jackson.annotate.JsonProperty;

public class Cliente extends Usuario{
	
	/**
	 * Boletas del cliente
	 */
	private ArrayList<VOBoleta> boletas;
	
	/**
	 * Preferencias del cliente
	 */
	private ArrayList<Preferencia> preferencias;
	
	/**
	 * Metodo constructor de la clase cliente
	 * <b>post: </b> Crea el cliente con los valores que entran como parámetro
	 * @param id Id del cliente
	 * @param nombre Nombre del cliente
	 * @param correo Correo del cliente
	 */
	public Cliente(@JsonProperty(value="id")Long id, @JsonProperty(value="nombre")String nombre,
			@JsonProperty(value="correo") String correo)
	{
		super(id, nombre, correo, (long) 2);
		boletas = new ArrayList<VOBoleta>();
		preferencias = new ArrayList<Preferencia>();
	}
	
	/**
	 * Metodo getter del atributo boletas
	 * @return Boletas del cliente
	 */
	public ArrayList<VOBoleta> getBoletas() {
		return boletas;
	}

	/**
	 * Metodo setter del atributo boletas
	 *  <b>post: </b> Las boletas del cliente han sido cambiados con el arreglo que entra como parametro
	 * @param boletas- Boletas del cliente.
	 */
	public void setBoletas(ArrayList<VOBoleta> boletas) {
		this.boletas = boletas;
	}
	
	/**
	 * Metodo getter del atributo preferencias
	 * @return Preferencias del cliente
	 */
	public ArrayList<Preferencia> getPreferencias() {
		return preferencias;
	}

	/**
	 * Metodo setter del atributo preferencias
	 *  <b>post: </b> Las preferencias del cliente han sido cambiados con el arreglo que entra como parametro
	 * @param preferencias - Preferencias del cliente.
	 */
	public void setPreferencias(ArrayList<Preferencia> preferencias) {
		this.preferencias = preferencias;
	}
	

}
