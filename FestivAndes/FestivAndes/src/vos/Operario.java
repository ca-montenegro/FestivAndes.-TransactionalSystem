package vos;

import java.util.ArrayList;

import org.codehaus.jackson.annotate.JsonProperty;

public class Operario extends Usuario {
	
	/**
	 * Espectaculos del sistema que el operario debe conocer
	 */
	private ArrayList<Espectaculo> espectaculos;
	
	/**
	 * Metodo constructor de la clase operario
	 * <b>post: </b> Crea el operario con los valores que entran como parámetro
	 * @param id Id del operario
	 * @param nombre Nombre del operario
	 * @param correo Correo del operario
	 */
	public Operario(@JsonProperty(value="id")Long id, @JsonProperty(value="nombre")String nombre,
			@JsonProperty(value="correo") String correo)
	{
		super(id, nombre, correo, (long) 4);
		espectaculos = new ArrayList<Espectaculo>();
	}
	
	/**
	 * Metodo getter del atributo espectaculos
	 * @return Espectaculos del organizador
	 */
	public ArrayList<Espectaculo> getEspectaculos() {
		return espectaculos;
	}

	/**
	 * Metodo setter del atributo espectaculos
	 *  <b>post: </b> Los espectaculos del organizador han sido cambiados con el arreglo que entra como parametro
	 * @param espectaculos - Espectaculos del usuario.
	 */
	public void setEspectaculos(ArrayList<Espectaculo> espectaculos) {
		this.espectaculos = espectaculos;
	}
	
	public void addEspectaculo(Espectaculo espectaculo)
	{
		espectaculos.add(espectaculo);
	}
	
	

}
