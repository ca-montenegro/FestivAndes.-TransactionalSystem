package vos;

import java.util.ArrayList;

import org.codehaus.jackson.annotate.JsonProperty;

public class Organizador extends Usuario{
	
	/**
	 * Espectaculos a cargo del organizador
	 */
	private ArrayList<Espectaculo> espectaculos;
	
	/**
	 * Metodo constructor de la clase organizador
	 * <b>post: </b> Crea el organizador con los valores que entran como parámetro
	 * @param id Id del organizador
	 * @param nombre Nombre del organizador
	 * @param correo Correo del organizador
	 */
	public Organizador(@JsonProperty(value="id")Long id, @JsonProperty(value="nombre")String nombre,
			@JsonProperty(value="correo") String correo)
	{
		super(id, nombre, correo, (long) 3);
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
