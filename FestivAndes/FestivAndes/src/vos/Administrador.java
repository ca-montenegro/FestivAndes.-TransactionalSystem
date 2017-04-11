package vos;

import java.util.ArrayList;

import org.codehaus.jackson.annotate.JsonProperty;

public class Administrador extends Usuario{
	
	/**
	 * Festival que administra
	 */
	private Festival festival;
	
	/**
	 * Metodo constructor de la clase administrador
	 * <b>post: </b> Crea el administrador con los valores que entran como parámetro
	 * @param id Id del administrador
	 * @param nombre Nombre del administrador
	 * @param correo Correo del administrador
	 */
	public Administrador(@JsonProperty(value="id")Long id, @JsonProperty(value="nombre")String nombre,
			@JsonProperty(value="correo") String correo)
	{
		super(id, nombre, correo, (long) 1);
	}
	
	/**
	 * Metodo getter del atributo festival
	 * @return Festival del administrador
	 */
	public Festival getFestival() {
		return festival;
	}

	/**
	 * Metodo setter del atributo espectaculos
	 *  <b>post: </b> Los espectaculos del organizador han sido cambiados con el arreglo que entra como parametro
	 * @param espectaculos - Espectaculos del usuario.
	 */
	public void setFestival(Festival festival) {
		this.festival = festival;
	}
	
}
