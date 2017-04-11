package vos;

import org.codehaus.jackson.annotate.JsonProperty;

public class Usuario {
	
	/**
	 * Identificacion del usuario 
	 */
	@JsonProperty(value="id")
	private Long id;
	
	/**
	 * Nombre del usuario
	 */
	@JsonProperty(value="nombre")
	private String nombre;
	
	/**
	 * Correo del usuario
	 */
	@JsonProperty(value="correo")
	private String correo;
	
	/**
	 * Rol del usuario
	 */
	@JsonProperty(value="rol")
	private Long rol;
	
	/**
	 * Método constructor de la clase usuario
	 * <b>post: </b> Crea el usuario con los valores que entran como parámetro
	 * @param id - Identificacion del usuario. id != null
	 * @param nombre - Nombre del usuario. name != null
	 * @param correo - Correo del usuario. nombre != null.
	 * @param rol - Rol del usuario. rol != null
	 */
	public Usuario(@JsonProperty(value="id")Long id, @JsonProperty(value="nombre")String nombre,
			@JsonProperty(value="correo")String correo, @JsonProperty(value="rol")Long rol)
	{
		this.id = id;
		this.nombre = nombre;
		this.correo = correo;
		this.rol = rol;
	}

	/**
	 * Metodo getter del atributo id
	 * @return identificacion del usuario
	 */
	public Long getId() {
		return id;
	}

	/**
	 * Metodo setter del atributo id
	 *  <b>post: </b> La identificacion del usuario ha sido cambiado con el valor que entra como parametro
	 * @param id - Identificacion del usuario.
	 */
	public void setId(Long id) {
		this.id = id;
	}

	/**
	 * Metodo getter del atributo nombre
	 * @return nombre del usuario
	 */
	public String getNombre() {
		return nombre;
	}

	/**
	 * Metodo setter del atributo nombre
	 *  <b>post: </b> El nombre del usuario ha sido cambiado con el valor que entra como parametro
	 * @param nombre - Nombre del usuario.
	 */
	public void setNombre(String nombre) {
		this.nombre = nombre;
	}

	/**
	 * Metodo getter del atributo correo
	 * @return correo del usuario
	 */
	public String getCorreo() {
		return correo;
	}

	/**
	 * Metodo setter del atributo correo
	 *  <b>post: </b> El corro del usuario ha sido cambiado con el valor que entra como parametro
	 * @param correo - Correo del usuario.
	 */
	public void setCorreo(String correo) {
		this.correo = correo;
	}

	/**
	 * Metodo getter del atributo rol
	 * @return rol del usuario
	 */
	public Long getRol() {
		return rol;
	}

	/**
	 * Metodo setter del atributo rol
	 *  <b>post: </b> El rol del usuario ha sido cambiado con el valor que entra como parametro
	 * @param rol - Rol del usuario.
	 */
	public void setRol(Long rol) {
		this.rol = rol;
	}
	
	

}
