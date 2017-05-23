/**-------------------------------------------------------------------
 * $Id$
 * Universidad de los Andes (Bogotá - Colombia)
 * Departamento de Ingeniería de Sistemas y Computación
 *
 * Materia: Sistemas Transaccionales
 * Ejercicio: VideoAndes
 * Autor: Juan Felipe García - jf.garcia268@uniandes.edu.co
 * -------------------------------------------------------------------
 */
package jms;

import vos.ListaVideos;

/**
 * clase IncompleteReplyException
 * @author Juan
 */
public class IncompleteReplyException extends Exception {
	

	private static final long serialVersionUID = 1L;
	
	/**
	 * Atributo con las respuestas parciales
	 */
	private ListaVideos partialResponse;
	
	/**
	 * Método constructor de la clase IncompleteReplyException
	 * <b>post: </b> Crea la  IncompleteReplyException con los valores que entran como parámetro
	 * @param message - mensaje de la IncompleteReplyException
	 * @param partialResponse - respuesta parcial a guardar.
	 */
	public IncompleteReplyException(String message,ListaVideos partialResponse){
		super(message);
		this.partialResponse = partialResponse;
	}
	
	/**
	 * Método que retorna la respuesta parcial
	 * @return ListaVideos - respuesta parcial
	 */
	public ListaVideos getPartialResponse(){
		return this.partialResponse;
	}

}
