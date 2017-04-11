package vos;

import java.util.ArrayList;

import org.codehaus.jackson.annotate.JsonProperty;

public class Preferencia {

	@JsonProperty(value="id")
	private Long id;
	
	@JsonProperty(value="nombre")
	private String nombre;
	
	@JsonProperty(value="descripcion")
	private String descripcion;
	
	private ArrayList<Cliente> clientes;

	public Preferencia(@JsonProperty(value="id") Long id,
	@JsonProperty(value="nombre") String nombre,
	@JsonProperty(value="descripcion") String descripcion)
	{
		this.id = id;
		this.nombre = nombre;
		this.descripcion = descripcion;
		clientes = new ArrayList<Cliente>();
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre;
	}

	public String getDescripcion() {
		return descripcion;
	}

	public void setDescripcion(String descripcion) {
		this.descripcion = descripcion;
	}

	public ArrayList<Cliente> getClientes() {
		return clientes;
	}

	public void setClientes(ArrayList<Cliente> clientes) {
		this.clientes = clientes;
	}
	
	public void addCliente(Cliente cliente)
	{
		clientes.add(cliente);
	}
	
}
