package vos;

import java.util.ArrayList;

import org.codehaus.jackson.annotate.JsonProperty;

public class Festival {
	
	@JsonProperty(value="id")
	private Long id;
	
	@JsonProperty(value="fechaInicio")
	private String fechaInicio;
	
	@JsonProperty(value="fechaFin")
	private String fechaFin;
	
	@JsonProperty(value="idAdministrador")
	private Long idAdministrador;
	
	
	private Administrador administrador;
	
	private ArrayList<Compania> companias;
	
	public Festival(@JsonProperty(value="id")Long id, 
			@JsonProperty(value="fechaInicio")String fechaInicio, 
			@JsonProperty(value="fechaFin")String fechaFin,
			@JsonProperty(value="idAdministrador")Long idAdministrador)
	{
		this.id = id;
		this.fechaInicio = fechaInicio;
		this.fechaFin = fechaFin;
		this.idAdministrador = idAdministrador;
		companias = new ArrayList<Compania>();
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getFechaInicio() {
		return fechaInicio;
	}

	public void setFechaInicio(String fechaInicio) {
		this.fechaInicio = fechaInicio;
	}

	public String getFechaFin() {
		return fechaFin;
	}

	public void setFechaFin(String fechaFin) {
		this.fechaFin = fechaFin;
	}

	public Administrador getAdministrador() {
		return administrador;
	}

	public void setAdministrador(Administrador administrador) {
		this.administrador = administrador;
	}

	public ArrayList<Compania> getCompanias() {
		return companias;
	}

	public void setCompanias(ArrayList<Compania> companias) {
		this.companias = companias;
	}
	
	public Long getIdAdministrador() {
		return idAdministrador;
	}

	public void setIdAdministrador(Long idAdministrador) {
		this.idAdministrador = idAdministrador;
	}

	public void addCompania(@JsonProperty(value="id")Long id, @JsonProperty(value="nombre")String nombre,
			@JsonProperty(value="representante") String representante, @JsonProperty(value = "paisOrigen") String paisOrigen,
			@JsonProperty(value = "paginaWeb") String paginaWeb, @JsonProperty(value = "fechaLlegada") String fechaLlegada,
			@JsonProperty(value = "fechaSalida") String fechaSalida,
			@JsonProperty(value = "fechaSalida")Long idOrganizador)
	{
		Compania compania = new Compania(id, nombre, representante, paisOrigen, paginaWeb, fechaLlegada, fechaSalida, this.id, idOrganizador);
		companias.add(compania);
	}

}
