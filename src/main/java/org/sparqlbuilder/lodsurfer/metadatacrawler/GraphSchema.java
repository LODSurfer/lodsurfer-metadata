package org.sparqlbuilder.lodsurfer.metadatacrawler;

import java.net.URI;
import java.util.HashSet;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;

public class GraphSchema {

	public Model model = null;
	public Resource datasetResource = null;
	public int propertyCategory = 0;
	public int classCategory = 0;
	public int[] countProperties;
	public int graphCategory = 0;
	public int numTriples = 0;
	public HashSet<URI> inferedClasses; // internally used
	public HashSet<String> datatypes;
	public URI[] classURIs;
	public URI[] propertyURIs;
	public URI graphURI = null;
	
	public GraphSchema(Model model, URI graphURI, Resource datasetResource) {
		this.model = model;
		this.graphURI = graphURI;
		this.datasetResource = datasetResource;
	}

	
	public GraphSchema(Model model, Resource datasetResource, int propertyCategory, int[] countProperties,
			int classCategory, int graphCategory, int numTriples,
			HashSet<URI> inferedClasses, HashSet<String> datatypes) {
		this.model = model;
		this.datasetResource = datasetResource;
		this.propertyCategory = propertyCategory;
		this.countProperties = countProperties;
		this.classCategory = classCategory;
		this.graphCategory = graphCategory;
		this.numTriples = numTriples;
		this.inferedClasses = inferedClasses;
		this.datatypes = datatypes;
	}




}
