package org.sparqlbuilder.lodsurfer.metadatacrawler;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Set;

import org.apache.jena.rdf.model.Model;

public class EndpointSchema {

	public Model model = null;
	private HashMap<URI, GraphSchema> graphSchemaTable = null;
	int endpointCategory = 0;
	int endpointAccessCount = 0;
	
	public void close() {
		graphSchemaTable.clear();
		graphSchemaTable = null;
		model.close();
		model = null;
	}
	
	
	public void write2File(String fileName, String lang) throws Exception{
		if( model != null && fileName != null ){
			if( lang == null ) {
				lang = "RDF/XML-ABBREV";
			}
			File file = new File(fileName);
			FileOutputStream fos = new FileOutputStream(file);
			model.write(fos, lang,"http://sparqlbuilder.org/");
		}
	}

	
	public int getEndpointCategory() {
		return endpointCategory;
	}

	public void setEndpointCategory(int endpointCategory) {
		this.endpointCategory = endpointCategory;
	}

	public int getEndpointAccessCount() {
		return endpointAccessCount;
	}

	public void setEndpointAccessCount(int endpointAccessCount) {
		this.endpointAccessCount = endpointAccessCount;
	}

	public EndpointSchema(Model model) {
		this.model = model;
		graphSchemaTable = new HashMap<URI, GraphSchema>();
	}

	public EndpointSchema(Model model, HashMap<URI, GraphSchema> graphSchemaTable) {
		this.model = model;
		this.graphSchemaTable = graphSchemaTable;
	}
	
	public void putGraphSchema(URI graphURI, GraphSchema graphSchema) {
		graphSchemaTable.put(graphURI, graphSchema);
	}

	public GraphSchema getGraphSchema(URI graphURI) {
		return graphSchemaTable.get(graphURI);
	}
	
	public Set<URI> getGraphURISet() {
		return graphSchemaTable.keySet();
	}

	public boolean conains(URI graphURI) {
		return graphSchemaTable.containsKey(graphURI);
	}
	
}
