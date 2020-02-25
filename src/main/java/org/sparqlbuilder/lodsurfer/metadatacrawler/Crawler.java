package org.sparqlbuilder.lodsurfer.metadatacrawler;

import java.net.URI;
//import java.util.HashSet;

import org.apache.jena.rdf.model.Model;
//import org.apache.jena.rdf.model.Resource;



public interface Crawler {
	
	public URI endpointURI = null;
	
	public URI[] getGraphURIs() throws Exception;
	public void crawl(URI[] graphURIs) throws Exception;

	
	
	
//	public SchemaCategory determineSchemaCategory(Model model, Resource datasetResource) throws Exception;
	public SchemaCategory determineSchemaCategory(GraphSchema schema) throws Exception;

//	public GraphSchema getClassSchema(GraphSchema schema, URI[] classURIs) throws Exception;
	public GraphSchema getClassSchema(GraphSchema schema) throws Exception;
	
//	public GraphSchema getPropertySchema(Model model, Resource datasetRes, URI[] propertyURIs, HashSet<String> datatypeSet) throws Exception;
	public GraphSchema getPropertySchema(GraphSchema schema) throws Exception;
	

	public Model getPropertiesFromInstanceDecls() throws Exception;
	
	public Model getProperiesFromDomainRangeDecls() throws Exception;
	public URI[] getDeclaredRDFsClasses() throws Exception;
//	public String[] getInferedRDFsClassesFromInstances() throws Exception;
	public URI[] getRDFProperties(URI[] graphURIs) throws Exception;
//	public String[] getDatatypes(URI[] propertyURIs) throws Exception;


}
