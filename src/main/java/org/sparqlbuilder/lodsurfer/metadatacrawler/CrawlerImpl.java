package org.sparqlbuilder.lodsurfer.metadatacrawler;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;

public class CrawlerImpl {

	long INTERVAL = 100L;
	long INTERVAL_ERROR = 5000L;

	static String version = "20200221";

	URI endpointURI = null;
	String crawlName = null;
	File outDir = null;
	// URI[] graphURIs = null;
	String[] graphURIFilter = URICollection.FILTER_GRAPH;
	String userGraphURIFiler = "^http://metadb.riken.jp/structure.*|^http://metadb.riken.jp/system.*";

	int metadataLayer = 3; // 2: simple, 3: obtains statistical data

	public static void main(String[] args) throws Exception {

		System.out.println("  Version: " + version);

		if (args.length == 2 && args[0].equals("-g")) {
			String endPURIStr = args[1];
			URI endPURI = new URI(endPURIStr);
			// graphlist
			CrawlerImpl impl = new CrawlerImpl(endPURI);
			URI[] graphURIs = null;
			graphURIs = impl.getGraphURIs();
			if (graphURIs != null) {
				Arrays.sort(graphURIs);
				for (URI graphURI : graphURIs) {
					System.out.println("Graph: " + graphURI);
				}
			}
			return;
		}

		if (args.length == 4 && (args[0].equals("-e2") || args[0].equals("-e3"))) {
			String endPURIStr = args[1];
			URI endPURI = new URI(endPURIStr);
			String crawlName = args[2];
			String outDir = args[3];
			CrawlerImpl impl = new CrawlerImpl(endPURI, crawlName, outDir);
			if (args[0].equals("-e2")) {
				impl.metadataLayer = 2;
			} else {
				if (args[0].equals("-e3")) {
					impl.metadataLayer = 3;
				}
			}

			URI[] graphURIs = null;
			graphURIs = impl.getGraphURIs();
			if (graphURIs == null || graphURIs.length == 0) {
				graphURIs = new URI[0];
			} else {
				Arrays.sort(graphURIs);
				if (!Arrays.asList(graphURIs).contains((URI) null)) {
					URI[] extGraphURIs = new URI[graphURIs.length + 1];
					extGraphURIs[graphURIs.length] = null;
					for (int i = 0; i < graphURIs.length; i++) {
						extGraphURIs[i] = graphURIs[i];
					}
					graphURIs = extGraphURIs;
				}
			}
			impl.crawl(graphURIs, true, false);
			return;
		}

		if (args.length == 4 && (args[0].equals("-d2") || args[0].equals("-d3"))) {
			String endPURIStr = args[1];
			URI endPURI = new URI(endPURIStr);
			String crawlName = args[2];
			String outDir = args[3];
			// graphlist
			CrawlerImpl impl = new CrawlerImpl(endPURI, crawlName, outDir);
			if (args[0].equals("-d2")) {
				impl.metadataLayer = 2;
			} else {
				if (args[0].equals("-d3")) {
					impl.metadataLayer = 3;
				}
			}
			URI[] graphURIs = new URI[1];
			graphURIs[0] = null;
			impl.crawl(graphURIs, false, false);
			return;
		}

		if (args.length == 4 && (args[0].equals("-a2") || args[0].equals("-a3"))) {
			String endPURIStr = args[1];
			URI endPURI = new URI(endPURIStr);
			String crawlName = args[2];
			String outDir = args[3];
			CrawlerImpl impl = new CrawlerImpl(endPURI, crawlName, outDir);
			if (args[0].equals("-a2")) {
				impl.metadataLayer = 2;
			} else {
				if (args[0].equals("-a3")) {
					impl.metadataLayer = 3;
				}
			}

			URI[] graphURIs = null;
			graphURIs = impl.getGraphURIs();
			if (graphURIs == null || graphURIs.length == 0) {
				graphURIs = new URI[1];
				graphURIs[0] = null;
			} else {
				Arrays.sort(graphURIs);
				if (!Arrays.asList(graphURIs).contains((URI) null)) {
					URI[] extGraphURIs = new URI[graphURIs.length + 1];
					extGraphURIs[graphURIs.length] = null;
					for (int i = 0; i < graphURIs.length; i++) {
						extGraphURIs[i] = graphURIs[i];
					}
					graphURIs = extGraphURIs;
				}
				for (URI graphURI : graphURIs) {
					System.out.println("Graph: " + graphURI);
				}
			}
			impl.crawl(graphURIs, false, false);
			return;
		}

		if (args.length == 5 && (args[0].equals("-g2") || args[0].equals("-g3"))) {
			// boolean errorState = false;
			String endPURIStr = args[1];
			URI endPURI = new URI(endPURIStr);
			String crawlName = args[2];
			String targetGraphURIStr = args[3];
			URI targetGraphURI = new URI(targetGraphURIStr);
			String outDir = args[4];
			CrawlerImpl impl = new CrawlerImpl(endPURI, crawlName, outDir);
			if (args[0].equals("-g2")) {
				impl.metadataLayer = 2;
			} else {
				if (args[0].equals("-g3")) {
					impl.metadataLayer = 3;
				}
			}

			URI[] graphURIs = null;
			graphURIs = impl.getGraphURIs();
			if (graphURIs != null) {
				for (URI graphURI : graphURIs) {
					if (graphURI.equals(targetGraphURI)) {
						graphURIs = new URI[1];
						graphURIs[0] = targetGraphURI;
						impl.crawl(graphURIs, false, false);
					}
				}
			}
			return;
		}
		if (args.length == 5 && args[0].equals("-r")) {
			String endPURIStr = args[1];
			URI endPURI = new URI(endPURIStr);
			String crawlName = args[2];
			String outDir = args[3];
			CrawlerImpl impl = new CrawlerImpl(endPURI, crawlName, outDir);
			URI[] graphURIs = null;
			graphURIs = impl.getGraphURIs();
			if (graphURIs == null || graphURIs.length == 0) {
				graphURIs = new URI[0];
			} else {
				Arrays.sort(graphURIs);
				if (!Arrays.asList(graphURIs).contains((URI) null)) {
					URI[] extGraphURIs = new URI[graphURIs.length + 1];
					extGraphURIs[graphURIs.length] = null;
					for (int i = 0; i < graphURIs.length; i++) {
						extGraphURIs[i] = graphURIs[i];
					}
					graphURIs = extGraphURIs;
				}
			}
			impl.crawl(graphURIs, false, true);
			return;
		}
		// usage
		printUsage();
	}

	private static void printUsage() throws Exception {
		System.out.println("Usage: java org.sparqlbuilder.metadata.crawler.sparql.RDFsCrawlerImpl [options]");
		System.out.println("   [options]");
		System.out.println("       1. to print a list of graphURIs");
		System.out.println("            -g endpointURL");
		System.out.println("       2. to crawl whole data in the endpoint");
		System.out.println("            -a[2/3] endpointURL crawlName outputDirName");
		System.out.println("       3. to crawl the specified graph in the endpoint");
		System.out.println("            -g[2/3] endpointURL crawlName graphURI outputDirName");
		System.out.println("       4. to crawl the default named graph in the endpoint");
		System.out.println("            -d[2/3] endpointURL crawlName outputFileDirName");
		System.out.println("       5. to recovery the previous crawl task");
		System.out.println("            -r endpointURL crawlName outputFileDirName");

	}

	private EndpointSchema createInitEndpointSchema(URI[] graphURIs) {
		Model wholeModel = null;
		wholeModel = ModelFactory.createDefaultModel();

		EndpointSchema endpointSchema = new EndpointSchema(wholeModel);

		Property endpointPro = wholeModel.createProperty(URICollection.PROPERTY_SD_ENDPOINT);
		Resource serviceRes = wholeModel.createResource(URICollection.RESOURCE_SD_SERVICE);
		Resource endpointRes = wholeModel.createResource(endpointURI.toString());
		Property typePro = wholeModel.createProperty(URICollection.PROPERTY_RDF_TYPE);

		Resource thisRes = wholeModel.createResource("");
		thisRes.addProperty(typePro, serviceRes);
		thisRes.addProperty(endpointPro, endpointRes);

		// dataset
		Property defaultDatasetPro = wholeModel.createProperty(URICollection.PROPERTY_SD_DEFAULT_DATA_SET);
		Resource datasetBlankRes = wholeModel.createResource(AnonId.create());
		thisRes.addProperty(defaultDatasetPro, datasetBlankRes);
		Resource datasetRes = wholeModel.createResource(URICollection.RESOURCE_SD_DATA_SET);
		datasetBlankRes.addProperty(typePro, datasetRes);
		endpointSchema.putGraphSchema(null, new GraphSchema(wholeModel, null, datasetBlankRes));

		if (graphURIs != null && graphURIs.length != 0) {
			Property namedGraphPro = wholeModel.createProperty(URICollection.PROPERTY_SD_NAMED_GRAPH);
			Resource namedGraphRes = wholeModel.createResource(URICollection.RESOURCE_SD_NAMED_GRAPH);
			Property namePro = wholeModel.createProperty(URICollection.PROPERTY_SD_NAME);
			Property graphPro = wholeModel.createProperty(URICollection.PROPERTY_SD_GRAPH);
			Resource graphRes = wholeModel.createResource(URICollection.RESOURCE_SD_GRAPH);
			Resource voidDatasetRes = wholeModel.createResource(URICollection.RESOURCE_VOID_DATASET);
			for (URI graphURI : graphURIs) {
				if (graphURI != null) {
					System.out.println(graphURI);
					Resource gRes = wholeModel.createResource(AnonId.create());
					datasetBlankRes.addProperty(namedGraphPro, gRes);
					gRes.addProperty(typePro, namedGraphRes);
					Resource gIRI = wholeModel.createResource(graphURI.toString());
					gRes.addProperty(namePro, gIRI);
					Resource sdgRes = wholeModel.createResource(AnonId.create());
					gRes.addProperty(graphPro, sdgRes);
					sdgRes.addProperty(typePro, voidDatasetRes);
					sdgRes.addProperty(typePro, graphRes);
					endpointSchema.putGraphSchema(graphURI, new GraphSchema(wholeModel, graphURI, sdgRes));
				} else {

				}
			}
		}
		return endpointSchema;
	}

	public void crawl(URI[] graphURIs, boolean wholeEndpointMode, boolean recoveryMode) throws Exception {
		if (wholeEndpointMode) {
			String resultTurtleFileName = new File(outDir, crawlName + ".ttl").getCanonicalPath();
			crawl(graphURIs, resultTurtleFileName, recoveryMode);
		} else {
			int gCount = 0;
			for (URI graphURI : graphURIs) {
				gCount++;
				String dbURI = null;
				if (graphURI != null) {
					String graphURIStr = graphURI.toString();
					dbURI = graphURIStr.substring(graphURIStr.lastIndexOf("/") + 1);
					dbURI = dbURI.replaceAll("\\.", "_");
					dbURI = dbURI.replaceAll("\\:", "_");
					if (dbURI.length() > 32) {
						dbURI = dbURI.substring(0, 32);
					}
				} else {
					dbURI = "";
				}
				String resultTurtleFileName = new File(outDir, crawlName + "_" + dbURI + ".ttl").getCanonicalPath();
				System.out.println(resultTurtleFileName);
				
				URI[] tempURIs = new URI[] { graphURI };
				System.out.println("-----------------------------------------------------------");
				System.out.println("  Graph: " + graphURI + "        " + gCount + " / " + graphURIs.length);
				System.out.println("-----------------------------------------------------------");
				crawl(tempURIs, resultTurtleFileName, recoveryMode);
			}
		}
	}

	private void crawl(URI[] graphURIs, String outputFileName, boolean recoveryMode) throws Exception {

		if (graphURIs.length == 0) {
			// error
			System.err.println("Internal error occured.");
		} else {

			EndpointSchema endpointSchema = createInitEndpointSchema(graphURIs);
			// int endpointAccessCount = 0;

			boolean errorState = false;

			// String[] extGraphURIs = new String[graphURIs.length + 1];
			// extGraphURIs[0] = null;
			// for (int i = 0; i < graphURIs.length; i++) {
			// extGraphURIs[i + 1] = graphURIs[i];
			// }
			// graphURIs = extGraphURIs;

			/*
			 * String recoverLogFileName = new File(outDir, "log_" +
			 * crawlName).getCanonicalPath(); File recoveryLogFile = new
			 * File(recoverLogFileName);
			 * 
			 * FileWriter recoveryLogFileWriter = null; BufferedWriter bw = null; FileReader
			 * recoveryLogFileReader = null; BufferedReader br = null; HashSet<String>
			 * recoveryGraphSet = null; if (recoveryMode) { recoveryLogFileReader = new
			 * FileReader(recoveryLogFile); br = new BufferedReader(recoveryLogFileReader);
			 * recoveryGraphSet = new HashSet<String>(); String buf = null; while ((buf =
			 * br.readLine()) != null) { buf = buf.trim(); if (buf.startsWith("http")) {
			 * recoveryGraphSet.add(buf); } } br.close(); } recoveryLogFileWriter = new
			 * FileWriter(recoveryLogFile); bw = new BufferedWriter(recoveryLogFileWriter);
			 */

			for (URI graphURI : graphURIs) {
				// if (!recoveryMode || (recoveryMode &&
				// recoveryGraphSet.contains(graphURI.toString()))) {
				// TODO
				if (!recoveryMode) {
					errorState = false;
					Date startDate = new Date();
					long start = startDate.getTime();

					GraphSchema graphSchema = endpointSchema.getGraphSchema(graphURI);

					try {
						graphSchema = determineSchemaCategory(graphSchema);
						System.out.println("GraphCategory:" + graphSchema.graphCategory);

					} catch (Exception ex) {
						ex.printStackTrace();
						errorState = true;
/*						
						// reovery
						bw.write(graphURI.toString());
						bw.newLine();
						bw.flush();
*/
					}

					Date endDate = new Date();
					long end = endDate.getTime();
					System.out.println((end - start) + " msec.");
					if (errorState) {
						System.err.println("Error occured.");
						// throw new Exception("Error occured s(292)");
					}
				}

			} // end of for
/*
 * 			bw.close();
 */
			
			endpointSchema.write2File(outputFileName, "Turtle");
			System.out.println("OutputFile: " + outputFileName);
			endpointSchema.close();
			endpointSchema = null;
		}
	}

	private synchronized void interval() throws Exception {
		try {
			wait(INTERVAL);
		} catch (InterruptedException e) {
		}
	}

	private synchronized void interval_error() throws Exception {
		try {
			wait(INTERVAL_ERROR);
		} catch (InterruptedException e) {
		}
	}

	// public void setGraphURIs(URI[] graphURIs) {
	// this.graphURIs = graphURIs;
	// }

	public URI[] getGraphURIs() throws Exception {
		// step: getGraphURIs

		Pattern ptn = null;
		try {
			ptn = Pattern.compile(userGraphURIFiler);
		}catch(Exception ex) {
		}
		
		// String stepName = "getGraphURIs";

		// QUERY 1 (metadata layer 2,3)
		// ---------------------------------------------------------------------------------
		// obtains all graphs on the SPARQL endpoint.
		// ---------------------------------------------------------------------------------------
		StringBuffer queryBuffer = new StringBuffer();
		// queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
		// queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
		// queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
		queryBuffer.append("SELECT ?g\n");
		queryBuffer.append("WHERE{\n");
		queryBuffer.append(" GRAPH ?g { ?s ?p ?o.}\n");
		queryBuffer.append("} GROUP BY ?g");

		String queryString = queryBuffer.toString();

		// System.out.println(queryString);

		Query query = QueryFactory.create(queryString);

		QueryExecution qexec = null;
		ResultSet results = null;
		//String[] recoveryStrings = null;
		try {
			// long start = System.currentTimeMillis();
			qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
			interval();
			results = qexec.execSelect();
			// long end = System.currentTimeMillis();
			// System.out.println("EXEC TIME: " + (end - start));
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}

		// results
		ArrayList<URI> graphList = new ArrayList<URI>();
		/*
		if (recoveryStrings != null) {
			for (String uri : recoveryStrings) {
				if (uriFilter(uri, graphURIFilter) != null) {
					graphList.add(new URI(uri));
				}
			}
		*/
		// } else {
		ArrayList<String> forLog = new ArrayList<String>();
		for (; results.hasNext();) {
			QuerySolution sol = results.next();
			// label
			Resource graph = sol.getResource("g");
			forLog.add(graph.getURI());
			Matcher matcher = ptn.matcher(graph.getURI());
			if( !matcher.find() ) {
				if (uriFilter(graph.getURI(), graphURIFilter) != null) {
					graphList.add(new URI(graph.getURI()));
				}
			}
		}
		// }
		qexec.close();

		URI[] resultList = graphList.toArray(new URI[0]);
		return resultList;
	}

	int endpointAccessCount = 0;

	public GraphSchema determineSchemaCategory(GraphSchema schema) throws Exception {
		Calendar start = GregorianCalendar.getInstance();
		URI graphURI = schema.graphURI;
		URI[] res1 = null;
		
		int cCount = endpointAccessCount;
		
		// Metadata Layer 2,3
		if (metadataLayer == 2 || metadataLayer == 3) {
			// try {
			res1 = getRDFProperties(graphURI);
			// } catch (Exception ex) {
			// HashSet<String> res1Set = new HashSet<String>();
			// for (String gURI : graphURIs) {
			// String[] ss = getRDFProperties(gURI);
			// if (ss != null) {
			// for (String s : ss) {
			// res1Set.add(s);
			// }
			// }
			// }
			// res1 = res1Set.toArray(new String[0]);
			// }

			// obtains a list of properties
			System.out.println("\nproperties");
			for (URI uri : res1) {
				System.out.println(uri);
			}
			System.out.println();
		}

		// obtains datatypeSet: this is used to filter classes.
		// What these codes here do is the set of declared classes minus the set of obtained datatypes.
		String[] datatypes = null;
		// Metadata Layer 2,3
		if (metadataLayer == 2 || metadataLayer == 3) {
			try {
				datatypes = getDatatypes(graphURI, res1);
			} catch (Exception ex) {
				ex.printStackTrace();
				datatypes = null;
			}
		}

		// obtains a list of classes
		URI[] res3 = null;
		if (metadataLayer == 2 || metadataLayer == 3) {
			try {
				res3 = getDeclaredRDFsClasses(graphURI, datatypes == null);
			} catch (Exception ex) {
				// TODO
				// throw ex;
			}
		}
		// filter out classes used for datatypes of literal values such as xsd:integer
		if (datatypes != null) {
			HashSet<String> datatypeSet = new HashSet<String>();
			for (String datatype : datatypes) {
				datatypeSet.add(datatype);
			}
			ArrayList<URI> tmpStrList = new ArrayList<URI>();
			for (URI uri : res3) {
				if (!datatypeSet.contains(uri.toString())) {
					tmpStrList.add(uri);
				}
			}
			res3 = tmpStrList.toArray(new URI[0]);
		}

		if (res3 != null) {
			System.out.println("classes");
			for (URI uri : res3) {
				System.out.println(uri);
			}
			System.out.println();
		}

		if (res3 != null) {
			System.out.println("#Decl Class(total): " + res3.length);
		}
		if (res1 != null) {
			System.out.println("#Decl Property(total): " + res1.length);
		}

		//
		// Further schema analysis
		//

		// Model model = null;
		// model = ModelFactory.createDefaultModel();
		// Property endpointPro = model
		// .createProperty(URICollection.PROPERTY_SD_ENDPOINT);
		// Resource serviceRes = model
		// .createResource(URICollection.RESOURCE_SD_SERVICE);
		// Resource endpointRes = model.createResource(endpointURI);
		// Property typePro = model
		// .createProperty(URICollection.PROPERTY_RDF_TYPE);
		//
		// Resource thisRes = model.createResource("");
		// thisRes.addProperty(typePro, serviceRes);
		// thisRes.addProperty(endpointPro, endpointRes);
		//
		// // dataset
		// Property defaultDatasetPro = model
		// .createProperty(URICollection.PROPERTY_SD_DEFAULT_DATA_SET);
		// Resource datasetBlankRes = model.createResource(AnonId.create());
		// thisRes.addProperty(defaultDatasetPro, datasetBlankRes);
		// Resource datasetRes = model
		// .createResource(URICollection.RESOURCE_SD_DATA_SET);
		// datasetBlankRes.addProperty(typePro, datasetRes);
		// // graphs
		// if (graphURIs != null && graphURIs.length != 0) {
		// Property namedGraphPro = model
		// .createProperty(URICollection.PROPERTY_SD_NAMED_GRAPH);
		// Resource namedGraphRes = model
		// .createResource(URICollection.RESOURCE_SD_NAMED_GRAPH);
		// Property namePro = model
		// .createProperty(URICollection.PROPERTY_SD_NAME);
		// for (String graphURI : graphURIs) {
		// Resource gRes = model.createResource(AnonId.create());
		// datasetBlankRes.addProperty(namedGraphPro, gRes);
		// gRes.addProperty(typePro, namedGraphRes);
		// Resource gIRI = model.createResource(graphURI);
		// gRes.addProperty(namePro, gIRI);
		// }
		// }

		if (metadataLayer == 2 || metadataLayer == 3) {
			if (res1.length != 0 && res3.length != 0) {
				schema.propertyURIs = res1;
				schema.classURIs = res3;
				schema = getPropertySchema(schema);
				if (metadataLayer == 3) {
					if (schema.propertyCategory == 4) {
						schema.classCategory = 3;
					} else {
						schema = getClassSchema(schema);
					}
					if (schema.propertyCategory <= 2 || schema.classCategory == 1) {
						schema.graphCategory = 1;
					} else {
						if (schema.classCategory == 3) {
							schema.graphCategory = 3;
						} else {
							schema.graphCategory = 2;
						}
					}
				}
			} else {
				schema.propertyCategory = 4;
				schema.countProperties = null;
				schema.classCategory = 3;
				schema.graphCategory = 3;
				schema.numTriples = 0;
				schema.inferedClasses = null;
				schema.datatypes = null;
			}
			System.out.println(
					"PropertyCategory: " + schema.propertyCategory + "  ClassCategory: " + schema.classCategory);
		}

		// triples
		// int numTriples = schema.numTriples;
		// Property triplesPro =
		// model.createProperty(URICollection.PROPERTY_VOID_TRIPLES);
		// schema.datasetResource.addLiteral(triplesPro, numTriples);

		// classes
		// int numClasses = schema.inferedClasses.size();
		// Property classesPro =
		// model.createProperty(URICollection.PROPERTY_VOID_CLASSES);
		// schema.datasetResource.addLiteral(classesPro, numClasses);

		// datatypes
		// Property datatypesPro =
		// model.createProperty(URICollection.PROPERTY_SB_DATATYPES);
		// int numDatatypes = schema.datatypes.size();
		// schema.datasetResource.addLiteral(datatypesPro, numDatatypes);

		Model wholeModel = schema.model;
		Calendar end = GregorianCalendar.getInstance();

		writeLogTriples(schema.model, schema.datasetResource, start, end, (endpointAccessCount-cCount));
		
	
		if (metadataLayer == 3) {
			Property catPro = wholeModel.createProperty(URICollection.PROPERTY_SB_GRAPH_CATEGORY);
			// schema.datasetResource.addLiteral(catPro, schema.endpointCategory);
			schema.datasetResource.addLiteral(catPro, schema.graphCategory);
			Property clsCatPro = wholeModel.createProperty(URICollection.PROPERTY_SB_CLASS_CATEGORY);
			schema.datasetResource.addLiteral(clsCatPro, schema.classCategory);
			Property proCatPro = wholeModel.createProperty(URICollection.PROPERTY_SB_PROPERTY_CATEGORY);
			schema.datasetResource.addLiteral(proCatPro, schema.propertyCategory);
		}

		// Property numTriplesPro = model
		// .createProperty(URICollection.PROPERTY_SB_NUMBER_OF_TRIPLES);
		// datasetRes.addLiteral(numTriplesPro, schema.numTriples);

		System.out.println("#EndpointAccess: " + (endpointAccessCount-cCount));

		return schema;
	}

	private void writeLogTriples(Model model, Resource datasetResource, Calendar start, Calendar end, int endpointCount) {
				Property typePro = model.createProperty(URICollection.PROPERTY_RDF_TYPE);
				Resource crawlLogRes = model.createResource(URICollection.RESOURCE_SB_CRAWL_LOG);
				Property crawlLogPro = model.createProperty(URICollection.PROPERTY_SB_CRAWL_LOG);
				Resource clBlankRes = model.createResource(AnonId.create());
				datasetResource.addProperty(crawlLogPro, clBlankRes);
				clBlankRes.addProperty(typePro, crawlLogRes);
				Property crawlStartTimePro = model.createProperty(URICollection.PROPERTY_SB_CRAWL_START_TIME);
				clBlankRes.addLiteral(crawlStartTimePro, model.createTypedLiteral(start));
				Property crawlEndTimePro = model.createProperty(URICollection.PROPERTY_SB_CRAWL_END_TIME);
				clBlankRes.addLiteral(crawlEndTimePro, model.createTypedLiteral(end));
				Property endpointAccessesPro = model.createProperty(URICollection.PROPERTY_SB_ENDPOINT_ACCESSES);
				clBlankRes.addLiteral(endpointAccessesPro, endpointCount);
	}

	public GraphSchema getClassSchema(GraphSchema schema) throws Exception {
		// String[] filterStrs = URICollection.FILTER_CLASS;
		// String[] unfilterStrs = URICollection.UNFILTER_CLASS;

		HashSet<URI> inferedClasses = schema.inferedClasses;
		int classCategory = 1;
		int cnt = 0;
		URI[] classURIs = schema.classURIs;
		for (URI classURI : classURIs) {
			if (inferedClasses.contains(classURI)) {
				cnt++;
			}
		}
		if (cnt == inferedClasses.size()) {
			classCategory = 1;
		} else {
			if (cnt == 0) {
				classCategory = 3;
			} else {
				classCategory = 2;
			}
		}

		Model model = schema.model;
		// create model
		Property labelPro = model.createProperty(URICollection.PROPERTY_RDFS_LABEL);
		Property entitiesPro = model.createProperty(URICollection.PROPERTY_VOID_ENTITIES);
		Property classPartitionPro = model.createProperty(URICollection.PROPERTY_VOID_CLASS_PARTITION);
		Property classPro = model.createProperty(URICollection.PROPERTY_VOID_CLASS);

		ArrayList<Resource> classList = new ArrayList<Resource>();

		// for each class, obtains its labels
		for (URI clsURI : inferedClasses) {
			Resource cls = model.createResource(clsURI.toString());
			Resource cpBlankRes = model.createResource(AnonId.create());
			schema.datasetResource.addProperty(classPartitionPro, cpBlankRes);
			cpBlankRes.addProperty(classPro, cls);

			classList.add(cls);

			// QUERY 4 (Metadata Layer 2,3)
			// ---------------------------------------------------------------------------------
			// obtains all labels associated with the class
			// ---------------------------------------------------------------------------------------
			StringBuffer queryBuffer = new StringBuffer();
			queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
			queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
			queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
			queryBuffer.append("SELECT DISTINCT ?label\n");
			if (schema.graphURI != null) {
				queryBuffer.append("FROM <");
				queryBuffer.append(schema.graphURI.toString());
				queryBuffer.append(">\n");
			}
			queryBuffer.append("WHERE{\n");
			queryBuffer.append("  <" + clsURI + "> rdfs:label ?label.\n");
			queryBuffer.append("}");

			String queryString = queryBuffer.toString();

			// System.out.println(queryString);

			Query query = QueryFactory.create(queryString);

			QueryExecution qexec = null;
			ResultSet results = null;
			try {
				// long start = System.currentTimeMillis();
				qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
				endpointAccessCount++;
				interval();
				results = qexec.execSelect();
				// long end = System.currentTimeMillis();
				// System.out.println("EXEC TIME: " + (end - start));
				System.out.print("L");
			} catch (Exception ex) {
				ex.printStackTrace();
				System.out.println("The error caused by this query:\n" + queryString);
				throw ex;
			}

			for (; results.hasNext();) {
				QuerySolution sol = results.next();
				// label
				Literal labelLiteral = sol.getLiteral("label");
				if (labelLiteral != null) {
					String label = labelLiteral.getString();
					cls.addLiteral(labelPro, label);
				}
			}
			qexec.close();

			// QUERY 6 (Matadata Layer 3)
			// ---------------------------------------------------------------------------------
			// obtains the number of instances of the class by checking triples
			// <?i,rdf:type,cls>,
			// (<[],?p,?i>,<?p,rdfs:range,cls>), and
			// (<?i,?p,[]>,<?p,rdfs:domain,cls>).
			// ---------------------------------------------------------------------------------------
			if (metadataLayer == 3) {
				queryBuffer = new StringBuffer();
				queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
				queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
				queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
				queryBuffer.append("SELECT (count(DISTINCT ?i) AS ?num) \n");
				if (schema.graphURI != null) {
					queryBuffer.append("FROM <");
					queryBuffer.append(schema.graphURI.toString());
					queryBuffer.append(">\n");
				}
				queryBuffer.append("WHERE{\n");
				queryBuffer.append("  {?i rdf:type ?class.}\n");
				queryBuffer.append(" UNION  {[] ?p ?i.  ?p rdfs:range ?class.}\n");
				queryBuffer.append(" UNION  {?i ?p [].  ?p rdfs:domain ?class.}\n");
				queryBuffer.append(" VALUES ?class {<" + cls.getURI() + ">}\n");
				queryBuffer.append("}");

				queryString = queryBuffer.toString();

				// System.out.println(queryString);

				query = QueryFactory.create(queryString);

				qexec = null;
				results = null;

				int sCount = 3;
				while (sCount > 0) {
					try {
						// long start = System.currentTimeMillis();
						qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
						qexec.setTimeout(-1);
						endpointAccessCount++;
						interval();
						results = qexec.execSelect();
						// long end = System.currentTimeMillis();
						// System.out.println("EXEC TIME: " + (end - start));
						break;
					} catch (Exception ex) {
						sCount--;
						if (sCount == 0) {
							System.out.println("The error caused by this query:\n" + queryString);
							ex.printStackTrace();
							// throw ex;
						} else {
							interval_error();
						}

					}
				}

				// create model
				if (results != null) {
					for (; results.hasNext();) {
						QuerySolution sol = results.next();
						Literal lit = sol.getLiteral("num");
						if (lit != null) {
							int num = lit.getInt();
							cpBlankRes.addLiteral(entitiesPro, num);
						}
					}
				}
				qexec.close();
			}
		}

		// subclassOf
		// matadataLayer 2,3
		StringBuffer queryBuffer = new StringBuffer();
		queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
		queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
		queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
		queryBuffer.append("SELECT DISTINCT ?c ?d \n");
		if (schema.graphURI != null) {
			queryBuffer.append("FROM <");
			queryBuffer.append(schema.graphURI.toString());
			queryBuffer.append(">\n");
		}
		queryBuffer.append("WHERE{\n");
		queryBuffer.append(" ?c rdfs:subclassOf ?d.\n");
		// queryBuffer.append("FILTER(! contains(str(?c),\"openlinksw\"))\n");
		queryBuffer.append("}");

		String queryString = queryBuffer.toString();

		// System.out.println(queryString);

		Query query = QueryFactory.create(queryString);
		QueryExecution qexec = null;
		ResultSet results = null;
		try {
			// long start = System.currentTimeMillis();
			qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
			endpointAccessCount++;
			results = qexec.execSelect();
			// // long end = System.currentTimeMillis();
			// // System.out.println("EXEC TIME: " + (end - start));
			System.out.println("sb");
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}

		Property subClsOf = model.createProperty(URICollection.PROPERTY_RDFS_SUBCLASSOF);
		String[] filterStrs = URICollection.FILTER_CLASS;
		for (; results.hasNext();) {
			QuerySolution sol = results.next();
			Resource cCls = sol.getResource("c");
			Resource pCls = sol.getResource("d");
			if (cCls != null && pCls != null && uriFilter(cCls.getURI(), filterStrs) != null) {
				cCls = model.createResource(cCls.getURI());
				pCls = model.createResource(pCls.getURI());
				cCls.addProperty(subClsOf, pCls);
			}
		}
		qexec.close();

		schema.model = model;
		schema.classCategory = classCategory;
		return schema;
	}
/*
	class ResourceNumberPair {
		public Resource resource = null;
		public int number = 0;

		ResourceNumberPair(Resource resource, int number) {
			this.resource = resource;
			this.number = number;
		}
	}
*/
	public GraphSchema getPropertySchema(GraphSchema schema)
			throws Exception {
		// String[] filterStrs = { "http://www.openlinksw" };

		
		URI[] classURIs = schema.classURIs;
		URI[] propertyURIs = schema.propertyURIs;
		Model model = schema.model;
		URI graphURI = schema.graphURI;
		Resource datasetRes = schema.datasetResource;
		
		HashSet<String> declClassURISet = new HashSet<String>();
		if (classURIs != null) {
			for (URI classURI : classURIs) {
				declClassURISet.add(classURI.toString());
			}
		}

		Resource literalCls = model.createResource(URICollection.RESOURCE_RDFS_LITERAL);

		// create model
		Property domainPro = model.createProperty(URICollection.PROPERTY_RDFS_DOMAIN);
		Property rangePro = model.createProperty(URICollection.PROPERTY_RDFS_RANGE);

		// Property inferedDomainPro = model
		// .createProperty("http://sparqlbuilder.org/inferedDomain");
		// Property inferedRangePro = model
		// .createProperty("http://sparqlbuilder.org/inferedRange");

		Property typePro = model.createProperty(URICollection.PROPERTY_RDF_TYPE);
		Property labelPro = model.createProperty(URICollection.PROPERTY_RDFS_LABEL);
		Resource clsCls = model.createResource(URICollection.RESOURCE_RDFS_CLASS);
		Resource propRes = model.createResource(URICollection.CLASS_RDF_PROPERTY);

		// Property numTriplePro = model
		// .createProperty(URICollection.PROPERTY_SB_NUMBER_OF_TRIPLES);
		Property distinctSubjectsPro = model.createProperty(URICollection.PROPERTY_VOID_DISTINCT_SUBJECTS);
		Property distinctObjectsPro = model.createProperty(URICollection.PROPERTY_VOID_DISTINCT_OBJECTS);

		Property subjectClassesPro = model.createProperty(URICollection.PROPERTY_SB_SUBJECT_CLASSES);
		Property objectClassesPro = model.createProperty(URICollection.PROPERTY_SB_OBJECT_CLASSES);
		Property objectDatatypesPro = model.createProperty(URICollection.PROPERTY_SB_OBJECT_DATATYPES);

		Property classesPro = model.createProperty(URICollection.PROPERTY_VOID_CLASSES);
		// Property datatypesPro = model.createProperty(URICollection.PROPERTY_SB_DATATYPES);

		// Property numDomClsNumInsPro = model
		// .createProperty(URICollection.PROPERTY_SB_NUMBER_OF_INSTANCES_OF_DOMAIN_CLASS);
		// Property numRanClsNumInsPro = model
		// .createProperty(URICollection.PROPERTY_SB_NUMBER_OF_INSTANCES_OF_RANGE_CLASS);
		// Property indCatPro = model
		// .createProperty(URICollection.PROPERTY_SB_INDIVIDUAL_PROPERTY_CATEGORY);

		Property propCatPro = model.createProperty(URICollection.PROPERTY_SB_PROPERTY_CATEGORY);

		Resource classRelationCls = model.createResource(URICollection.RESOURCE_SB_CLASS_RELATION);
		Property subjectClsPro = model.createProperty(URICollection.PROPERTY_SB_SUBJECT_CLASS);
		Property objectClsPro = model.createProperty(URICollection.PROPERTY_SB_OBJECT_CLASS);
		// Property objectDatatypePro =
		// model.createProperty(URICollection.PROPERTY_SB_OBJECT_DATATYPE);
		Property propPro = model.createProperty(URICollection.PROPERTY_VOID_PROPERTY);
		// Resource propProfileCls = model
		// .createResource(URICollection.RESOURCE_SB_PROPERTY_PROFILE);
		// Property isDomClsLimPro = model
		// .createProperty(URICollection.PROPERTY_SB_START_CLASS_LIMITED_Q);
		// Property isRanClsLimPro = model
		// .createProperty(URICollection.PROPERTY_SB_END_CLASS_LIMITED_Q);

		Property propertyPartitionPro = model.createProperty(URICollection.PROPERTY_VOID_PROPERTY_PARTITION);
		Property propertyPro = model.createProperty(URICollection.PROPERTY_VOID_PROPERTY);
		Property triplesPro = model.createProperty(URICollection.PROPERTY_VOID_TRIPLES);
		Property searchableTriplesPro = model.createProperty(URICollection.PROPERTY_SB_SEARCHABLE_TRIPLES);
		Property classRelationPro = model.createProperty(URICollection.PROPERTY_SB_CLASS_RELATION);

		HashSet<URI> classURISet = new HashSet<URI>();
		int wholePropertyCategory = 0;
		int totalNumTriples = 0;
		int[] countProperties = new int[4];
		long[] countAllTriples = new long[4];
		long[] countClassTriples = new long[4];

		int propCnt = 0;
		int propLen = propertyURIs.length;

		// for each property...
		for (URI propURI : propertyURIs) {
			propCnt++;
			System.out.println("(" + propCnt + "/" + propLen + ")  " + propURI);
			HashSet<String> domClassSet = new HashSet<String>();
			HashSet<String> ranClassSet = new HashSet<String>();
			HashSet<String> literalDatatypeSet = new HashSet<String>();
			boolean literalFlag = false;

			try {
				// QUERY 8 (Matadata Layer 2,3)
				// ---------------------------------------------------------------------------------
				// obtains all domain classes of the property by checking
				// triples <prop,rdfs:domain,?d>.
				// ---------------------------------------------------------------------------------------
				StringBuffer queryBuffer = new StringBuffer();
				queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
				queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
				queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
				queryBuffer.append("SELECT DISTINCT ?d\n");
				if (graphURI != null ) {
						queryBuffer.append("FROM <");
						queryBuffer.append(graphURI.toString());
						queryBuffer.append(">\n");
					}
				queryBuffer.append("WHERE{\n");
				queryBuffer.append(" <" + propURI + "> rdfs:domain ?d.\n");
				queryBuffer.append("}");
				String queryString = queryBuffer.toString();
				// System.out.println(queryString);
				Query query = QueryFactory.create(queryString);

				QueryExecution qexec = null;
				ResultSet results = null;
				try {
					// long start = System.currentTimeMillis();
					qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
					endpointAccessCount++;
					interval();
					results = qexec.execSelect();
					// long end = System.currentTimeMillis();
					// System.out.println("EXEC TIME: " + (end - start));
					System.out.print("d");
				} catch (Exception ex) {
					System.out.println("The error caused by this query:\n" + queryString);
					ex.printStackTrace();
					throw ex;
				}
				for (; results.hasNext();) {
					QuerySolution sol = results.next();
					Resource dom = sol.getResource("d");
					if (dom != null && dom.getURI() != null) {
						domClassSet.add(dom.getURI());
					}
				}
				System.out.print("(" + domClassSet.size() + ")");
				qexec.close();

				// range
				// QUERY 9 (Metadata Layer 2,3)
				// ---------------------------------------------------------------------------------
				// obtains all range classes of the property by checking triples
				// <prop,rdfs:range,?r>.
				// ---------------------------------------------------------------------------------------
				queryBuffer = new StringBuffer();
				queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
				queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
				queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
				queryBuffer.append("SELECT DISTINCT ?r\n");
				if (graphURI != null ) {
					queryBuffer.append("FROM <");
					queryBuffer.append(graphURI.toString());
					queryBuffer.append(">\n");
				}
				queryBuffer.append("WHERE{\n");
				queryBuffer.append(" <" + propURI + "> rdfs:range ?r.\n");
				queryBuffer.append("}");
				queryString = queryBuffer.toString();
				// System.out.println(queryString);
				query = QueryFactory.create(queryString);

				qexec = null;
				results = null;
				try {
					// long start = System.currentTimeMillis();
					qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
					endpointAccessCount++;
					interval();
					results = qexec.execSelect();
					// long end = System.currentTimeMillis();
					// System.out.println("EXEC TIME: " + (end - start));
					System.out.print("r");
				} catch (Exception ex) {
					System.out.println("The error caused by this query:\n" + queryString);
					ex.printStackTrace();
					throw ex;
				}
				for (; results.hasNext();) {
					QuerySolution sol = results.next();
					Resource ran = sol.getResource("r");
					if (ran != null && ran.getURI() != null) {
						// remove rdfs:literal
						if (!ran.getURI().equals(URICollection.RESOURCE_RDFS_LITERAL)) {
							if (declClassURISet.contains(ran.getURI())) {
								ranClassSet.add(ran.getURI());
							}
						}
					}
				}
				System.out.print("(" + ranClassSet.size() + ")");
				qexec.close();

				// inference
				ArrayList<PropertyDomainRangeDecl> propDomRanDeclList = new ArrayList<PropertyDomainRangeDecl>();
				ArrayList<PropertyDomainRangeDecl> propDomRanDeclList2 = new ArrayList<PropertyDomainRangeDecl>();
				// QUERY 10 (Metadata Layer 2,3)
				// ---------------------------------------------------------------------------------
				queryBuffer = new StringBuffer();
				queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
				queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
				queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
				// queryBuffer
				// .append("SELECT ?d ?r (count(?i ?o) AS ?numTriples)
				// (count(DISTINCT ?i) AS ?numDomIns) (count(DISTINCT ?o) AS
				// ?numRanIns)\n");
				queryBuffer.append("SELECT DISTINCT ?d ?r\n");

				if (graphURI != null ) {
					queryBuffer.append("FROM <");
					queryBuffer.append(graphURI.toString());
					queryBuffer.append(">\n");
				}
				queryBuffer.append("WHERE{\n");
				queryBuffer.append("   ?i <" + propURI + "> ?o. \n");
				queryBuffer.append("   ?i rdf:type ?d.\n");
				queryBuffer.append("   ?o rdf:type ?r.\n");
				queryBuffer.append("}");
				queryString = queryBuffer.toString();
				// System.out.println(queryString);
				query = QueryFactory.create(queryString);

				qexec = null;
				results = null;

				int sCount = 3;
				while (sCount > 0) {

					try {
						// long start = System.currentTimeMillis();
						qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
						endpointAccessCount++;
						qexec.setTimeout(-1);
						interval();
						results = qexec.execSelect();
						// long end = System.currentTimeMillis();
						// System.out.println("EXEC TIME: " + (end - start));
						System.out.print("i");
						break;
					} catch (Exception ex) {
						sCount--;
						if (sCount == 0) {
							System.out.println("The error caused by this query:\n" + queryString);
							ex.printStackTrace();
							throw ex;
						} else {
							interval_error();
						}

					}

				}
				// domain-range pair
				System.out.println("Sol: " + results.getRowNumber());
				for (; results.hasNext();) {
					QuerySolution sol = results.next();
					Resource dom = sol.getResource("d");
					Resource ran = sol.getResource("r");
					String domURI = null;
					if (dom != null) {
						domURI = dom.getURI();
					}
					PropertyDomainRangeDecl pdrd = null;
					String ranURI = null;
					if (ran != null) {
						if (!ran.getURI().equals(URICollection.RESOURCE_RDFS_LITERAL)
								&& declClassURISet.contains(ran.getURI())) {
							ranURI = ran.getURI();
							pdrd = new PropertyDomainRangeDecl(propURI, domURI, ranURI, null, 0, 0, 0, false, false);

						} else {
							pdrd = new PropertyDomainRangeDecl(propURI, domURI, null,
									URICollection.RESOURCE_RDFS_LITERAL, 0, 0, 0, false, false);

						}
					}

					// System.out.println(domURI + "--" + ranURI);
					// if( dom == null && ran == null ){
					// System.out.println("NullNull");
					// }

					if (!propDomRanDeclList.contains(pdrd)) {
						propDomRanDeclList.add(pdrd);
					} else {
						System.err.println("ERROR: duplicate PDRD found! (L1144): ");
						System.out.println(pdrd.getDomainClass() + "-----" + pdrd.getRangeClass() + ","
								+ pdrd.getLiteralDataType());
					}
				}
				System.out.println("done");
				qexec.close();

				// literal
				// QUERY 11 (Metadata Layer 2,3)
				// ---------------------------------------------------------------------------------
				queryBuffer = new StringBuffer();
				queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
				queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
				queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
				// queryBuffer
				// .append("SELECT ?d ?r (count(?i) AS ?numTriples)
				// (count(DISTINCT ?i) AS ?numDomIns) (count(DISTINCT ?o) AS
				// ?numRanIns)\n");
				queryBuffer.append("SELECT DISTINCT ?d \n");

				if (graphURI != null ) {
					queryBuffer.append("FROM <");
					queryBuffer.append(graphURI.toString());
					queryBuffer.append(">\n");
				}
				queryBuffer.append("# Query 11\n");
				queryBuffer.append("WHERE{\n");
				queryBuffer.append("   ?i <" + propURI + "> ?o. \n");
				queryBuffer.append("   ?i rdf:type ?d.\n");
				queryBuffer.append("   FILTER( isLiteral(?o) ). }");
				queryString = queryBuffer.toString();
				// System.out.println(queryString);
				query = QueryFactory.create(queryString);

				qexec = null;
				results = null;

				// TODO
				// ---------------------------------------------------------
				// Virtuoso
				// virtuoso error flag
				// ---------------------------------------------------------
				boolean virtuosoErrorFlag = false;

				sCount = 3;
				while (sCount > 0) {

					try {
						// long start = System.currentTimeMillis();
						qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
						qexec.setTimeout(-1);
						endpointAccessCount++;
						interval();
						results = qexec.execSelect();
						// long end = System.currentTimeMillis();
						// System.out.println("EXEC TIME: " + (end - start));
						System.out.print("i");
						break;
					} catch (Exception ex) {
						sCount--;
						if (sCount == 0) {
							System.out.println("The error caused by this query:\n" + queryString);
							// TODO
							// ---------------------------------------------------------
							// Virtuoso
							// virtuoso error flag
							// ---------------------------------------------------------

							ex.printStackTrace();
							// throw ex;
							virtuosoErrorFlag = true;
						} else {
							interval_error();
							interval_error();
						}
					}

				}
				if (virtuosoErrorFlag) {
					System.err.println("Virtuoso ERROR: skip datatype check.");
				} else {
					for (; results.hasNext();) {
						QuerySolution sol = results.next();
						Resource dom = sol.getResource("d");
						String domURI = null;
						if (dom != null) {
							domURI = dom.getURI();
						}
						PropertyDomainRangeDecl pdrd = new PropertyDomainRangeDecl(propURI, domURI, null,
								URICollection.RESOURCE_RDFS_LITERAL, 0, 0, 0, false, false);
						literalFlag = true;
						if (!propDomRanDeclList.contains(pdrd)) {
							propDomRanDeclList.add(pdrd);
						} else {
							System.err.println("ERROR: duplicate PDRD found! (L1259)");
							System.out.println(pdrd.getDomainClass() + "-----" + pdrd.getRangeClass() + ","
									+ pdrd.getLiteralDataType());
						}
					}
				}
				qexec.close();

				// debug
				// for (PropertyDomainRangeDecl pdrd : propDomRanDeclList) {
				// System.out.println(pdrd.getProperty() + " (" +
				// pdrd.getDomainClass()
				// + ",{"+ pdrd.getRangeClass() +
				// ","+pdrd.getLiteralDataType()+"})");
				// }

				System.out.println("\n PDRD (L1224): " + propDomRanDeclList.size());

				for (PropertyDomainRangeDecl pdrd : propDomRanDeclList) {

					if (propDomRanDeclList.size() < 100) {
						System.out.println(pdrd.getDomainClass() + "---" + pdrd.getRangeClass() + ", "
								+ pdrd.getLiteralDataType());
					}

					// QUERY 12 (Metadata Layer 3)
					// ---------------------------------------------------------------------------------
					if (metadataLayer == 3) {
						queryBuffer = new StringBuffer();
						queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
						queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
						queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
						queryBuffer.append(
								"SELECT (count(?i) AS ?numTriples) (count(DISTINCT ?i) AS ?numDomIns) (count(DISTINCT ?o) AS ?numRanIns) \n");

						if (graphURI != null ) {
							queryBuffer.append("FROM <");
							queryBuffer.append(graphURI.toString());
							queryBuffer.append(">\n");
						}
						queryBuffer.append("# Query 12\n");
						queryBuffer.append("WHERE{\n");

						queryBuffer.append("{SELECT ?i ?o WHERE{\n");
						queryBuffer.append("   ?i <" + propURI + "> ?o. \n");
						if (pdrd.getDomainClass() != null) {
							queryBuffer.append(" ?i rdf:type <" + pdrd.getDomainClass() + ">.\n");
						}
						if (pdrd.getRangeClass() != null) {
							queryBuffer.append("?o rdf:type <" + pdrd.getRangeClass() + ">.\n");
						} else {
							if (pdrd.getLiteralDataType() != null) {
								queryBuffer.append("FILTER(isLiteral(?o))\n");
							}
						}
						queryBuffer.append("}}\n");
						queryBuffer.append("}");
						queryString = queryBuffer.toString();
						System.out.println(queryString);

						qexec = null;
						results = null;
						sCount = 5;
						while (sCount > 0) {
							try {
								query = QueryFactory.create(queryString);
								// long start = System.currentTimeMillis();
								qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
								qexec.setTimeout(-1);
								endpointAccessCount++;
								interval();
								results = qexec.execSelect();
								// long end = System.currentTimeMillis();
								// System.out.println("EXEC TIME: " + (end -
								// start));
								System.out.print("p");
								break;
							} catch (Exception ex) {
								sCount--;
								if (sCount == 0) {
									System.out.println("The error caused by this query:\n" + queryString);
									// TODO
									// ---------------------------------------------------------
									// Virtuoso
									// virtuoso error flag
									// ---------------------------------------------------------

									ex.printStackTrace();
									throw ex;
									// virtuosoErrorFlag = true;
								} else {
									interval_error();
								}
							}
						}

						ArrayList<String> domList = new ArrayList<String>();
						ArrayList<String> ranList = new ArrayList<String>();
						String literalDataType = null;

						for (; results.hasNext();) {
							QuerySolution sol = results.next();
							String domURI = pdrd.getDomainClass();
							if (domURI != null) {
								domList.add(domURI);
							}
							if (domURI == null && domClassSet.size() > 0) {
								Iterator<String> it = domClassSet.iterator();
								while (it.hasNext()) {
									domList.add(it.next());
								}
							}

							String ranURI = pdrd.getRangeClass();
							if (ranURI != null) {
								ranList.add(ranURI);
							}
							if (ranURI == null) {
								if (pdrd.getLiteralDataType() != null) {
									literalDataType = pdrd.getLiteralDataType();
								} else {
									if (ranClassSet.size() > 0) {
										Iterator<String> it = ranClassSet.iterator();
										while (it.hasNext()) {
											ranList.add(it.next());
										}
									}
								}
							}

							Literal lit = sol.getLiteral("numTriples");
							int numTriples = 0;
							if (lit != null) {
								numTriples = lit.getInt();
							}
							lit = sol.getLiteral("numDomIns");
							int numDomIns = 0;
							if (lit != null) {
								numDomIns = lit.getInt();
							}
							lit = sol.getLiteral("numRanIns");
							int numRanIns = 0;
							if (lit != null) {
								numRanIns = lit.getInt();
							}

							for (String domURIs : domList) {
								for (String ranURIs : ranList) {
									PropertyDomainRangeDecl pdrd2 = new PropertyDomainRangeDecl(propURI, domURIs,
											ranURIs, null, numTriples, numDomIns, numRanIns, (domURI == null),
											(ranURI == null && literalDataType == null));
									if (!propDomRanDeclList2.contains(pdrd2)) {
										propDomRanDeclList2.add(pdrd2);
									} else {
										System.err.println("ERROR: duplicate PDRD found! (L1396)");
										System.out.println(pdrd2.getDomainClass() + "-----" + pdrd2.getRangeClass()
												+ "," + pdrd2.getLiteralDataType());

									}
								}
								if (literalDataType != null) {
									PropertyDomainRangeDecl pdrd2 = new PropertyDomainRangeDecl(propURI, domURIs, null,
											literalDataType, numTriples, numDomIns, numRanIns, (domURI == null), false);
									if (!propDomRanDeclList2.contains(pdrd2)) {
										propDomRanDeclList2.add(pdrd2);
									} else {
										System.err.println("ERROR: duplicate PDRD found! (L1405)");
										System.out.println(pdrd2.getDomainClass() + "-----" + pdrd2.getRangeClass()
												+ "," + pdrd2.getLiteralDataType());
									}
								}
							}
						}
						qexec.close();
					}
				} // end of for pdrd

				// DEBUG
				// for (PropertyDomainRangeDecl pdrd : propDomRanDeclList2) {
				// System.out.println(pdrd.getProperty() + " (" +
				// pdrd.getDomainClass()
				// + ",{"+ pdrd.getRangeClass() +
				// ","+pdrd.getLiteralDataType()+"})");
				// }

				int numDomIns = 0;
				int numRanIns = 0;
				int numTriples = 0;
				int numTriplesWithDomClass = 0;
				int numTriplesWithRanClass = 0;
				int numTriplesWithBothClass = 0;
				// numbers of triples, dom instances and ran instances
				// Metadata Layer 3
				if (metadataLayer == 3) {
					System.out.println("Query 12 p1");
					queryBuffer = new StringBuffer();
					queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
					queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
					queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
					queryBuffer.append(
							"SELECT (count(?i) AS ?numTriples) (count(DISTINCT ?i) AS ?numDomIns) (count(DISTINCT ?o) AS ?numRanIns)");
					queryBuffer.append(" (count(?d) AS ?numTriplesWithDom) (count(?r) AS ?numTriplesWithRan) (count(?lo) AS ?numTriplesWithRanL)\n");
					if (graphURI != null ) {
						queryBuffer.append("FROM <");
						queryBuffer.append(graphURI.toString());
						queryBuffer.append(">\n");
					}
					queryBuffer.append("# Query 12 p1\n");
					queryBuffer.append("WHERE{\n");
					queryBuffer.append("   ?i <" + propURI + "> ?o.\n");
					queryBuffer.append("   OPTIONAL { ?i rdf:type ?d. }\n");
					queryBuffer.append("   OPTIONAL { ?o rdf:type ?r. }\n");
					queryBuffer.append("   OPTIONAL { FILTER isLiteral(?o) BIND(?o AS ?lo) }\n");
					queryBuffer.append("}");
					queryString = queryBuffer.toString();
					System.out.println(queryString);
					query = QueryFactory.create(queryString);

					qexec = null;
					results = null;
					try {
						// long start = System.currentTimeMillis();
						qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
						endpointAccessCount++;
						interval();
						results = qexec.execSelect();
						// long end = System.currentTimeMillis();
						// System.out.println("EXEC TIME: " + (end - start));
						System.out.print("n");
					} catch (Exception ex) {
						System.out.println("The error caused by this query:\n" + queryString);
						ex.printStackTrace();
						throw ex;
					}
					if (results.hasNext()) {
						QuerySolution sol = results.next();
						Literal lit = sol.getLiteral("numDomIns");
						if (lit != null) {
							numDomIns = lit.getInt();
						}
						lit = sol.getLiteral("numRanIns");
						if (lit != null) {
							numRanIns = lit.getInt();
						}
						lit = sol.getLiteral("numTriples");
						if (lit != null) {
							numTriples = lit.getInt();
						}
//						lit = sol.getLiteral("numDomInsWithDom");
//						if (lit != null) {
//							numDomInsWithClass = lit.getInt();
//						}
						lit = sol.getLiteral("numTriplesWithDom");
						if (lit != null) {
							numTriplesWithDomClass = lit.getInt();
						}
						lit = sol.getLiteral("numTriplesWithRan");
						if (lit != null) {
							numTriplesWithRanClass = lit.getInt();
						}
						lit = sol.getLiteral("numTriplesWithRanL");
						if (lit != null) {
							numTriplesWithRanClass += lit.getInt();
						}
						// System.out.println("NumTriples: " + numTriples + ",
						// numDomIns: " + numDomIns + ", numRanIns: " + numRanIns);

					}
					qexec.close();

				}

//				int numTriplesWithBothClass = 0;
//				int numDomInsWithClass = 0;
//				int numTriplesWithDomClass = 0;
//				int numRanInsWithClass = 0;
//				int numTriplesWithRanClass = 0;

				// QUERY
				// ---------------------------------------------------------------------------------
				// ---------------------------------------------------------------------------------------
				// Metadata Layer 3
				if (metadataLayer == 3) {
					/*
					System.out.println("Query 12 p2");
					queryBuffer = new StringBuffer();
					queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
					queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
					queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
					queryBuffer
							.append("SELECT (count(DISTINCT ?i) AS ?numDomIns) (count(?i) AS ?numTriplesWithDom) \n");
					if (graphURI != null ) {
						queryBuffer.append("FROM <");
						queryBuffer.append(graphURI.toString());
						queryBuffer.append(">\n");
					}
					queryBuffer.append("# Query 12 p2\n");
					queryBuffer.append("WHERE{\n");
					queryBuffer.append("   ?i <" + propURI + "> ?o. \n");
					queryBuffer.append("   ?i rdf:type ?d. \n");
					queryBuffer.append("}");
					queryString = queryBuffer.toString();
					System.out.println(queryString);

					qexec = null;
					results = null;
					try {
						query = QueryFactory.create(queryString);
						// long start = System.currentTimeMillis();
						qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
						endpointAccessCount++;
						interval();
						results = qexec.execSelect();
						// long end = System.currentTimeMillis();
						// System.out.println("EXEC TIME: " + (end - start));
						System.out.print("t");
					} catch (Exception ex) {
						System.out.println("The error caused by this query:\n" + queryString);
						ex.printStackTrace();
						throw ex;
					}
					if (results.hasNext()) {
						QuerySolution sol = results.next();
						Literal lit = sol.getLiteral("numDomIns");
						if (lit != null) {
//							numDomInsWithClass = lit.getInt();
						}
						lit = sol.getLiteral("numTriplesWithDom");
						if (lit != null) {
							numTriplesWithDomClass = lit.getInt();
						}
					}
					qexec.close();
					*/

					// range
					// QUERY
					// ---------------------------------------------------------------------------------
					// ---------------------------------------------------------------------------------------
					/*
					System.out.println("Query 12 p3");
					queryBuffer = new StringBuffer();
					queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
					queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
					queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
					queryBuffer
							.append("SELECT (count(DISTINCT ?o) AS ?numRanIns) (count(?o) AS ?numTriplesWithRan) \n");

					if (graphURI != null ) {
						queryBuffer.append("FROM <");
						queryBuffer.append(graphURI.toString());
						queryBuffer.append(">\n");
					}
					queryBuffer.append("# Query 12 p3\n");
					queryBuffer.append("WHERE{\n");
					queryBuffer.append("   ?i <" + propURI + "> ?o. \n");
					queryBuffer.append("   ?o rdf:type ?r. \n");
					// queryBuffer.append("UNION{ FILTER(isLiteral(?o)) }\n");
					queryBuffer.append("}");
					queryString = queryBuffer.toString();
					System.out.println(queryString);
					query = QueryFactory.create(queryString);

					qexec = null;
					results = null;
					try {
						// long start = System.currentTimeMillis();
						qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
						endpointAccessCount++;
						interval();
						results = qexec.execSelect();
						// long end = System.currentTimeMillis();
						// System.out.println("EXEC TIME: " + (end - start));
						System.out.print("r");
					} catch (Exception ex) {
						System.out.println("The error caused by this query:\n" + queryString);
						ex.printStackTrace();
						throw ex;
					}
					if (results.hasNext()) {
						QuerySolution sol = results.next();
//						Literal lit = sol.getLiteral("numRanIns");
//						if (lit != null) {
//							numRanInsWithClass = lit.getInt();
//						}
						Literal lit = sol.getLiteral("numTriplesWithRan");
						if (lit != null) {
							numTriplesWithRanClass = lit.getInt();
						}

					}
					qexec.close();
					*/

					// literal
					// QUERY
					// ---------------------------------------------------------------------------------
					// ---------------------------------------------------------------------------------------
					/*
					System.out.println("Query 12 p4");
					queryBuffer = new StringBuffer();
					queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
					queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
					queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
					queryBuffer
							.append("SELECT (count(DISTINCT ?o) AS ?numRanIns) (count(?o) AS ?numTriplesWithRan) \n");

					if (graphURI != null ) {
						queryBuffer.append("FROM <");
						queryBuffer.append(graphURI.toString());
						queryBuffer.append(">\n");
					}
					queryBuffer.append("# Query 12 p4\n");
					queryBuffer.append("WHERE{\n");
					queryBuffer.append("   ?i <" + propURI + "> ?o. \n");
					queryBuffer.append(" FILTER(isLiteral(?o)) \n");
					queryBuffer.append("}");
					queryString = queryBuffer.toString();
					System.out.println(queryString);
					query = QueryFactory.create(queryString);

					qexec = null;
					results = null;
					try {
						// long start = System.currentTimeMillis();
						qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
						endpointAccessCount++;
						interval();
						results = qexec.execSelect();
						// long end = System.currentTimeMillis();
						// System.out.println("EXEC TIME: " + (end - start));
						System.out.print("l");
					} catch (Exception ex) {
						System.out.println("The error caused by this query:\n" + queryString);
						ex.printStackTrace();
						throw ex;
					}
					if (results.hasNext()) {
						QuerySolution sol = results.next();
//						Literal lit = sol.getLiteral("numRanIns");
//						if (lit != null) {
//							numRanInsWithClass += lit.getInt();
//						}
						Literal lit = sol.getLiteral("numTriplesWithRan");
						if (lit != null) {
							numTriplesWithRanClass += lit.getInt();
						}

					}
					qexec.close();
					*/

					// // literal
					// queryBuffer = new StringBuffer();
					// queryBuffer
					// .append("PREFIX owl: <http://www.w3.org/2002/07/owl#>\n");
					// queryBuffer
					// .append("PREFIX rdfs:
					// <http://www.w3.org/2000/01/rdf-schema#>\n");
					// queryBuffer
					// .append("PREFIX rdf:
					// <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
					// queryBuffer
					// .append("SELECT (count(DISTINCT ?o) AS ?numRanIns)\n");
					// if (graphURIs != null && graphURIs.length != 0) {
					// for (String graphURI : graphURIs) {
					// queryBuffer.append("FROM <");
					// queryBuffer.append(graphURI);
					// queryBuffer.append(">\n");
					// }
					// }
					// queryBuffer.append("WHERE{\n");
					// queryBuffer.append(" ?i <" + propURI + "> ?o. \n");
					// queryBuffer.append("FILTER( datatype(?o) = ?c) \n");
					// queryBuffer.append("}");
					// queryString = queryBuffer.toString();
					// // System.out.println(queryString);
					// query = QueryFactory.create(queryString);
					//
					// qexec = null;
					// results = null;
					// try {
					// // long start = System.currentTimeMillis();
					// qexec = QueryExecutionFactory.sparqlService(endpointURI,
					// query);
					// results = qexec.execSelect();
					// // long end = System.currentTimeMillis();
					// // System.out.println("EXEC TIME: " + (end - start));
					// System.out.print(".");
					// } catch (Exception ex) {
					// System.out.println(queryString);
					// ex.printStackTrace();
					// throw ex;
					// }
					// for (; results.hasNext();) {
					// QuerySolution sol = results.next();
					// Literal lit = sol.getLiteral("numRanIns");
					// if (lit != null) {
					// numRanInsWithClass += lit.getInt();
					// }
					// }
					// qexec.close();

					// domain,
					// rang
					// QUERY
					// ---------------------------------------------------------------------------------
					System.out.println("Query 12 p5");
					queryBuffer = new StringBuffer();
					queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
					queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
					queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
					queryBuffer.append("SELECT (count(?i) AS ?numTriples)\n");
					if (graphURI != null ) {
						queryBuffer.append("FROM <");
						queryBuffer.append(graphURI.toString());
						queryBuffer.append(">\n");
					}
					queryBuffer.append("# Query 12 p5\n");
					queryBuffer.append("WHERE{\n");
					queryBuffer.append("   ?i <" + propURI + "> ?o. \n");
					// if (domClassSet.size() == 0) {
					queryBuffer.append("   ?i rdf:type ?d. \n");
					// }
					// if (ranClassSet.size() == 0) {
					queryBuffer.append("   ?o rdf:type ?r. \n");
					// queryBuffer.append("FILTER( isLiteral(?o) ) \n");
					// }

					queryBuffer.append("}");
					queryString = queryBuffer.toString();
					System.out.println(queryString);
					query = QueryFactory.create(queryString);

					qexec = null;
					results = null;
					try {
						// long start = System.currentTimeMillis();
						qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
						endpointAccessCount++;
						interval();
						results = qexec.execSelect();
						// long end = System.currentTimeMillis();
						// System.out.println("EXEC TIME: " + (end - start));
						System.out.print("m");
					} catch (Exception ex) {
						System.out.println("The error caused by this query:\n" + queryString);
						ex.printStackTrace();
						throw ex;
					}
					for (; results.hasNext();) {
						QuerySolution sol = results.next();
						Literal lit = sol.getLiteral("numTriples");
						if (lit != null) {
							numTriplesWithBothClass = lit.getInt();
						}
					}
					qexec.close();

					// QUERY
					// ---------------------------------------------------------------------------------
					System.out.println("Query 12 p6");
					queryBuffer = new StringBuffer();
					queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
					queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
					queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
					queryBuffer.append("SELECT (count(?i) AS ?numTriples) \n");
					if (graphURI != null ) {
						queryBuffer.append("FROM <");
						queryBuffer.append(graphURI.toString());
						queryBuffer.append(">\n");
					}
					queryBuffer.append("# Query 12 p6\n");
					queryBuffer.append("WHERE{\n");
					queryBuffer.append("   ?i <" + propURI + "> ?o. \n");
					// if (domClassSet.size() == 0) {
					queryBuffer.append("   ?i rdf:type ?d. \n");
					// }
					// if (ranClassSet.size() == 0) {
					// queryBuffer.append(" ?o rdf:type ?r. \n");
					queryBuffer.append("   FILTER( isLiteral(?o) ) \n");
					// }

					queryBuffer.append("}");
					queryString = queryBuffer.toString();
					System.out.println(queryString);
					query = QueryFactory.create(queryString);

					qexec = null;
					results = null;
					try {
						// long start = System.currentTimeMillis();
						qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
						endpointAccessCount++;
						interval();
						results = qexec.execSelect();
						// long end = System.currentTimeMillis();
						// System.out.println("EXEC TIME: " + (end - start));
						System.out.print("d");
					} catch (Exception ex) {
						System.out.println("The error caused by this query:\n" + queryString);
						ex.printStackTrace();
						throw ex;
					}
					for (; results.hasNext();) {
						QuerySolution sol = results.next();
						Literal lit = sol.getLiteral("numTriples");
						if (lit != null) {
							numTriplesWithBothClass += lit.getInt();
						}
					}
					qexec.close();

					// } // end of IF

					// int numTriplesWithClass =
					// (numTriplesWithDomClass+numTriplesWithRanClass) -
					// numTriplesWithBothClass;

					// System.out.println("\nnumTriplesWithClass:
					// "+numTriplesWithClass);
					System.out.print("\nnumTriplesWithDomClass: " + numTriplesWithDomClass);
					System.out.print("   numTriplesWithRanClass: " + numTriplesWithRanClass);
					System.out.println("   numTriplesWithBothClass: " + numTriplesWithBothClass);
					System.out.println("   numTriples: " + numTriples);
				}

				// label
				ArrayList<String> labelList = new ArrayList<String>();
				// QUERY
				// ---------------------------------------------------------------------------------
				System.out.println("Query 12 p7");
				queryBuffer = new StringBuffer();
				queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
				queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
				queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
				queryBuffer.append("SELECT ?label\n");
				if (graphURI != null ) {
					queryBuffer.append("FROM <");
					queryBuffer.append(graphURI.toString());
					queryBuffer.append(">\n");
				}
				queryBuffer.append("# Query 12 p7\n");
				queryBuffer.append("WHERE{\n");
				queryBuffer.append("  <" + propURI + "> rdfs:label ?label. \n");
				queryBuffer.append("}");
				queryString = queryBuffer.toString();
				System.out.println(queryString);
				query = QueryFactory.create(queryString);

				qexec = null;
				results = null;
				try {
					// long start = System.currentTimeMillis();
					qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
					endpointAccessCount++;
					interval();
					results = qexec.execSelect();
					// long end = System.currentTimeMillis();
					// System.out.println("EXEC TIME: " + (end - start));
					// System.out.print(".");
					System.out.print("L");
				} catch (Exception ex) {
					System.out.println("The error caused by this query:\n" + queryString);
					ex.printStackTrace();
					throw ex;
				}
				for (; results.hasNext();) {
					QuerySolution sol = results.next();
					Literal lit = sol.getLiteral("label");
					if (lit != null) {
						String label = lit.getString();
						labelList.add(label);
						// pro.addLiteral(labelPro, label);
					}
				}
				qexec.close();

				int propCategory = 0;
				if (metadataLayer == 3) {
					// domain
					if (numTriples == 0) {
						propCategory = 4;
					} else {
						// category 1
						if (domClassSet.size() != 0 && (ranClassSet.size() != 0 || literalFlag)) {
							propCategory = 1;
						} else {
							// category 2
							if (numTriples == numTriplesWithBothClass) {
								propCategory = 2;
							} else {
								if (numTriplesWithBothClass != 0) {
									propCategory = 3;
								} else {
									propCategory = 4;
								}
							}
						}
					}

					switch (wholePropertyCategory) {
					case 0:
						wholePropertyCategory = propCategory;
						break;
					case 1:
						if (propCategory == 4) {
							wholePropertyCategory = 3;
						} else {
							wholePropertyCategory = propCategory;
						}
						break;
					case 2:
						if (propCategory > 2) {
							wholePropertyCategory = 3;
						}
						break;
					case 3:
						break;
					case 4:
						if (propCategory < 4) {
							wholePropertyCategory = 3;
						}
						break;
					}

					countProperties[propCategory - 1]++;
					countAllTriples[propCategory - 1] += numTriples;
					countClassTriples[propCategory - 1] += numTriplesWithBothClass;
				}

				// write to model
				Resource propPartRes = model.createResource(AnonId.create());

				datasetRes.addProperty(propertyPartitionPro, propPartRes);
				// property
				Property pro = model.createProperty(propURI.toString());
				// labels
				for (String label : labelList) {
					pro.addLiteral(labelPro, label);
				}
				propPartRes.addProperty(propertyPro, pro);
				if (metadataLayer == 3) {
					propPartRes.addLiteral(triplesPro, numTriples);
				}

				System.out.println("\n PDRD classRelations(L2033): " + propDomRanDeclList2.size());

				// class relations
				for (PropertyDomainRangeDecl pdrd : propDomRanDeclList2) {
					Resource classRelation = model.createResource(AnonId.create());
					classRelation.addProperty(typePro, classRelationCls);
					propPartRes.addProperty(classRelationPro, classRelation);

					//
					classRelation.addProperty(subjectClsPro, model.createResource(pdrd.getDomainClass()));

					classURISet.add(new URI(pdrd.getDomainClass()));

					//
					if (pdrd.getRangeClass() != null) {
						classRelation.addProperty(objectClsPro, model.createResource(pdrd.getRangeClass()));
						classURISet.add(new URI(pdrd.getRangeClass()));
					} else {
						if (pdrd.getLiteralDataType() != null) {
							classRelation.addProperty(objectClsPro, literalCls);
						}
					}

					// classRelation.addProperty(propPro,
					// model.createProperty(pdrd.getProperty()));

					if (metadataLayer == 3) {
						//
						classRelation.addLiteral(triplesPro, pdrd.getNumTriples());
						//
						classRelation.addLiteral(distinctSubjectsPro, pdrd.getNumDomainInstances());
						//
						classRelation.addLiteral(distinctObjectsPro, pdrd.getNumRangeInstances());
					}

					// TODO
					//
					// classRelation.addLiteral(isDomClsLimPro,
					// pdrd.isDomainClassLimitedQ());
					// classRelation.addLiteral(isRanClsLimPro,
					// pdrd.isRangeClassLimitedQ());
				}

				// Resource propProfile = model.createResource(AnonId.create());
				// propProfile.addProperty(typePro, propProfileCls);

				propPartRes.addProperty(propPro, pro);
				if (metadataLayer == 3) {
					propPartRes.addLiteral(propCatPro, propCategory);
				}
				pro.addProperty(typePro, propRes);

				for (String domURI : domClassSet) {
					Resource dom = model.createResource(domURI);
					dom.addProperty(typePro, clsCls);
					pro.addProperty(domainPro, dom);
					classURISet.add(new URI(domURI));
				}

				for (String ranURI : ranClassSet) {
					Resource ran = model.createResource(ranURI);
					ran.addProperty(typePro, clsCls);
					pro.addProperty(rangePro, ran);
					classURISet.add(new URI(ranURI));
				}

				// TODO literal
				// 
				for (String datatypeURI : literalDatatypeSet) {
					Resource ran = model.createResource(datatypeURI);
					ran.addProperty(typePro, clsCls);
					pro.addProperty(rangePro, ran);
				}

				propPartRes.addLiteral(classesPro, classURISet.size());
				// propPartRes.addLiteral(datatypesPro, datatypeSet.size());

				propPartRes.addLiteral(subjectClassesPro, domClassSet.size());
				propPartRes.addLiteral(objectClassesPro, ranClassSet.size());
				propPartRes.addLiteral(objectDatatypesPro, literalDatatypeSet.size());

				if (metadataLayer == 3) {
					propPartRes.addLiteral(distinctSubjectsPro, numDomIns);
					propPartRes.addLiteral(distinctObjectsPro, numRanIns);
					propPartRes.addLiteral(triplesPro, numTriples);

					totalNumTriples += numTriples;

					System.out.println("\n" + propURI + " (" + propCategory + ")(" + numTriplesWithBothClass + " / "
							+ numTriples + ", " + numDomIns + ", " + numRanIns + ")");
				}

			} catch (Exception ex) {
				System.out.println("\nERROR: propURI " + propURI);
				ex.printStackTrace();
			}
		} // end of for (propURI)

		Property subsetProp = model.createProperty(URICollection.PROPERTY_SB_PROPERTY_CATEGORY_SUBSET);
		Property propertyCategoryPro = model.createProperty(URICollection.PROPERTY_SB_PROPERTY_CATEGORY);
		Property propertiesPro = model.createProperty(URICollection.PROPERTY_VOID_PROPERTIES);

		int proLen = 0;
		if (countProperties != null) {
			int cnt = 1;
			for (int c : countProperties) {
				Resource sbRes = model.createResource(AnonId.create());
				datasetRes.addProperty(subsetProp, sbRes);
				sbRes.addLiteral(triplesPro, countAllTriples[cnt - 1]);
				sbRes.addLiteral(searchableTriplesPro, countClassTriples[cnt - 1]);
				sbRes.addLiteral(propertyCategoryPro, cnt);
				sbRes.addLiteral(propertiesPro, c);

				System.out.println("property[" + cnt + "]: " + c + ": " + countClassTriples[cnt - 1] + " / "
						+ countAllTriples[cnt - 1]);
				proLen += c;
				cnt++;
			}
		}

		System.out.println("#Classes: " + classURISet.size());
		System.out.println("#Triples: " + totalNumTriples);

		datasetRes.addLiteral(triplesPro, totalNumTriples);
		datasetRes.addLiteral(classesPro, classURISet.size());
		datasetRes.addLiteral(propertiesPro, proLen);

		schema.propertyCategory = wholePropertyCategory;
		schema.countProperties = countProperties;
		schema.classCategory = 0;
		schema.graphCategory = 0;
		schema.numTriples = totalNumTriples;
		schema.inferedClasses = classURISet;
		return schema;
	}

	class PropertyDomainRangeDecl {

		URI property = null;
		String domainClass = null;
		String rangeClass = null;
		String literalDataType = null;
		int numTriples = 0;
		int numDomainInstances = 0;
		int numRangeInstances = 0;
		boolean domainClassLimitedQ = false;
		boolean rangeClassLimitedQ = false;

		public PropertyDomainRangeDecl(URI property, String domainClass, String rangeClass, String literalDataType,
				int numTriples, int numDomainInstances, int numRangeInstances, boolean domainClassLimitedQ,
				boolean rangeClassLimitedQ) {
			this.property = property;
			this.domainClass = domainClass;
			this.rangeClass = rangeClass;
			this.literalDataType = literalDataType;
			this.numTriples = numTriples;
			this.numDomainInstances = numDomainInstances;
			this.numRangeInstances = numRangeInstances;
			this.domainClassLimitedQ = domainClassLimitedQ;
			this.rangeClassLimitedQ = rangeClassLimitedQ;
		}

		public boolean equals(Object obj) {
			if (!(obj instanceof PropertyDomainRangeDecl)) {
				return false;
			}
			PropertyDomainRangeDecl pdrd = (PropertyDomainRangeDecl) obj;
			if (property == null) {
				if (pdrd.getProperty() != null) {
					return false;
				}
			} else {
				if (!property.equals(pdrd.getProperty())) {
					return false;
				}
			}
			if (domainClass == null) {
				if (pdrd.getDomainClass() != null) {
					return false;
				}
			} else {
				if (!domainClass.equals(pdrd.getDomainClass())) {
					return false;
				}
			}
			if (rangeClass == null) {
				if (pdrd.getRangeClass() != null) {
					return false;
				}
			} else {
				if (!rangeClass.equals(pdrd.getRangeClass())) {
					return false;
				}
			}
			if (literalDataType == null) {
				if (pdrd.getLiteralDataType() != null) {
					return false;
				}
			} else {
				if (!literalDataType.equals(pdrd.getLiteralDataType())) {
					return false;
				}
			}

			return true;
		}

		public int hashCode() {
			return super.hashCode();
		}

		public URI getProperty() {
			return property;
		}

		public String getDomainClass() {
			return domainClass;
		}

		public String getRangeClass() {
			return rangeClass;
		}

		public String getLiteralDataType() {
			return literalDataType;
		}

		public final int getNumTriples() {
			return numTriples;
		}

		public final int getNumDomainInstances() {
			return numDomainInstances;
		}

		public final int getNumRangeInstances() {
			return numRangeInstances;
		}

		public final boolean isDomainClassLimitedQ() {
			return domainClassLimitedQ;
		}

		public final boolean isRangeClassLimitedQ() {
			return rangeClassLimitedQ;
		}

	}

	public CrawlerImpl(URI endpointURI) {
		this.endpointURI = endpointURI;
	}

	public CrawlerImpl(URI endpointURI, String crawlName, String outDirName) throws Exception {
		this.endpointURI = endpointURI;
		this.crawlName = crawlName;
		this.outDir = new File(outDirName);
		if (!this.outDir.exists()) {
			boolean b = this.outDir.mkdirs();
			if (!b) {
				throw new Exception("New output directory cannot be created: " + this.outDir.getCanonicalPath());
			}
		} else {
			if (this.outDir.isFile()) {
				throw new Exception("Output File exists and new output directory cannot be created: "
						+ this.outDir.getCanonicalPath());
			}
		}
	}

//	public CrawlerImpl(URI endpointURI, URI[] graphURIs) {
//		this.endpointURI = endpointURI;
//		this.graphURIs = graphURIs;
//	}

	
/*
 	public Model getPropertiesFromInstanceDecls() throws Exception {

		// QUERY
		// ---------------------------------------------------------------------------------
		// obtains all triples [?p,?d,?r] where classes of subject and object in
		// triple <?i,?p,?o>
		// are declared by <?i,rdf:type,?d> and <?j,rdf:type,?r>.
		// ---------------------------------------------------------------------------------------
		StringBuffer queryBuffer = new StringBuffer();
		queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
		queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
		queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
		queryBuffer.append("SELECT DISTINCT ?p ?d ?r \n");
		if (graphURI != null ) {&& graphURIs.length != 0) {
			for (URI graphURI : graphURIs) {
				queryBuffer.append("FROM <");
				queryBuffer.append(graphURI.toString());
				queryBuffer.append(">\n");
			}
		}
		queryBuffer.append("WHERE{\n");
		queryBuffer.append("  ?i rdf:type ?d.\n");
		queryBuffer.append("  ?j rdf:type ?r.\n");
		queryBuffer.append("  ?i ?p ?j.\n");
		// queryBuffer.append(" ?p rdfs:domain ?m.\n");
		// queryBuffer.append(" ?p rdfs:range ?r.\n");
		queryBuffer.append("}");

		String queryString = queryBuffer.toString();

		// System.out.println(queryString);

		Query query = QueryFactory.create(queryString);

		QueryExecution qexec = null;
		ResultSet results = null;
		try {
			// long start = System.currentTimeMillis();
			qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
			endpointAccessCount++;
			interval();
			results = qexec.execSelect();
			// long end = System.currentTimeMillis();
			// System.out.println("EXEC TIME: " + (end - start));
			System.out.print(".");
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}

		// create model
		Model model = ModelFactory.createDefaultModel();
		Property domainPro = model.createProperty(URICollection.PROPERTY_RDFS_DOMAIN);
		Property rangePro = model.createProperty(URICollection.PROPERTY_RDFS_RANGE);

		// FILTER LITERAL+alpha
		String[] filterStrs = URICollection.FILTER_PROPERTY;
		for (; results.hasNext();) {
			QuerySolution sol = results.next();
			Resource pro = sol.getResource("p");
			Resource dom = sol.getResource("d");
			Resource ren = sol.getResource("r");
			if (pro != null && uriFilter(pro.getURI(), filterStrs) != null) {
				pro = model.createResource(pro);
				if (dom != null) {
					dom = model.createResource(dom);
					pro.addProperty(domainPro, dom);
				}
				if (ren != null) {
					ren = model.createResource(ren);
					pro.addProperty(rangePro, ren);
				}
			}
		}
		qexec.close();
		return model;
	}


		
		
		public Model getProperiesFromDomainRangeDecls() throws Exception {

		// QUERY
		// ---------------------------------------------------------------------------------
		// obtains all pairs of domain and range for each property.
		// ---------------------------------------------------------------------------------------
		StringBuffer queryBuffer = new StringBuffer();
		queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
		queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
		queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
		queryBuffer.append("SELECT ?p ?d ?r \n");
		if (graphURIs != null && graphURIs.length != 0) {
			for (URI graphURI : graphURIs) {
				queryBuffer.append("FROM <");
				queryBuffer.append(graphURI.toString());
				queryBuffer.append(">\n");
			}
		}
		queryBuffer.append("WHERE{\n");
		queryBuffer.append("  ?p rdfs:domain ?d.\n");
		queryBuffer.append("  ?p rdfs:range ?r.\n");
		queryBuffer.append("}");

		String queryString = queryBuffer.toString();

		// System.out.println(queryString);

		Query query = QueryFactory.create(queryString);

		QueryExecution qexec = null;
		ResultSet results = null;
		try {
			// long start = System.currentTimeMillis();
			qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
			endpointAccessCount++;
			interval();
			results = qexec.execSelect();
			// long end = System.currentTimeMillis();
			// System.out.println("EXEC TIME: " + (end - start));
			System.out.print("x");
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}

		// create model
		Model model = ModelFactory.createDefaultModel();
		Property domainPro = model.createProperty(URICollection.PROPERTY_RDFS_DOMAIN);
		Property rangePro = model.createProperty(URICollection.PROPERTY_RDFS_RANGE);

		// FILTER LITERAL+alpha
		String[] filterStrs = URICollection.FILTER_PROPERTY;
		for (; results.hasNext();) {
			QuerySolution sol = results.next();
			Resource pro = sol.getResource("p");
			Resource dom = sol.getResource("d");
			Resource ren = sol.getResource("r");
			if (pro != null && uriFilter(pro.getURI(), filterStrs) != null) {
				pro = model.createResource(pro);
				if (dom != null) {
					dom = model.createResource(dom);
					pro.addProperty(domainPro, dom);
				}
				if (ren != null) {
					ren = model.createResource(ren);
					pro.addProperty(rangePro, ren);
				}
			}
		}
		qexec.close();
		return model;
	}
*/
		
		
	// heavy
	public URI[] getDeclaredRDFsClasses(URI graphURI, boolean filterFlag) throws Exception {

		String[] filterStrs = URICollection.FILTER_CLASS;
		String[] unfilterStrs = null;
		// String[] unfilterStrs = URICollection.UNFILTER_CLASS;
		// where
		String[] lines = new String[] { "", "?c rdf:type rdfs:Class.", "[] rdf:type ?c.", "[] rdfs:domain ?c.",
				"[] rdfs:range ?c.", "?c rdfs:subclassOf [].", "[] rdfs:subclassOf ?c." };

		// QUERY 2 (Metadata Layer 2, 3);
		// ---------------------------------------------------------------------------------
		// obtains all classes by checking triples of <?c,rdf:type,rdfs:Class>,
		// <[],rdf:type,?c>,
		// <[],rdfs:domain,?c>, <[],rdfs:range,?c>, <?c,rdfs:subclassOf,[]> and
		// <[] rdfs:subClassof,?c>.
		// ---------------------------------------------------------------------------------------
		StringBuffer queryBuffer = new StringBuffer();
		for (int i = 1; i < lines.length; i++) {
			if (i != 1) {
				queryBuffer.append(" UNION ");
			}
			queryBuffer.append("{");
			queryBuffer.append(lines[i]);
			queryBuffer.append("}\n");
		}
		lines[0] = queryBuffer.toString();

		HashSet<URI> resultClassSet = new HashSet<URI>();
		for (int i = 0; i < lines.length; i++) {
			queryBuffer = new StringBuffer();
			queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
			queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
			queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
			queryBuffer.append("SELECT DISTINCT ?c  \n");
			if (graphURI != null ) {
					queryBuffer.append("FROM <");
					queryBuffer.append(graphURI.toString());
					queryBuffer.append(">\n");
			}
			queryBuffer.append("WHERE{\n");
			queryBuffer.append(lines[i]);
			queryBuffer.append("}");

			String queryString = queryBuffer.toString();

			// System.out.println(queryString);

			Query query = QueryFactory.create(queryString);

			QueryExecution qexec = null;
			ResultSet results = null;
			try {
				// long start = System.currentTimeMillis();
				qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
				endpointAccessCount++;
				interval();
				results = qexec.execSelect();
				// long end = System.currentTimeMillis();
				// System.out.println("EXEC TIME: " + (end - start));
				System.out.print(".");
				for (; results.hasNext();) {
					QuerySolution sol = results.next();
					Resource res = null;
					try {
						res = sol.getResource("c");
					} catch (Exception ex) {
						// todo
						ex.printStackTrace();
						// just try
						try {
							Literal lit = sol.getLiteral("c");
							System.out.println("Warning: found Literal: " + lit.toString());
						} catch (Exception ex2) {
							// nothing to do
						}
					}
					if (res != null && uriFilter(res.getURI(), filterStrs, unfilterStrs) != null) {
						resultClassSet.add(new URI(res.getURI()));
					}
				}
				qexec.close();
				if (i == 0) {
					break;
				}
			} catch (Exception ex) {
				qexec.close();
				if (i != 0) {
					System.out.println("The error caused by this query:\n" + queryString);
					ex.printStackTrace();
					throw ex;
				}
			}
		}
		if (filterFlag) {
			resultClassSet = removeDatatypes(graphURI, resultClassSet);
		}
		return resultClassSet.toArray(new URI[0]);
	}

	private HashSet<URI> removeDatatypes(URI graphURI, HashSet<URI> classSet) throws Exception {
		if (classSet == null) {
			return null;
		}
		StringBuffer queryBuffer = null;
		HashSet<URI> resultSet = new HashSet<URI>();
		for (URI uri : classSet) {
			// ASK
			queryBuffer = new StringBuffer();
			queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
			queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
			queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
			queryBuffer.append("ASK \n");
			if (graphURI != null ) {
					queryBuffer.append("FROM <");
					queryBuffer.append(graphURI.toString());
					queryBuffer.append(">\n");
			}
			queryBuffer.append("{\n");
			queryBuffer.append(" ?s ?p ?o.\n");
			queryBuffer.append(" Filter( isLiteral(?o) && datatype(?o) = <");
			queryBuffer.append(uri);
			queryBuffer.append("> ).\n");
			queryBuffer.append("}");

			String queryString = queryBuffer.toString();

			Query query = null;
			try {
				query = QueryFactory.create(queryString);
			} catch (Exception ex) {
				System.out.println("The error caused by this query:\n" + queryString);
				ex.printStackTrace();
				throw ex;
			}

			boolean bResult = false;
			QueryExecution qexec = null;
			try {
				// long start = System.currentTimeMillis();
				qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
				endpointAccessCount++;
				interval();
				bResult = qexec.execAsk();
				// long end = System.currentTimeMillis();
				// System.out.println("EXEC TIME: " + (end - start));
			} catch (Exception ex) {
				System.out.println("The error caused by this query:\n" + queryString);
				ex.printStackTrace();
			}
			qexec.close();
			if (!bResult) {
				resultSet.add(uri);
			}
			System.out.print(".");
		}
		System.out.print("\n");

		return resultSet;
	}


	public URI[] getRDFProperties(URI graphURI) throws Exception {
//		String stepName = "getRDFProperties";

		// QUERY 7 (Metadata Layer 2, 3)
		// ---------------------------------------------------------------------------------
		// obtains all properties written in the given graphs by simply checking
		// triples ?s ?p ?o.
		// ---------------------------------------------------------------------------------------
		StringBuffer queryBuffer = new StringBuffer();
		queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
		queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
		queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");

		queryBuffer.append("SELECT DISTINCT ?p \n");
		if (graphURI != null ) {
				queryBuffer.append("FROM <");
				queryBuffer.append(graphURI.toString());
				queryBuffer.append(">\n");
		}
		queryBuffer.append("WHERE{\n");
		queryBuffer.append("{  ?s ?p ?o.}\n");
		// queryBuffer.append("UNION\n");
		// queryBuffer.append("{ ?p rdfs:domain ?d.}\n");
		// queryBuffer.append("UNION\n");
		// queryBuffer.append("{ ?p rdfs:range ?r.}\n");
		queryBuffer.append("}");
		String queryString = queryBuffer.toString();

		// System.out.println(queryString);

		Query query = QueryFactory.create(queryString);

		QueryExecution qexec = null;
		ResultSet results = null;

		int sCount = 3;
		while (sCount > 0) {
			try {
				// long start = System.currentTimeMillis();
				qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
				// qexec.setTimeout(240000L);
				endpointAccessCount++;
				results = qexec.execSelect();
				// long end = System.currentTimeMillis();
				// System.out.println("EXEC TIME: " + (end - start));
				System.out.print("P");
				break;
			} catch (Exception ex) {
				sCount--;
				if (sCount == 0) {
					ex.printStackTrace();
					// recoveredStrings =
					// logFileManagerForRecovery.recoverStrings(stepName, null);
					// if (recoveredStrings == null) {
					// todoFileManager.writeQuery(stepName, null, queryString);
					throw ex;
					// }
				} else {
					interval_error();
				}
			}
		}

		ArrayList<URI> resultList = new ArrayList<URI>();
		String[] filterStrs = URICollection.FILTER_PROPERTY;
		ArrayList<String> forLog = new ArrayList<String>();
		for (; results.hasNext();) {
			QuerySolution sol = results.next();
			Resource res = sol.getResource("p");
			if (res != null) {
				forLog.add(res.getURI());
				if (uriFilter(res.getURI(), filterStrs) != null) {
					resultList.add(new URI(res.getURI()));
				}
			}
		}
		qexec.close();
		URI[] resultStringArray = resultList.toArray(new URI[0]);
		return resultStringArray;
	}

	public String[] getDatatypes(URI graphURI, URI[] propertyURIs) throws Exception {
		HashSet<String> datatypes = new HashSet<String>();

		boolean errorFlag = false;
		if (propertyURIs != null) {
//			String stepName = "getDatatypes";
			//String properties = String.join("", propertyURIs)
			ArrayList<String> predicates = new ArrayList<String>();
			Arrays.asList(propertyURIs).forEach(s -> predicates.add(s.toString()));
//			System.out.println("<" + String.join("> <", predicates) + ">");

//			for (URI propertyURI : propertyURIs) {
//				String target = propertyURI.toString();
				boolean targetErrorFlag = false;

				// QUERY 19 (Metadata Layer 2,3)
				// ---------------------------------------------------------------------------------
				// obtains all datatypes associated with the given properties
				// Note: virtuoso has bugs and cannot handle datatypes
				// correctly.
				// ---------------------------------------------------------------------------------------
				StringBuffer queryBuffer = new StringBuffer();
				queryBuffer.append("PREFIX owl: <" + URICollection.PREFIX_OWL + ">\n");
				queryBuffer.append("PREFIX rdfs: <" + URICollection.PREFIX_RDFS + ">\n");
				queryBuffer.append("PREFIX rdf: <" + URICollection.PREFIX_RDF + ">\n");
				queryBuffer.append("SELECT DISTINCT (datatype(?o) AS ?ldt) \n");
				if (graphURI != null ) {
						queryBuffer.append("FROM <");
						queryBuffer.append(graphURI.toString());
						queryBuffer.append(">\n");
				}
				// if (graphURIs != null && graphURIs.length != 0) {
				// for (String graphURI : graphURIs) {
				// queryBuffer.append("FROM <");
				// queryBuffer.append(graphURI);
				// queryBuffer.append(">\n");
				// }
				// }
				queryBuffer.append("WHERE{\n");
//				queryBuffer.append(" [] <");
//				queryBuffer.append(propertyURI);
//				queryBuffer.append("> ?o.\n  FILTER(isLiteral(?o))\n");
				queryBuffer.append(" [] ?p ?o.\n  FILTER(isLiteral(?o))\n");
				queryBuffer.append(" VALUES ?p {<" + String.join("> <", predicates) + ">}\n");
				queryBuffer.append("}");
				String queryString = queryBuffer.toString();

				// System.out.println(queryString);

				Query query = QueryFactory.create(queryString);

				QueryExecution qexec = null;
				QueryEngineHTTP alt_qexec = null;
				ResultSet results = null;
//				String[] recoveredStringArray = null;

				int sCount = 3;
				while (sCount > 0) {
					try {
						// long start = System.currentTimeMillis();
						alt_qexec = new QueryEngineHTTP(endpointURI.toString(), query);
						alt_qexec.setTimeout(-1);

						qexec = QueryExecutionFactory.sparqlService(endpointURI.toString(), query);
						endpointAccessCount++;
						qexec.setTimeout(-1);
						interval();
						alt_qexec.setAcceptHeader("text/csv");
						System.out.println("Accept:" + alt_qexec.getAcceptHeader());
						results = alt_qexec.execSelect();
						//results = qexec.execSelect();
						break;
					} catch (Exception ex) {
						sCount--;
						if (sCount == 0) {
							ex.printStackTrace();
							System.out.println("The error caused by this query:\n" + queryString);
						} else {
							interval_error();
						}
					}
				}

				if (!targetErrorFlag) {
					/*
					if (recoveredStringArray != null) {
						String[] filterStrs = URICollection.FILTER_CLASS;
						String[] unfilterStrs = URICollection.UNFILTER_CLASS;
						for (String litURI : recoveredStringArray) {
							if (uriFilter(litURI, filterStrs, unfilterStrs) != null) {
								datatypes.add(litURI);
							}
						}
					*/
//					} else {
						String[] filterStrs = URICollection.FILTER_CLASS;
						String[] unfilterStrs = URICollection.UNFILTER_CLASS;
						ArrayList<String> targetDataTypes = new ArrayList<String>();
						for (; results.hasNext();) {
							RDFNode lit = results.next().get("ldt");
							String litURI = lit.toString();
							if (litURI.length() > 0) {
								targetDataTypes.add(litURI);
								if (uriFilter(litURI, filterStrs, unfilterStrs) != null) {
									System.out.println("litURI: " + litURI);
									datatypes.add(litURI);
								}
							}
						}
//					}
//				}
				alt_qexec.close();
//				qexec.close();
			}
		}
		if (!errorFlag) {
			String[] resultStringArray = datatypes.toArray(new String[0]);
			return resultStringArray;
		}
		throw new Exception();
	}

	public String uriFilter(String uriStr, String[] filterStrs) throws Exception {
		if (filterStrs == null || filterStrs.length == 0) {
			return uriStr;
		}
		for (String str : filterStrs) {
			if (uriStr != null) {
				if (uriStr.startsWith(str)) {
					return null;
				}
			}

		}
		return uriStr;
	}

	public String uriFilter(String uriStr, String[] filterStrs, String[] unFilterStrs) throws Exception {
		if (unFilterStrs == null || unFilterStrs.length == 0) {
			// nothing to do
		} else {
			for (String str : unFilterStrs) {
				if (uriStr != null) {
					if (uriStr.startsWith(str)) {
						return uriStr;
					}
				}
			}
		}

		if (filterStrs == null || filterStrs.length == 0) {
			return uriStr;
		}
		for (String str : filterStrs) {
			if (uriStr != null) {
				if (uriStr.startsWith(str)) {
					return null;
				}
			}

		}
		return uriStr;
	}

}