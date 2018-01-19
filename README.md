# lodsurfer-metadata

Metadata definition and implementaton for LOD Surfer, a data search tool for life-sciences databases

#Usage of the LODSurfer metadata crawler

% java org.sparqlbuilder.lodsurfer.metadatacrawler.CrawlerImpl \[options\]

\[options\] <br>
  1. to print a list of graphURIs <br>
     -g endpointURL <br>
  2. to crawl whole data in the endpoint <br>
     -ac endpointURL crawlName outputFileName <br>
  3. to crawl the specified graph in the endpoint <br>
     -gc endpointURL crawlName graphURI outputFileName <br>  
	4. to crawl the default named graph in the endpoint <br>
     -d endpointURL crawlName outputFileName <br>  
