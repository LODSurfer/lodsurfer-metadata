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

# development version

Development version is also available (the branch name is develop).  Please note that options are different from the stable version (main branch).

#Usage of the LODSurfer metadata crawler (development version)

% java org.sparqlbuilder.lodsurfer.metadatacrawler.CrawlerImpl \[options\]

\[options\] <br>
  1. to print a list of graphURIs <br>
     -g endpointURL <br>
  2. to crawl whole data in the endpoint and obtain level [2/3] metadata of the endpoint<br>
     -e2 endpointURL crawlName outputFileName <br>
     -e3 endpointURL crawlName outputFileName <br>
  3. to crawl whole data in the endpoint and obtain level [2/3] metadata for each graph of the endpoint<br>
     -a2 endpointURL crawlName outputFileName <br>
     -a3 endpointURL crawlName outputFileName <br>
  4. to crawl a user-specified graph in the endpoint and obtain level [2/3] metadata of user-specified graph<br>
     -g2 endpointURL crawlName graphURI outputFileName <br>
     -g3 endpointURL crawlName graphURI outputFileName <br>
  4. to crawl the default graph in the endpoint and obtain level [2/3] metadata of the graph<br>
     -d2 endpointURL crawlName outputFileName <br>
     -d3 endpointURL crawlName outputFileName <br>
     
The data structure of LODSurfer metadata is shown [here](https://github.com/LODSurfer/lodsurfer-metadata/wiki/LOD-Surfer-Metadata-Structure).


