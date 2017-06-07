import crawler.Crawler
import crawler.InfoLogger
import crawler.Site
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import groovy.transform.Synchronized
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element
import org.jsoup.select.Elements

import java.security.MessageDigest

/**
 * Created by Artur on 01.06.2017.
 */
class Kwarler {

    static def timestamp = "1496678593606"

    static Random rnd = new Random()

    public static void main(args){
       /* 6.times { num ->
            def numOfThreads = Math.pow(2,num)
            Crawler crawler = new Crawler("https://www.upce.cz/")
            crawler.download(numOfThreads)
        }
        return*/
        //def siteList = loadSitesFromFile();
        //def adjacencyMatrix = loadAdjacencyMatrix(siteList)
        20.times { h ->
            def aMatrix = loadMatrixFromFile()
            //println "Liczba wierszy:\t${aMatrix.size()}"
            def liczbaLukow = 0
            def inEdges = [:]
            List<Node> nodes = []
            aMatrix.eachWithIndex { it, index ->
                if (it) {
                    liczbaLukow += it.size()
                    it.each { b ->
                        if (b < 5205)
                            inEdges[b] = inEdges[b] ? inEdges[b] + index : [index]
                    }
                    nodes << new Node(id: index, outs: it)
                }
            }
            inEdges.eachWithIndex { e, i ->
                if (nodes[i])
                    nodes[i].ins = e.value
            }
            //pageRank(nodes)
            println wreckPageRnd(nodes)
            //println "Liczba lukow:\t${liczbaLukow}"
        }

        return
        /*
        def max = -1;
        def odp = []
        def odleglosci = []
        10.times {
            odleglosci << 0
        }
        for (int i = 0; i < aMatrix.length; i++) {

            def start = System.currentTimeMillis()
            def path = BFS(i,aMatrix)
            path.each{
                odleglosci[it[1].size()]++
                if(max < it[1].size()){
                    max = it[1].size()
                    odp = it
                }
            }
            def stop = System.currentTimeMillis()
            if(i%500 == 0)
                println " "+i+" "+  (stop-start) + " cur " + max +" " + odp
        }
        println "${odp} ${max}"
        odleglosci.eachWithIndex { e,i ->
            println "${i}\t${e}"
        }

        //path[1].each{
        //    println siteList[it]
        //}
        //getGraphDiameter(aMatrix)
        */
    }

    public static void main2(args){
        def aMatrix = loadMatrixFromFile("list")
        def start = System.currentTimeMillis()
        ShortestPath t = new ShortestPath();
        t.dijkstra(aMatrix, 0);
        def stop = System.currentTimeMillis()
        println stop-start
    }

    static void getGraphDiameter(int[][] graph) {
        def size = graph.length
        int max = -1
        int odp = []
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                def isEdge = graph[i][j]
                def nextStep = j
                def curr = []
                while(isEdge){
                    curr += j
                }
            }
        }
    }

    static def loadMatrixFromFile(String m = "list") {
        String matrixFile = "C:/tmp/${timestamp}-${m}.log"
        FileInputStream fis = new FileInputStream(matrixFile);
        ObjectInputStream iis = new ObjectInputStream(fis);
        return iis.readObject();
    }

    static def loadSitesFromFile() {
        def fileName = "C:/tmp/${timestamp}-enumeratedSites.log"
        def file = new File(fileName)
        def sites = []
        file.readLines().each {
            sites << it.tokenize()[1]
        }
        sites
    }

    static def loadAdjacencyMatrix(List siteList) {
        def matrix = new List[siteList.size()]
        def fileName = "C:/tmp/${timestamp}-sites.log"
        def file = new File(fileName)
        def jsonSlurper = new JsonSlurper()
        file.readLines().eachWithIndex { it, i ->
            def site = jsonSlurper.parseText(it)
            int index = siteList.findIndexOf{ it == site['absolutURL']}
            matrix[index] = []
            site['sites'].each{ neigh ->
                def n = siteList.findIndexOf{ it == neigh}
                if(n != -1) {
                    matrix[index] << n
                    println " " + index + " " + n
                }
            }
        }

        printMatrix matrix
    }

    static def printMatrix(def matrix){
        String matrixFile = "C:/tmp/${timestamp}-paths.log"
        FileOutputStream fos = new FileOutputStream(matrixFile);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(matrix);
    }

    static def BFS(def s,def matrix)
    {
        def visited = new boolean[matrix.length]
        def queue = [] as Queue

        visited[s] = true
        queue.add([s,[s]])
        def results = []
        while (queue.size() != 0)
        {
            s = queue.poll();
            results << s;
            matrix[s[0]].each { i ->
                if(!visited[i]){
                    visited[i] = true;
                    queue.add([i,s[1]+i])
                }
            }
        }
        return results
    }

    static def pageRank(List<Node> nodes){
        def pageRanks = []
        4.times{ i ->
            PageRank pr = new PageRank(nodes,0.2*i)
            pr.iterate()
            pageRanks << pr
        }
        nodes.eachWithIndex { e, i ->
            print "${e}\t"
            pageRanks.each{ pr ->
                print "\t${pr.ranks[i]}"
            }
            print "\n"
        }
    }

    static def wreckPage(List<Node> nodes) {
        nodes.each { node ->
            node.ins.removeAll{ it == node.id || it >= nodes.size() }
            node.outs.removeAll{ it == node.id || it >= nodes.size() }
        }

        List<Node> copyNodes = []
        nodes.each { it ->
            copyNodes << it
        }
        //Usuwanie wszystkich wierzchołkow po kolei - od najwiekszego pod wzgledem out
        def size = nodes.size()
        Node node = findMaxOut(nodes)
        int i = 0
        while(node){
            i++;
            //Get max out
            nodes.remove(node)
            node.outs.each{ nID ->
                def n = nodes.find { it.id == nID }
                if(n){
                    n.outs.removeAll{ it == node.id }
                    n.ins.removeAll{ it == node.id }
                }
            }
            println "${node.id}\t"+nodes.findAll {it.ins.size() == 0}.size() + "\t${i}\t"+nodes.size()
            node = findMaxOut(nodes)
        }
    }

    static def wreckPageRnd(List<Node> nodes){
        nodes.each { node ->
            node.ins.removeAll{ it == node.id || it >= nodes.size() }
            node.outs.removeAll{ it == node.id || it >= nodes.size() }
        }

        List<Node> copyNodes = []
        nodes.each { it ->
            copyNodes << it
        }
        //Usuwanie wszystkich wierzchołkow po kolei - od najwiekszego pod wzgledem out
        def size = nodes.size()
        Node node = getRnd(nodes)
        int i = 0
        while(node){
            i++;
            nodes.removeAll {it.id == node.id}
            node.outs.each{ nID ->
                def n = nodes.find { it.id == nID }
                if(n){
                    n.outs.removeAll{ it == node.id }
                    n.ins.removeAll{ it == node.id }
                }
            }
            //println "${node.id}\t"+nodes.findAll {it.ins.size() == 0}.size() + "\t${i}\t"+nodes.size()
            if(nodes.findAll {it.ins.size() == 0}.size() > 0){
                return i;
            }
            node = getRnd(nodes)
        }
    }


    static Node findMaxOut(List<Node> nodes) {
        Node node = null
        def max = 0
        nodes.each {
            if (it.outs.size() > max){
                max = it.outs.size()
                node = it
            }
        }
        //println "Znaleziono max: ${max} dla ${node.id}"
        return node
    }

    static Node getRnd(List<Node> nodes){
        Node node = nodes[rnd.nextInt(nodes.size())]
        node
    }
}

class Node{
    int id
    def ins
    def outs

    @Override
    public String toString() {
        return "\t${id}\t${ins.size()}\t${outs.size()}"
    }
}
class PageRank{

    List<Node> nodes
    def ranks
    def damping

    PageRank(nodes,damping=0){
        this.nodes = nodes
        this.damping = damping
        initRanks()
    }

    def initRanks() {
        this.ranks = []
        int size = nodes.size();
        size.times{
            ranks << 1.0/size
        }
    }

    def iterate(){
        def size = nodes.size()
        nodes.each{ node ->
            double rank = damping / size + (1.0 - damping) * computeRank(node);
            ranks[node.id] = rank
        }
    }

    def computeRank(Node node) {
        def sum = 0
        def parents = node.ins
        parents.each{ parentID ->
            Node parent = nodes[parentID]
            if(parent && parentID != node.id){
                sum += ranks[parentID] / parent.outs.size()
            }
        }
        return sum
    }


}


class ShortestPath {
    static final int V = 3000;

    int minDistance(def dist, def sptSet) {
        // Initialize min value
        int min = Integer.MAX_VALUE, min_index = -1;

        for (int v = 0; v < V; v++)
            if (sptSet[v] == false && dist[v] <= min) {
                min = dist[v];
                min_index = v;
            }

        return min_index;
    }
    void printSolution(def dist, int n) {
        System.out.println("Vertex   Distance from Source");
        for (int i = 0; i < V; i++)
            System.out.println(i + " \t\t " + dist[i]);
    }

    void dijkstra(def graph, int src) {

        def dist = new int[V]
        def sptSet = new Boolean[V];
        for (int i = 0; i < V; i++) {
            dist[i] = Integer.MAX_VALUE;
            sptSet[i] = false;
        }
        dist[src] = 0;
        for (int count = 0; count < V - 1; count++) {

            int u = minDistance(dist, sptSet);
            sptSet[u] = true;

            def adjSize = graph[u].size()
            for (int k = 0; k < adjSize; k++) {
                int v = graph[u][k]
                if (v < V && !sptSet[v]&&
                        dist[u] != Integer.MAX_VALUE &&
                        dist[u] + 1 < dist[v])
                    dist[v] = dist[u] + 1;
            }
        }
        printSolution(dist, V);
    }

}

class Crawler {

    private final String SITE_URL
    def links = [] as Queue
    def visitedLinks = []
    def deniedRules = []
    private Object listLock = new Object[0]
    InfoLogger logger = new InfoLogger();

    Crawler(url){
        this.SITE_URL = url
    }

    public def download(numOfThreads){

        println "Start downloading..."

        def s1 = System.currentTimeMillis()
        String url = SITE_URL
        readRobot()
        visitLink(url)
        def threads = []
        numOfThreads.times {
            threads += Thread.start {
                visiter()
            }
        }
        boolean stop = false
        while (!stop) {
            sleep(500)
            stop = true
            if (threads.any { it.state == Thread.State.RUNNABLE }) {
                stop = false
            }
        }
        threads.each {
            it.interrupt()
        }
        def s2 = System.currentTimeMillis()
        println "${numOfThreads}\t" + (s2 - s1)

        println "Stop downloading..."
    }

    def readRobot() {
        def robots = SITE_URL+"/robots.txt"
        def response =  Jsoup.connect(robots).execute()
        Document doc = response.parse()
        def robotstxt = ""
        if (response.statusCode() == 200){
            robotstxt = Jsoup.connect(robots).ignoreContentType(true).execute().body();
        }
        robotstxt.eachLine {
            def tokens = it.tokenize()
            if(tokens[0] == "Disallow:"){
                deniedRules += getDomainName(SITE_URL) + tokens[1]
            }
        }

    }

    def visiter(){
        int visited = 0
        while(true){
            try {
                def link = getLink()
                if (visitLink(link)) {
                    visited++
                }
            }catch (InterruptedException e){
                //println "Visited by: ${Thread.currentThread().getId()} - ${visited}"
            }
        }
    }

    def getLink(){
        synchronized(listLock){
            if (links.empty) {
                listLock.wait()
            }
            def url = links.poll()
            visitedLinks += url
            if (visitedLinks.size() % 10 == 0)
                logger.logCount(links.size(), visitedLinks.size())
            return url
        }
    }

    def addLink(String link){
        synchronized(listLock) {
            if (visitedLinks.size() + links.size() < 500 && !visitedLinks.contains(link) && !links.contains(link)) {
                links << link
                listLock.notifyAll()
            }
        }
    }


    def visitLink(String url){
        try{
            def start = System.currentTimeMillis()
            def response =  Jsoup.connect(url).execute()
            Document doc = response.parse()
            def stop = System.currentTimeMillis()
            println stop-start
            Elements hrefs = doc.select("a[href]")
            List<String> shrefs = []
            for (Element href : hrefs) {
                String ahref = href.attr("abs:href")
                if(canVisit(ahref)){
                    shrefs += ahref
                    addLink(ahref)
                    //saveToFile(url,doc)
                }
            }
            logSite(new Site(absolutURL: url,sites:shrefs))
        }catch (IOException e){
            println e.class.simpleName
            return false
        }
        return true

    }

    @Synchronized
    def logSite(Site site) {
        logger.saveSite(site)
    }

    def saveToFile(String name, Document body) {
        def file = new File("C:/tmp/${getDomainName(SITE_URL)}/"+generateMD5_A(name))
        file.write body.html()
    }

    boolean canVisit(String url) {
        if(!url)
            return false
        if(!url.startsWith("http"))
            return false
        if(!(getDomainName(url) == getDomainName(SITE_URL)))
            return false
        for (it in deniedRules) {
            if (url.contains(it))
                return false
        }
        return true
    }

    String getDomainName(String url){
        try {
            URI uri = new URI(url)
            String domain = uri.getHost()
            def ret = domain.startsWith("www.") ? domain.substring(4) : domain
            return ret
        }catch (Exception e){
            println e.message
            return ""
        }
    }

    def generateMD5_A(String s){
        MessageDigest.getInstance("MD5").digest(s.bytes).encodeHex().toString()
    }
}

class Site {
    String absolutURL
    List<String> sites
}

class InfoLogger {

    def timestamp = new Date().getTime()
    File countFile = new File("C:/tmp/${timestamp}-count.log")
    File sites = new File("C:/tmp/${timestamp}-sites.log")
    File enumaratedSites = new File("C:/tmp/${timestamp}-enumeratedSites.log")
    int siteCounter = 0

    def logCount(int visited,int leftToVisit){
        countFile << "${visited}\t${leftToVisit}\n"
    }

    def saveSite(crawler.Site site){
        sites << new JsonBuilder(site).toString() + "\n"
        enumaratedSites << "${siteCounter}\t${site.absolutURL}\n"
        siteCounter++
    }

}
