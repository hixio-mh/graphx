package graphx.patternMatching;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.spark.graphx.*;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wso2 on 5/2/17.
 */
public class MinePatterns {
    private  String[] str;
    private Graph graph;
    private List<Long> idList;
    private ClassTag<String> classtagstring;
    private ClassTag<EdgeTriplet<VertexRDD,Edge>> classtagedgetriplet;
    private GraphOps graphOps;
    private VertexRDD<Tuple2<Object,String>[]> neighboursOfAllVetexesAsVertexRdd;
    private Tuple2<Object,Tuple2<Object,String>[]>[] neighboursOfAllVetexesAsTupleArray;
    private Tuple2<String,String>[] solutionids;
    private List<Tuple2<Object,String>> vertexlist;

    public MinePatterns(final String[] str, Graph graph, List<Long> idList, Tuple2<String,String>[] solutionids, List<Tuple2<Object,String>> vertexlist){
        this.str = str;
        this.graph = graph;
        this.idList = idList;
        this.classtagstring  = ClassTag$.MODULE$.apply(String.class);
        this.classtagedgetriplet  = ClassTag$.MODULE$.apply(EdgeTriplet.class);
        this.graphOps = new GraphOps(graph,classtagstring,classtagstring);
        this.neighboursOfAllVetexesAsVertexRdd = graphOps.collectNeighbors(EdgeDirection.Out());
        this.neighboursOfAllVetexesAsTupleArray = (Tuple2<Object, Tuple2<Object,String>[]>[]) neighboursOfAllVetexesAsVertexRdd.collect();
        this.solutionids = solutionids;
        this.vertexlist = vertexlist;
    }
    public JsonObject patternMining(){
        List<String> minedSolutions = new ArrayList<String>();
        List<String> ending_vertex_ids = new ArrayList<String>();
        List<String> minedPatterns = new ArrayList<String>();
        List<String> temp = new ArrayList<String>();

        JsonObject finalArnswer = new JsonObject();
        JsonObject solutions = new JsonObject();

        for(int i=0; i<idList.size()-1; i++){
            if(this.getMappingNeighbour(idList.get(i),idList.get(i+1))){
                temp.add(str[i]);
                innerLoop:for(Tuple2<String,String> ff : solutionids){
                    if(ff._1().equals(str[i+1])){
                        minedSolutions.add(ff._2());
                        ending_vertex_ids.add(str[i+1]);
                        temp.add(str[i+1]);
                        minedPatterns.add(temp.toString());
                        temp.remove(temp.size()-1);
                        break innerLoop;
                    }
                }
            }
            else{
                temp.clear();
            }
        }
        String minedSolutionsJson = new Gson().toJson(minedSolutions);
        String ending_vertex_idsJson = new Gson().toJson(ending_vertex_ids);
        String minedPatternsJson = new Gson().toJson(minedPatterns);

        solutions.addProperty("minedSolutions",minedSolutionsJson);
        solutions.addProperty("ending_vertex_idsJson",ending_vertex_idsJson);
        solutions.addProperty("minedPatternsJson",minedPatternsJson);
        finalArnswer.add("message",solutions);

        return  finalArnswer;
    }
    public Boolean getMappingNeighbour(Long srcId,Long destId){
        for(int i=0; i<neighboursOfAllVetexesAsTupleArray[Integer.parseInt(srcId.toString())]._2().length; i++){
            if(((Number)neighboursOfAllVetexesAsTupleArray[1]._2()[i]._1()).longValue() == destId){
                return true;
            }
        }
        return false;
    }
}
