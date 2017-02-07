package graphx;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import graphx.patternMatching.MinePatterns;
import graphx.serializableFunction1.SerialiFunJRdd;
import graphx.serializableFunction1.SerializableFunction;
import graphx.serializableFunction1.SerializableFunction1;
import graphx.sharedSparkContext.SharedSC;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.storage.StorageLevel;
import org.wso2.carbon.analytics.jsservice.AnalyticsJSServiceConnector;
import org.wso2.carbon.analytics.jsservice.beans.ResponseBean;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.BoxedUnit;

import java.util.ArrayList;
import java.util.List;


public class FirstGraph{
    static SharedSC sc = new SharedSC();
    String stringlist;
    static SerialiFunJRdd<Tuple2<Row, java.lang.Long>, Row> myFunc;
    private  static DataFrame edge_dataframe;
    private  static DataFrame vertex_dataframe;
    private  static DataFrame solution_id;
    private static JsonObject json;

    public FirstGraph(){
        try {
            this.edge_dataframe = sc.getEdgeDataFrame("EDGE_RDD", -1234, sc.getSparkContext());
            this.vertex_dataframe = sc.getVertexDataFrame("VERTEX_RDD", -1234, sc.getSparkContext());
            this.solution_id = sc.getVertexDataFrame("SOLUTION_ID_WITH_ENDING_VERTEX", -1234, sc.getSparkContext());
        }catch(Exception e){
            System.out.println(e.toString());
        }

    }

public static ResponseBean findSolutions(String pettern){
    AnalyticsJSServiceConnector ajssc = new AnalyticsJSServiceConnector();
    try {


        //DataFrame vertex_dataframe = SoleSc.getVertexDataFrame();
        //DataFrame edge_dataframe = SoleSc.getEdgeDataFrame();
        //DataFrame solution_id = SoleSc.getSolutionsWithVertex();

        DataFrame final_vertex_dataframe = vertex_dataframe.join(solution_id,vertex_dataframe.col("vertex").equalTo(solution_id.col("ending_vertex")),"left").drop("ending_vertex");


        //add long unique id for vertex dataframe and get javaRdd
        myFunc = new SerialiFunJRdd<Tuple2<Row, java.lang.Long>, Row>() {
            public Row call(Tuple2<Row, java.lang.Long> rowLongTuple2) throws Exception {
                return RowFactory.create(rowLongTuple2._1().getString(0), rowLongTuple2._2());
            }
        };
        JavaRDD<Row> ff = final_vertex_dataframe.javaRDD().zipWithIndex().map(myFunc);

        //convert JavaRdd<Row> to JavaRdd<Tuple2<Long,String>>
        JavaRDD<Tuple2<Object,String>> vertex_javardd = ff.map(new SerializableFunction<Row, Tuple2<Object, String>>() {
            public Tuple2<Object, String> call(Row row) throws Exception {
                return new Tuple2<Object, String>(new Long(row.getLong(1)),row.getString(0));
            }
        });

        //convert JavaRdd<Tuple2<Long,String>> to Rdd<Tuple2<Long,String>> to create graph
        RDD<Tuple2<Object,String>> vertex_rdd = vertex_javardd.rdd();


        final List<Tuple2<Object,String>> vartexlist= vertex_javardd.collect();

        //creating class tags for graphx
        ClassTag<Edge<String>> classtagedge  = ClassTag$.MODULE$.apply(Edge.class);
        ClassTag<String> classtagstring  = ClassTag$.MODULE$.apply(String.class);

        //final RDD<Edge<String>>
        RDD<Edge<String>> edge_rdd = edge_dataframe.map(new SerializableFunction1<Row, Edge<String>>() {
            public Edge<String> apply(Row v1) {
                Edge edge = new Edge(getLongIdForVertex(vartexlist,v1.getString(0)),getLongIdForVertex(vartexlist,v1.getString(1)),"f");
                return edge;
            }
        },classtagedge);

        vertex_rdd.foreach(new SerializableFunction1<Tuple2<Object, String>, BoxedUnit>() {
            public BoxedUnit apply(Tuple2<Object, String> v1) {
                System.out.println(v1.toString());
                return BoxedUnit.UNIT;
            }
        });

        edge_rdd.foreach(new SerializableFunction1<Edge<String>, BoxedUnit>() {
            public BoxedUnit apply(Edge<String> v1) {
                System.out.println(v1.toString());
                return BoxedUnit.UNIT;
            }
        });


        //populate the graph
        Graph graph = Graph.apply(vertex_rdd,edge_rdd,"defaultVertexAttr",StorageLevel.MEMORY_ONLY(),StorageLevel.MEMORY_ONLY(),classtagstring,classtagstring);


        ClassTag<Tuple2<String,String>> classtagtuple2  = ClassTag$.MODULE$.apply(Tuple2.class);
        RDD<Tuple2<String,String>> solution_ids = solution_id.map(new SerializableFunction1<Row,Tuple2<String,String>>() {
            public Tuple2<String,String> apply(Row v1){
                return new Tuple2<String, String>(v1.getString(0),v1.getString(1));
            }
        },classtagtuple2);

        Tuple2<String,String>[] solutionidlist = (Tuple2<String,String>[])solution_ids.collect();

        //final String[] nn = new String[]{"6:1","3:3","5:3"};
        String[] kk = pettern.split("\"");
        final String[] nn = kk[1].split("-");//new String[]{"8:7","7:8","8:7","8:7","8:7","7:9","7:9","7:8","7:9","7:8","8:7","8:7","8:7"};//
        for(String jj: nn){
            System.out.println(jj);
        }
        List<Long> jj = new ArrayList<Long>();
        for (String t : nn) {
            for(Tuple2<Object,String> st : vartexlist){
                if(t.equals(st._2().trim())){
                    jj.add(new Long((Long)st._1()));
                }
            }
        }
        MinePatterns ll = new MinePatterns(nn,graph,jj,solutionidlist,vartexlist);
        System.out.println(pettern);
        json = ll.patternMining();

    } catch (Exception e) {
        return ajssc.handleResponse(AnalyticsJSServiceConnector.ResponseStatus.FAILED,"spark Exception:"+e.toString());
    }
    return ajssc.handleResponse(AnalyticsJSServiceConnector.ResponseStatus.SUCCESS,new Gson().toJson(json));
}
//public  static  void main(String[] ss){
 //   FirstGraph.findSolutions();
//}


//map Long id with the edge rdd sources and destinations
private static Long getLongIdForVertex(List<Tuple2<Object,String>> vartexlist,String str) {
    Long kk = 0L;
    for (int i = 0; i < vartexlist.size(); i++) {
        Tuple2<Object, String> hh = vartexlist.get(i);
        String ll = hh._2();
        if (ll.trim().compareTo(str.trim()) == 0) {
            kk = Long.parseLong(hh._1().toString());
            break;
        }
    }
    return kk;

}

}