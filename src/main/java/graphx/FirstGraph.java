package graphx;

import graphx.serializableFunction1.SerialiFunJRdd;
import graphx.serializableFunction1.SerializableFunction;
import graphx.serializableFunction1.SerializableFunction1;
import graphx.sharedSparkContext.SharedSC;
import graphx.sharedSparkContext.SoleSc;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.BoxedUnit;

import java.util.List;


public class FirstGraph{
    static SharedSC sc = new SharedSC();
    String stringlist;
    static SerialiFunJRdd<Tuple2<Row, java.lang.Long>, Row> myFunc;

public static void findSolutions(){
    try {

        System.out.println(sc.getSparkContext().appName().toString());
        DataFrame edge_dataframe = sc.getEdgeDataFrame("EDGE_RDD", -1234, sc.getSparkContext());
        DataFrame vertex_dataframe = sc.getVertexDataFrame("VERTEX_RDD", -1234, sc.getSparkContext());
        System.out.println(vertex_dataframe.collectAsList().toString());

        //DataFrame vertex_dataframe = SoleSc.getVertexDataFrame();
        //DataFrame edge_dataframe = SoleSc.getEdgeDataFrame();


        //add long unique id for vertex dataframe and get javaRdd
        myFunc = new SerialiFunJRdd<Tuple2<Row, java.lang.Long>, Row>() {
            public Row call(Tuple2<Row, java.lang.Long> rowLongTuple2) throws Exception {
                return RowFactory.create(rowLongTuple2._1().getString(0), rowLongTuple2._2());
            }
        };
        JavaRDD<Row> ff = vertex_dataframe.javaRDD().zipWithIndex().map(myFunc);

        //convert JavaRdd<Row> to JavaRdd<Tuple2<Long,String>>
        JavaRDD<Tuple2<Object,String>> vertex_javardd = ff.map(new SerializableFunction<Row, Tuple2<Object, String>>() {
            public Tuple2<Object, String> call(Row row) throws Exception {
                return new Tuple2<Object, String>(new Long(row.getLong(1)),row.getString(0));
            }
        });

        //convert JavaRdd<Tuple2<Long,String>> to Rdd<Tuple2<Long,String>> to create graph
        RDD<Tuple2<Object,String>> vertex_rdd = vertex_javardd.rdd();

        //print the vertex_rdd
        vertex_rdd.foreach(new SerializableFunction1<Tuple2<Object, String>, BoxedUnit>() {
            public BoxedUnit apply(Tuple2<Object, String> v1) {
                System.out.println(v1.toString());
                return BoxedUnit.UNIT;
            }
        });

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

        //print the edge_rdd
        edge_rdd.foreach(new SerializableFunction1<Edge<String>, BoxedUnit>() {
            public BoxedUnit apply(Edge<String> v1) {
                System.out.println(v1.toString());
                return BoxedUnit.UNIT;
            }
        });

        //populate the graph
        Graph graph = Graph.apply(vertex_rdd,edge_rdd,"defaultVertexAttr",StorageLevel.MEMORY_ONLY(),StorageLevel.MEMORY_ONLY(),classtagstring,classtagstring);
        System.out.println(graph.vertices().count());
        /*graph.vertices().foreach(new SerializableFunction1<Tuple2<Long,String>,BoxedUnit>() {
            public BoxedUnit apply(Tuple2<Long,String> v1) {
                System.out.println(v1);
                return  BoxedUnit.UNIT;
            }
        });*/


    } catch (Exception e) {
        System.out.print(e.toString());
    }
}
/*public  static  void main(String[] ss){
    FirstGraph.findSolutions();
}*/
//map Long id with the edge rdd sources and destinations
private static Long getLongIdForVertex(List<Tuple2<Object,String>> vartexlist,String str){
    Long kk = 0L;
    for (int i = 0; i < vartexlist.size(); i++) {
        Tuple2<Object,String> hh = vartexlist.get(i);
        String ll = hh._2();
        if(ll.trim().compareTo(str.trim())==0){
            kk = Long.parseLong(hh._1().toString());
            break;
        }
    }
    return kk;

}

}