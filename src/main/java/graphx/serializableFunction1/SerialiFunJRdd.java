package graphx.serializableFunction1;

import org.apache.spark.api.java.function.Function;

/**
 * Created by wso2 on 19/1/17.
 */
public abstract class SerialiFunJRdd<T1,R> implements Function<T1, R> , java.io.Serializable{

}

