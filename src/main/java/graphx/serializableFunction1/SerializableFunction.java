package graphx.serializableFunction1;

import org.apache.spark.api.java.function.Function;

/**
 * Created by wso2 on 20/1/17.
 */
public abstract class SerializableFunction<k,t> implements Function<k,t> , java.io.Serializable{
}
