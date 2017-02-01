package graphx.sharedSparkContext;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.wso2.carbon.analytics.api.AnalyticsDataAPI;
import org.wso2.carbon.analytics.datasource.commons.AnalyticsSchema;
import org.wso2.carbon.analytics.datasource.commons.ColumnDefinition;
import org.wso2.carbon.analytics.datasource.commons.exception.AnalyticsException;
import org.wso2.carbon.analytics.datasource.commons.exception.AnalyticsTableNotAvailableException;
import org.wso2.carbon.context.PrivilegedCarbonContext;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by wso2 on 17/1/17.
 */
public class SharedSC {
    private JavaSparkContext sparkContext ;
    public SharedSC(){

        this.sparkContext=new JavaSparkContext(SparkContext.getOrCreate());
    }

    public static DataFrame getVertexDataFrame(String tableName, int tenantId, JavaSparkContext sparkContext)
            throws AnalyticsTableNotAvailableException, AnalyticsException {
        String tableSchema = extractTableSchema(tableName, tenantId);
        SQLContext sqlCtx = new SQLContext(sparkContext);
        sqlCtx.sql("CREATE TEMPORARY TABLE VERTEX_RDD USING org.wso2.carbon.analytics.spark.core.sources.AnalyticsRelationProvider "
                + "OPTIONS ("
                + "tenantId \""
                + tenantId
                + "\", "
                + "tableName \""
                + tableName
                + "\", "
                + "schema \""
                + tableSchema + "\"" + ")");

        DataFrame dataFrame = sqlCtx.sql("select * from VERTEX_RDD");
        // Additional auto-generated column "_timestamp" needs to be dropped because it is not in the schema.
        dataFrame = dataFrame.drop("_timestamp");
        return dataFrame;
    }

    public static DataFrame getEdgeDataFrame(String tableName, int tenantId, JavaSparkContext sparkContext)
            throws AnalyticsTableNotAvailableException, AnalyticsException {
        String tableSchema = extractTableSchema(tableName, tenantId);
        SQLContext sqlCtx = new SQLContext(sparkContext);
        sqlCtx.sql("CREATE TEMPORARY TABLE EDGE_RDD USING org.wso2.carbon.analytics.spark.core.sources.AnalyticsRelationProvider "
                + "OPTIONS ("
                + "tenantId \""
                + tenantId
                + "\", "
                + "tableName \""
                + tableName
                + "\", "
                + "schema \""
                + tableSchema + "\"" + ")");

        DataFrame dataFrame = sqlCtx.sql("select * from EDGE_RDD");
        // Additional auto-generated column "_timestamp" needs to be dropped because it is not in the schema.
        dataFrame = dataFrame.drop("_timestamp");
        return dataFrame;
    }

    public static String extractTableSchema(String path, int tenantId) throws AnalyticsTableNotAvailableException,
            AnalyticsException {
        if (path == null) {
            return null;
        }
        AnalyticsDataAPI analyticsDataApi = (AnalyticsDataAPI) PrivilegedCarbonContext.getThreadLocalCarbonContext()
                .getOSGiService(AnalyticsDataAPI.class, null);
        // table schema will be something like; <col1_name> <col1_type>,<col2_name> <col2_type>
        StringBuilder sb = new StringBuilder();
        AnalyticsSchema analyticsSchema = analyticsDataApi.getTableSchema(tenantId, path);
        Map<String, ColumnDefinition> columnsMap = analyticsSchema.getColumns();
        for (Iterator<Map.Entry<String, ColumnDefinition>> iterator = columnsMap.entrySet().iterator(); iterator
                .hasNext();) {
            Map.Entry<String, ColumnDefinition> column = iterator.next();
            sb.append(column.getKey() + " " + column.getValue().getType().name() + ",");
        }

        return sb.substring(0, sb.length() - 1);
    }
    public JavaSparkContext getSparkContext(){
        return this.sparkContext;
    }
}
