import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.UUID;

public class MainJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        ParameterTool parameter = ParameterTool.fromArgs(args);

        String[] inputs = parameter.getRequired("input").split(";");
        String DBconnection = inputs[0];
        String DBuser = inputs[1];
        String DBpassword =  inputs[2];
        String DBname = inputs[3], DBtable = inputs[4];
        String topic = inputs[6], bootstrapServer = inputs[7];
        String schema = inputs[5].replace("%"," ");
        String key = null;
        if(inputs.length>7)
            key = inputs[8];
        String inputQuery = "CREATE TABLE InputTable("+ schema;
        String sinkQuery  = "CREATE TABLE SinkTable ("+ schema;

        if(key != null){
            inputQuery = inputQuery.concat(",PRIMARY KEY(`"+key+"`) NOT ENFORCED"
                            + ")"
                            + "WITH"
                            + "("
                            + "'connector' = 'upsert-kafka',"
                            + "'value.format' = 'json',"
                            + "'key.format' = 'json',");
            sinkQuery = sinkQuery.concat(",PRIMARY KEY(`"+key+"`) NOT ENFORCED");
        }
        else{
            inputQuery = inputQuery.concat(")"
                    + "WITH"
                    + "("
                    + "'connector' = 'kafka',"
                    + "'scan.startup.mode' = 'earliest-offset',"
                    + "'format' = 'json',");
        }
       inputQuery = inputQuery.concat("'topic' = '"+topic+"',"
                       + "'properties.bootstrap.servers' = '"+bootstrapServer+"',"
                       + "'properties.group.id' = '"+UUID.randomUUID().toString()+"'"
                       + ")");
       sinkQuery = sinkQuery.concat(")"
                       + "WITH"
                       + "("
                       + "'connector' = 'jdbc',"
                       + "'url' = '"+DBconnection+"/"+DBname+"?user="+DBuser+"&password="+DBpassword+"',"
                       + "'table-name' = '"+DBtable+"')");
       tEnv.executeSql(inputQuery);
       tEnv.executeSql(sinkQuery);
       Table Data = tEnv.from("InputTable");
       Data.executeInsert("SinkTable");
   }
}