import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.treasuredata.client.*;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDResultFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

/**
 * Created by ttruong on 2018-11-12.
 */
public class TDClientQuery
{
    private HashMap parameter;
    private static final Logger logger = LoggerFactory.getLogger(TDClientQuery.class);
    public static final String DB = "-db";
    public static final String TB = "-tb";
    public static final String CL = "-c";
    public static final String MIN = "-m";
    public static final String MAX = "-M";
    public static final String ENG = "-e";
    public static final String LIM = "-l";

    // This option has not been supported yet
    public final String FRM = "-f";

    public TDClientQuery(){
        parameter = new HashMap();
        parameter.put(DB, null);
        parameter.put(TB, null);
        parameter.put(CL, null);
        parameter.put(MIN, null);
        parameter.put(MAX, null);
        parameter.put(ENG, "presto");
        parameter.put(FRM, "tabular");
        parameter.put(LIM, null);
    }

    public void update(HashMap param){
        Set keys = param.keySet();
        Iterator i = keys.iterator();
        while (i.hasNext()){
            Object keyName = i.next();
            parameter.put(keyName, param.get(keyName));
        }
    }

    public void put(String keyName, Object value){
        parameter.put(keyName, value);
    }

    public boolean verifyParameters(){
        if (parameter.get(DB) == null){
            logger.error("Cannot find db_name or its value is null");
            return false;
        }
        if (parameter.get(TB) == null){
            logger.error("Cannot find tb_name or its value is null");
            return false;
        }
        if (parameter.get(MIN) != null && parameter.get(MAX) != null){
            if (Integer.parseInt(parameter.get(MIN).toString()) > Integer.parseInt(parameter.get(MAX).toString())){
                logger.error("Max timestamp is less than Min timestamp");
                return false;
            }
        }
        return true;
    }

    public String createQuery(){
        String query = "";
        if (verifyParameters()){
            query += (parameter.get(CL) == null || parameter.get(CL).toString().trim().isEmpty())? "select * from " : "select " + parameter.get(CL).toString() + " from ";
            query += parameter.get(TB).toString() + " ";
            if (parameter.get(MIN) != null || parameter.get(MAX) != null){
                query += "where ";
                if (parameter.get(MIN) != null)
                    query += "executedend >= " + parameter.get(MIN).toString() + " ";
                if (parameter.get(MAX) != null)
                    if (parameter.get(MIN) != null)
                        query += "and executedend <= " + parameter.get(MAX).toString() + " ";
                    else
                        query += "executedend <= " + parameter.get(MAX).toString() + " ";
            }
            if (parameter.get(LIM) != null)
                query += "limit " + parameter.get(LIM).toString();
        }
        return query;
    }

    public String executeQuery(TDClient client, String query){
        String jobId;
        if (parameter.get(ENG).toString().equalsIgnoreCase("presto"))
            jobId = client.submit(TDJobRequest.newPrestoQuery(parameter.get(DB).toString(), query));
        else {
            jobId = client.submit(TDJobRequest.newHiveQuery(parameter.get(DB).toString(), query));
        }
        return jobId;
    }

    // Parse parameters provided from command line
    public HashMap parseCommand(String [] args){
        HashMap result = new HashMap();

        for (int i = 0; i < args.length; i += 2){

            // Options should start wit '-'
            if (!args[i].startsWith("-")){
                logger.error("Option " + args[i] + " should start with '-'");
                break;
            }

            // No option or value provided
            if (args[i].length() < 2){
                logger.error("Option " + args[i] + " should has lenght greater than 2");
                break;
            }

            // parameters into a java hash map
            result.put(args[i], args[i + 1]);
        }
        return result;
    }

    public String [] stringToArray(String string){
        return string.split(" ");
    }

    public boolean executeQuery (TDClient client, String [] cmd){
        try{
            update(parseCommand(cmd));
            String jobId = executeQuery(client,createQuery());

            // Wait until the query finishes
            ExponentialBackOff backOff = new ExponentialBackOff();
            TDJobSummary job = client.jobStatus(jobId);
            while (!job.getStatus().isFinished()) {
                Thread.sleep(backOff.nextWaitTimeMillis());
                job = client.jobStatus(jobId);
            }

            // Read the detailed job information
            TDJob jobInfo = client.jobInfo(jobId);
            System.out.println("log:\n" + jobInfo.getCmdOut());
            System.out.println("error log:\n" + jobInfo.getStdErr());
            // Read the job results in msgpack.gz format
            client.jobResult(jobId, TDResultFormat.MESSAGE_PACK_GZ, new Function<InputStream, Integer>()
            {
                @Override
                public Integer apply(InputStream input)
                {
                    int count = 0;
                    try {
                        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(input));
                        while (unpacker.hasNext()) {
                            // Each row of the query result is array type value (e.g., [1, "name", ...])
                            ArrayValue array = unpacker.unpackValue().asArrayValue();
                            System.out.println(array);
                            count++;
                        }
                        unpacker.close();
                    }
                    catch (Exception e) {
                        throw Throwables.propagate(e);
                    }
                    return count;
                }
            });
        } catch(Exception e){
            logger.error("Exception: " + e.toString());
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static void main (String [] args){
        TDClient client = TDClient.newClient();
        TDClientQuery query = new TDClientQuery();
        query.executeQuery(client, args);
    }
}
