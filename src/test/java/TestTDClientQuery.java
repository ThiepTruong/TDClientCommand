import com.treasuredata.client.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by ttruong on 2018-11-12.
 */
public class TestTDClientQuery
{
    private static final String SAMPLE_DB = "automation_data";
    private static final String SAMPLE_TB = "automation_result";
    public static final Logger logger = LoggerFactory.getLogger(TestTDClientQuery.class);
    private List<String> savedQueries = new ArrayList<>();
    private TDClient client;

    @Before
    public void setUp() throws Exception {
        client = TDClient.newClient();
    }

    @After
    public void tearDown()
            throws Exception
    {
        for (String name : savedQueries) {
            try {
                client.deleteSavedQuery(name);
            }
            catch (Exception e) {
                logger.error("Failed to delete query: {}", name, e);
            }
        }
        client.close();
    }

    @Test
    public void sellectSome(){
        logger.info("\n-----------------------------------> start sellectSome\n");
        TDClientQuery query = new TDClientQuery();
        String cmd = String.format("-c %s -l %s -e %s -db %s -tb %s -m %s -M %s", "items,passed,failed", "10", "presto", SAMPLE_DB, SAMPLE_TB, "1510480920", "1541930460");
        assertTrue(query.executeQuery(client, query.stringToArray(cmd)));
        logger.info("\n-----------------------------------> end sellectSome\n");
    }

    @Test
    public void sellectAll(){
        logger.info("\n-----------------------------------> start sellectAll\n");
        TDClientQuery query = new TDClientQuery();
        String cmd = String.format("-l %s -e %s -db %s -tb %s -m %s -M %s", "10", "presto", SAMPLE_DB, SAMPLE_TB, "1510480920", "1541930460");
        assertTrue(query.executeQuery(client, query.stringToArray(cmd)));
        logger.info("\n-----------------------------------> end sellectAll\n");
    }

    @Test
    public void noTime(){
        logger.info("\n-----------------------------------> start noTime\n");
        TDClientQuery query = new TDClientQuery();
        String cmd = String.format("-l %s -e %s -db %s -tb %s", "10", "presto", SAMPLE_DB, SAMPLE_TB);
        assertTrue(query.executeQuery(client, query.stringToArray(cmd)));
        logger.info("\n-----------------------------------> end noTime\n");
    }

    @Test
    public void onlyMin(){
        logger.info("\n-----------------------------------> start onlyMin\n");
        TDClientQuery query = new TDClientQuery();
        String cmd = String.format("-l %s -e %s -db %s -tb %s -m %s", "10", "presto", SAMPLE_DB, SAMPLE_TB, "1510480920");
        assertTrue(query.executeQuery(client, query.stringToArray(cmd)));
        logger.info("\n-----------------------------------> end onlyMin\n");
    }

    @Test
    public void onlyMax(){
        logger.info("\n-----------------------------------> start onlyMax\n");
        TDClientQuery query = new TDClientQuery();
        String cmd = String.format("-l %s -e %s -db %s -tb %s -M %s", "10", "presto", SAMPLE_DB, SAMPLE_TB, "1510480920");
        assertTrue(query.executeQuery(client, query.stringToArray(cmd)));
        logger.info("\n-----------------------------------> end onlyMax\n");
    }

    @Test
    public void maxLessThanMin(){
        logger.info("\n-----------------------------------> start maxLessThanMin\n");
        TDClientQuery query = new TDClientQuery();
        String cmd = String.format("-l %s -e %s -db %s -tb %s -m %s -M %s", "10", "presto", SAMPLE_DB, SAMPLE_TB, "1541930460", "1510480920");
        assertFalse(query.executeQuery(client, query.stringToArray(cmd)));
        logger.info("\n-----------------------------------> end maxLessThanMin\n");
    }

    @Test
    public void wrongTable(){
        logger.info("\n-----------------------------------> start wrongTable\n");
        TDClientQuery query = new TDClientQuery();
        String cmd = String.format("-l %s -e %s -db %s -tb %s -m %s", "10", "presto", SAMPLE_DB, "wrong_table", "1541930460");
        assertFalse(query.executeQuery(client, query.stringToArray(cmd)));
        logger.info("\n-----------------------------------> end wrongTable\n");
    }

    @Test
    public void wrongDatabase(){
        logger.info("\n-----------------------------------> start wrongDatabase\n");
        TDClientQuery query = new TDClientQuery();
        String cmd = String.format("-l %s -e %s -db %s -tb %s -m %s", "10", "presto", "wrong_db", SAMPLE_TB, "1541930460");
        assertFalse(query.executeQuery(client, query.stringToArray(cmd)));
        logger.info("\n-----------------------------------> end wrongDatabase\n");
    }

}
