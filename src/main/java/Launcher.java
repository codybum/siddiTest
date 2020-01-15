
public class Launcher {

    public static void main(String[] args) {

        try {

            CEPEngine cepEngine = new CEPEngine();

//public void createCEP(String inputRecordSchemaString, String inputStreamName, String inputTopic, String outputStreamName, String outputTopic, String outputStreamAttributesString,String queryString) {

            String inputRecordSchemaString = "{\"type\":\"record\",\"name\":\"Ticker\",\"fields\":[{\"name\":\"source\",\"type\":\"string\"},{\"name\":\"urn\",\"type\":\"string\"},{\"name\":\"metric\",\"type\":\"string\"},{\"name\":\"ts\",\"type\":\"long\"},{\"name\":\"value\",\"type\":\"double\"}]}";
            String inputStreamName = "UserStream";

            String outputStreamName = "BarStream";
            String outputStreamAttributesString = "source string, avgValue double";

            String queryString = " " +
                    //from TempStream#window.timeBatch(10 min)
                    //"from UserStream#window.time(5 sec) " +
                    "from UserStream#window.timeBatch(5 sec) " +
                    "select source, avg(value) as avgValue " +
                    "  group by source " +
                    "insert into BarStream; ";

            cepEngine.createCEP(inputRecordSchemaString, inputStreamName, outputStreamName, outputStreamAttributesString, queryString);

            System.out.println("OUTPUT SCHEMA : " + cepEngine.getSchema(outputStreamName));

            cepEngine.input(inputStreamName, cepEngine.getStringPayload());

            //while (true) {
            //    cepEngine.input(inputStreamName, cepEngine.getStringPayload());
            //}

            //cepEngine.shutdown();

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

}
