package org.embulk.output.marketo_leadbulk;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.Map;

import java.net.ConnectException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.nio.file.StandardOpenOption;

import java.io.IOException;
//import java.io.Writer;
//import java.io.FileInputStream;
//import java.io.FileOutputStream;

//import java.io.FileWriter;
import java.io.BufferedWriter;
//import java.io.PrintWriter;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.BadRequestException;


import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
//import org.glassfish.jersey.media.multipart.BodyPart;
//import org.glassfish.jersey.media.multipart.BodyPartEntity;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;



import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.Execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import org.slf4j.Logger;

public class MarketoLeadbulkOutputPlugin
        implements OutputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("account_id")
        public String getAccountId();

        @Config("client_id")
        public String getClientId();

        @Config("client_secret")
        public String getClientSecret();

        @Config("lookupfield")
        public String getLookupField();

        @Config("tempDir")
        public String getTempDir();

        @Config("maximum_retries")
        @ConfigDefault("7")
        public int getMaximumRetries();

        @Config("initial_retry_interval_millis")
        @ConfigDefault("1000")
        public int getInitialRetryIntervalMillis();

        @Config("maximum_retry_interval_millis")
        @ConfigDefault("60000")
        public int getMaximumRetryIntervalMillis();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // retryable (idempotent) output:
        // return resume(task.dump(), schema, taskCount, control);

        // non-retryable (non-idempotent) output:
        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("marketo_leadbulk output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        return new MarketoLeadbulkPageOutput(schema, task);
    }

    public class MarketoLeadbulkPageOutput
        implements TransactionalPageOutput
    {
        private final PageReader pageReader;
        private final Schema schema;
        private PluginTask task;

        private ClientConfig clientconfig = new ClientConfig()
            .register(MultiPartFeature.class)
            .property(ClientProperties.CONNECT_TIMEOUT, "5000")
            .property(ClientProperties.READ_TIMEOUT, "3000");
        private Client client = ClientBuilder.newClient(clientconfig);
        private RetryPolicy retryPolicy = new RetryPolicy();

        private String tempFilePath = ""; 
        private StringBuilder requestBody = new StringBuilder();

        public MarketoLeadbulkPageOutput(Schema schema, PluginTask task) {           
            this.pageReader = new PageReader(schema);
            this.schema = schema;
            this.task = task;

            retryPolicy.retryOn(ConnectException.class)
                       .withDelay(1, TimeUnit.SECONDS)
                       .withMaxRetries(this.task.getMaximumRetries());
        }

        @Override
        public void add(Page page) { 

            String delimiterString = ",";
            Path tempFile = null;

            try {
                tempFile = Files.createTempFile(Paths.get(task.getTempDir()), "_", ".tmp");
                this.tempFilePath = tempFile.toString();
                logger.info(this.tempFilePath);

                try (BufferedWriter writer =
                    Files.newBufferedWriter(tempFile, Charset.forName("UTF8"), StandardOpenOption.DELETE_ON_CLOSE)) {
                    
                    //Header
                    for (Column col : schema.getColumns()) {
                        if (col.getIndex() != 0 ) {
                            writer.write(delimiterString);
                            requestBody.append(delimiterString);
                        }
                        writer.write(col.getName());                        
                        requestBody.append(col.getName());                        
                    }
                    
                    writer.newLine();
                    requestBody.append("\r\n");

                    pageReader.setPage(page);
                    while (pageReader.nextRecord()) {

                        schema.visitColumns(new ColumnVisitor() {
                            
                            @Override
                            public void booleanColumn(Column column) {
                                addDelimiter(column);
                                if (pageReader.isNull(column)) {
                                    addText("");
                                } else {
                                    addText(String.valueOf(pageReader.getBoolean(column))); 
                                    logger.info(column.getName() + ":" + String.valueOf(pageReader.getBoolean(column)));
                                }
                            }

                            @Override
                            public void doubleColumn(Column column) {
                                addDelimiter(column);
                                if (pageReader.isNull(column)) {
                                    addText("");
                                } else {
                                    addText(String.valueOf(pageReader.getDouble(column)));
                                    logger.info(column.getName() + ":" + String.valueOf(pageReader.getDouble(column)));
                                }
                            }

                            @Override
                            public void stringColumn(Column column) {
                                addDelimiter(column);
                                if (pageReader.isNull(column)) {
                                    addText("");
                                } else {
                                    addText(String.valueOf(pageReader.getString(column)));
                                    logger.info(column.getName() + ":" + String.valueOf(pageReader.getString(column)));
                                }
                            }

                            @Override
                            public void longColumn(Column column) {
                                addDelimiter(column);
                                if (pageReader.isNull(column)) {
                                    addText("");
                                } else {
                                    addText(String.valueOf(pageReader.getLong(column)));
                                    logger.info(column.getName() + ":" + String.valueOf(pageReader.getLong(column)));
                                }
                            }

                            @Override
                            public void timestampColumn(Column column) {
                                addDelimiter(column);
                                if (pageReader.isNull(column)) {
                                    addText("");
                                } else {
                                    addText(String.valueOf(pageReader.getTimestamp(column)));
                                    logger.info(column.getName() + ":" + String.valueOf(pageReader.getTimestamp(column)));
                                }
                            }

                            @Override
                            public void jsonColumn(Column column) {
                                addDelimiter(column);
                                if (pageReader.isNull(column)) {
                                    // do nothing.
                                } else {
                                    // do nothing.
                                }
                            }

                            private void addDelimiter(Column column) {
                                try {
                                    if (column.getIndex() != 0) {
                                        writer.write(delimiterString);
                                        requestBody.append(delimiterString);
                                    }
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }

                            private void addText(String str) {
                                try {
                                    writer.write(str);
                                    requestBody.append(str);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }                           
                        }) ;

                        writer.newLine();
                        requestBody.append("\r\n");
                    }

                    writer.flush();

                    logger.info(requestBody.toString());
                    this.push();
                } 
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void finish()
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        public void abort()
        {
        }

        @Override
        public TaskReport commit()
        {
            return Exec.newTaskReport();
        }

        private void push() {
            String accessToken = this.getAccessToken();
            
            try {
                
                final FormDataMultiPart formDataMultiPart = new FormDataMultiPart();
                final FormDataContentDisposition dispo = FormDataContentDisposition
                        .name("file")
                        .fileName("lead.csv")
                        .size(requestBody.toString().getBytes().length)
                        .build();
                final FormDataBodyPart bodyPart = new FormDataBodyPart(dispo, requestBody.toString());
                formDataMultiPart.bodyPart(bodyPart);

                //MultiPart multiPart = new MultiPart(MediaType.MULTIPART_FORM_DATA_TYPE);
                //multiPart.getBodyParts().add(filePart);

                MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
                headers.putSingle("Authorization", "Bearer " + accessToken);
                
               // WebTarget target = client.target("http://localhost:3000")
               //                          .path("/samples")
               //                          .queryParam("format", "csv");
                
                WebTarget target = client.target("https://" + task.getAccountId() + ".mktorest.com")
                                         .path("/bulk/v1/leads.json")
                                         .queryParam("format", "csv");
            

                Execution execution = new Execution(retryPolicy);
                while (!execution.isComplete()) {
                    logger.info("Post to " + target.getUri().toString().replace(accessToken,"XXXXXX"));
                    Response response = Failsafe.with(retryPolicy).get(() -> target.request().headers(headers).post(Entity.entity(formDataMultiPart, MediaType.MULTIPART_FORM_DATA_TYPE),  Response.class));
                    String result = response.readEntity(String.class);
                    //ObjectMapper objectMapper = new ObjectMapper();
                    //Map<String,Object> map = objectMapper.readValue(result,  new TypeReference<Map<String, Object>>() {});
                    logger.info("Status code: " + Integer.toString(response.getStatus()) + " " + response.getStatusInfo().getReasonPhrase());
                    logger.info(result);
                    execution.complete();
                }

            } catch (BadRequestException e) {
                System.out.println("response=" + e.getResponse().readEntity(String.class));
            //} catch (IOException e) {
            //    System.out.println(e.getMessage());
            }

        }

        private String getAccessToken() {
            RetryPolicy retryPolicy = new RetryPolicy()
                .retryOn(ConnectException.class)
                .withDelay(1, TimeUnit.SECONDS)
                .withMaxRetries(task.getMaximumRetries());

            WebTarget target = client.target("https://" + task.getAccountId() + ".mktorest.com")
                                .path("/identity/oauth/token") 
                                .queryParam("grant_type", "client_credentials")
                                .queryParam("client_id", task.getClientId())
                                .queryParam("client_secret", task.getClientSecret());

            String accessToken = "";

            try {
                logger.info("Post to " + target.getUri().toString().replace(task.getClientId(), "XXXXX").replace(task.getClientSecret(), "XXXXX"));

                Execution execution = new Execution(retryPolicy);
                while (!execution.isComplete()) {
                    try {
                        Response response = Failsafe.with(retryPolicy).get(() -> target.request().get(Response.class));
                        String result = response.readEntity(String.class);
                            
                        ObjectMapper objectMapper = new ObjectMapper();
                        Map<String,Object> map = objectMapper.readValue(result,  new TypeReference<Map<String, Object>>() {});
                                            
                        accessToken = map.get("access_token").toString();
                        execution.complete();
                        logger.info("Status code: " + Integer.toString(response.getStatus()) + " " + response.getStatusInfo().getReasonPhrase());

                    } catch (ConnectException e) {
                        execution.recordFailure(e);
                    }
                }

            } catch (BadRequestException e) {
                logger.error(e.getMessage());
            } catch (IOException e) {
                logger.error(e.getMessage());
            }

            return accessToken;
        }
    }

    private final Logger logger = Exec.getLogger(MarketoLeadbulkOutputPlugin.class);
}
