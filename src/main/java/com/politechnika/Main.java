package com.politechnika;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;


public class Main extends AbstractVerticle {
    private List<MutableInt> sentCount = new ArrayList<>();
    private long startTime;

    @Override
    public void start(Future<Void> future) {
        WebClientOptions options = new WebClientOptions().setUserAgent("LoadTest/1.0");
        options.setKeepAlive(false);
        WebClient client = WebClient.create(vertx, options);

        File[] csvFilesInDirectory = getCsvFilesInDirectory(config().getString("directoryPath", "csvData/"));

        startTime = System.currentTimeMillis();
        readAndPostFiles(csvFilesInDirectory, client);

        vertx.setPeriodic(1000, id -> estimatedEfficiency());
    }

    private File[] getCsvFilesInDirectory(String directoryPath) {
        File directory = new File(directoryPath);
        return directory.listFiles((dir, name) -> name.endsWith(".csv"));
    }

    private void readAndPostFiles(File[] csvFiles, WebClient client) {
        OpenOptions openOptions = getFileOpenOptions();

        for (File csvFile : csvFiles) {
            vertx.fileSystem()
                 .open(csvFile.getPath(), openOptions, measurementFileAsyncHandler(client));
        }
    }

    private OpenOptions getFileOpenOptions() {
        return new OpenOptions().setRead(true)
                                .setWrite(false)
                                .setCreate(false);
    }

    private Handler<AsyncResult<AsyncFile>> measurementFileAsyncHandler(WebClient client) {
        return handler -> {
            if (handler.succeeded()) {
                MutableInt count = new MutableInt();
                sentCount.add(count);

                AsyncFile asyncFile = handler.result();

                RecordParser recordParser = getCsvRecordParser(client, count);
                asyncFile.handler(recordParser)
                         .endHandler(v -> asyncFile.close());
            }
        };
    }

    private HttpRequest<Buffer> getPostRequest(WebClient client) {
        return client.post(config().getInteger("port", 8080),
                           config().getString("hostname", "localhost"),
                           config().getString("uri", "/measurement"));
    }

    private RecordParser getCsvRecordParser(WebClient client, MutableInt count) {
        HttpRequest<Buffer> postRequest = getPostRequest(client);

        return RecordParser.newDelimited("\n", bufferedLine -> {
            JsonObject jsonMeasurement = mapToJsonObject(new String(bufferedLine.getBytes(),
                                                                    StandardCharsets.UTF_8));
            postRequest.sendJsonObject(jsonMeasurement, request -> {
                if (request.succeeded()) {
                    count.value++;
                }
            });
        });
    }

    private JsonObject mapToJsonObject(String csvDataLine) {
        String[] splitData = csvDataLine.split(",");

        return new JsonObject().put("longitude", Double.valueOf(splitData[0]))
                               .put("latitude", Double.valueOf(splitData[1]))
                               .put("time", splitData[2])
                               .put("altitude", Double.valueOf(splitData[3]))
                               .put("pressure", Double.valueOf(splitData[4]))
                               .put("co2", Double.valueOf(splitData[5]))
                               .put("airDensity", Double.valueOf(splitData[6]))
                               .put("surfaceTemperature", Float.valueOf(splitData[7]));
    }

    private void estimatedEfficiency() {
        int count = sentCount.stream().mapToInt(mutableInt -> mutableInt.value).sum();
        double elapsedTimeInSec = (System.currentTimeMillis() - startTime) / 1000.d;

        System.out.println("Sent count: " + count);
        System.out.println("Estimated time in sec: " + elapsedTimeInSec);
        System.out.println("Estimated speed json / sec: " + count / elapsedTimeInSec);
    }
}
