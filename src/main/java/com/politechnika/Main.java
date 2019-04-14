package com.politechnika;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;


public class Main extends AbstractVerticle {

    @Override
    public void start(Future<Void> future) {
        WebClientOptions options = new WebClientOptions().setUserAgent("LoadTest/1.0");
        options.setKeepAlive(false);
        WebClient client = WebClient.create(vertx, options);

        client.post(config().getInteger("cassandra.port", 9042),
                    config().getString("hostname", "localhost"),
                    config().getString("uri", "/measurement"))
              .sendJsonObject(createJsonObject(), request -> {
                  if (request.succeeded()) {

                  } else {

                  }
              });
    }

    private JsonObject createJsonObject() {
        return new JsonObject().put("longitude", -91.053794860839844)
                               .put("latitude", -3.8782415390014648)
                               .put("time", "20040822194824")
                               .put("altitude", 1012.7100219726562)
                               .put("pressure", 1012.71)
                               .put("co2", 0.000365346)
                               .put("airDensity", 2.45201e+25)
                               .put("surfaceTemperature", 293.139);
    }
}
