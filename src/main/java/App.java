
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import static io.vertx.core.Vertx.vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;
import io.vertx.ext.mongo.MongoService;
import io.vertx.ext.mongo.MongoServiceVerticle;
import java.util.concurrent.CountDownLatch;

public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static final String ADDRESS = "mongodb-persistor";
    public static final String DEFAULT_MONGODB_CONFIG
            = "{"
            + "    \"address\": \"" + ADDRESS + "\","
            + "    \"host\": \"localhost\","
            + "    \"port\": 27017,"
            + "    \"db_name\": \"bs\","
            + "    \"useObjectId\" : true"
            + "}";

    public static void main(String[] args) throws InterruptedException {
        // approach 1
        startMongoServiceAndCount();
        
        // approach 2
//        startMongoServiceVerticle();
//        startTestMongoVerticle();
    }

    
    private static void startMongoServiceAndCount() throws InterruptedException {
        JsonObject jsonMongoDBOptions = new JsonObject(DEFAULT_MONGODB_CONFIG);
        logger.info("Config: connect to mongodb:" + jsonMongoDBOptions.encodePrettily());
        MongoService service = MongoService.create(vertx(), jsonMongoDBOptions);
        service.start();
        
        CountDownLatch latch = new CountDownLatch(1);
        service.count("credentials", new JsonObject(), result -> {
            if(result.succeeded()) {
                logger.info("ok: entry count = " + result.result());
            } else {
                logger.info("error", result.cause());
            }
            latch.countDown();
        });
        latch.await();
    }
    
    
    
    private static void startMongoServiceVerticle() throws InterruptedException {
        JsonObject jsonMongoDBOptions = new JsonObject(DEFAULT_MONGODB_CONFIG);
        DeploymentOptions mongoDBOptions = new DeploymentOptions();
        mongoDBOptions.setConfig(jsonMongoDBOptions);

        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Config: connect to mongodb:" + jsonMongoDBOptions.encodePrettily());
        vertx().deployVerticle(new MongoServiceVerticle(), mongoDBOptions, res -> {
            if (res.succeeded()) {
                logger.info("Connected to mongodb.");
            } else {
                logger.fatal("Could not connect to mongodb", res.cause());
            }
            latch.countDown();
        });
        latch.await();
    }

    private static void startTestMongoVerticle() {
        vertx().deployVerticle(new TestMongoVerticle(), res -> {
            if (res.succeeded()) {
                logger.info("TestMongoVerticle verticle started.");
            } else {
                logger.error("Could not start TestMongoVerticle verticle.", res.cause());
            }
        });
    }

    public static class TestMongoVerticle extends AbstractVerticle {

        private static final Logger logger = LoggerFactory.getLogger(TestMongoVerticle.class);
        private MongoService mongoService;

        @Override
        public void start() throws Exception {
            logger.info("TestMongoVerticle.start: " + Thread.currentThread().getName());
            super.start();

            mongoService = MongoService.createEventBusProxy(getVertx(), ADDRESS);
            // use the mongodb to execute a query
            mongoService.count("credentials", new JsonObject(), (AsyncResult<Long> result) -> {
                if (result.succeeded()) {
                    logger.info("ok: entry count = " + result.result());
                } else {
                    logger.info("error", result.cause());
                }
            });
        }
    }

}
