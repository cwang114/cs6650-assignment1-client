import com.google.gson.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.NoRouteToHostException;
import java.net.URL;
import java.util.concurrent.*;

public class ClientThread extends Thread {
  private int phaseId;
  private int startSkierId;
  private int endSkierId;
  private int startTime;
  private int endTime;
  private int factor;
  private CyclicBarrier cyclicBarrier;
  private BlockingQueue<SingleThreadMeasure> queue;
  private CountDownLatch countDownForNextPhaseLatch;
  public static final Logger logger = LogManager.getLogger(ClientThread.class.getName());

  public ClientThread(int phaseId, int startSkierId, int endSkierId, int startTime, int endTime, int factor,
                      CyclicBarrier cyclicBarrier, BlockingQueue<SingleThreadMeasure> queue,
                      CountDownLatch countDownForNextPhaseLatch) {
    this.phaseId = phaseId;
    this.startSkierId = startSkierId;
    this.endSkierId = endSkierId;
    this.startTime = startTime;
    this.endTime = endTime;
    this.factor = factor;
    this.cyclicBarrier = cyclicBarrier;
    this.countDownForNextPhaseLatch = countDownForNextPhaseLatch;
    this.queue = queue;

  }

  @Override
  public void run() {
    logger.debug("The thread from phase " + phaseId + " has started.");
    long clientID = Thread.currentThread().getId();
    for (int f = 0; f < factor; f++) {
      for (int i = 0; i <= endSkierId - startSkierId; i++) {
        String targetUrl = buildTargetUrl();
        JsonObject body = buildBody();
        doPost(targetUrl, body);
      }
    }
    logger.debug("The thread from phase " + phaseId + " has ended.");
//    logger.debug("At each thread, count down is " + countDownForNextPhaseLatch.getCount());
    countDownForNextPhaseLatch.countDown();
    // after posting, let it wait
    try {
      cyclicBarrier.await();
    } catch (InterruptedException | BrokenBarrierException e) {
      logger.error("Cyclic barrier interrupted.");
      e.printStackTrace();
    }
  }

  private String buildTargetUrl() {
    int resortId = ThreadLocalRandom.current().nextInt(0, 11);
    int seasonId = ThreadLocalRandom.current().nextInt(2016, 2020);
    int dayId = ThreadLocalRandom.current().nextInt(1, 366);
    logger.debug("start Skier id is " + startSkierId + ", and endSkierId is " + endSkierId);
    int skierId = ThreadLocalRandom.current().nextInt(startSkierId, endSkierId + 1);

    String baseUrl = Constant.ENV == 0 ? Constant.LOCAL_BASE_URL : Constant.EC2_BASE_URL;
    String targetUrl = baseUrl + "/skiers/" + resortId +
            "/seasons/" + seasonId + "/days/" + dayId + "/skiers/" + skierId;
    logger.debug("Target URL is " + targetUrl);
    return targetUrl;
  }

  private JsonObject buildBody() {
    JsonObject body = new JsonObject();
    int time = ThreadLocalRandom.current().nextInt(startTime, endTime + 1);
    int liftId = ThreadLocalRandom.current().nextInt(0, Constant.NUM_OF_SKI_LIFTS);
    body.addProperty("time", time);
    body.addProperty("liftID", liftId);
    logger.debug("Body is " + body.toString());
    return body;
  }

  private void doPost(String targetUrl, JsonObject body) {
    logger.debug("Begin posting");
    long startTimestamp = System.currentTimeMillis();
    int code = executeUrlConnection(targetUrl, body);
    long endTimestamp = System.currentTimeMillis();
    long latencyInMillis = endTimestamp - startTimestamp;
    putInBlockQueue(startTimestamp, latencyInMillis, code);
  }

  private int executeUrlConnection(String targetURL, JsonObject body) {
    HttpURLConnection connection = null;
    int code = -1;
    try {
      //Create connection
      URL url = new URL(targetURL);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Accept", "application/json");
      connection.setRequestProperty("Content-Language", "en-US");
      connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
      connection.setUseCaches(false);
      connection.setDoOutput(true);

      //Send request
      OutputStream os = connection.getOutputStream();
      os.write(body.toString().getBytes("UTF-8"));
      os.close();

      //Get Response
      code = connection.getResponseCode();
      logger.debug("The response code is " + code);

      if (code == HttpURLConnection.HTTP_CREATED) {
        SharedMeasure.numOfSuccessfulRequests.incrementAndGet();
      } else {
        SharedMeasure.numOfUnsuccessfulRequests.incrementAndGet();
        if (code == HttpURLConnection.HTTP_NOT_FOUND) {
          logger.info("Resource not found.");
        } else if (code == HttpURLConnection.HTTP_INTERNAL_ERROR) {
          logger.error("Internal server error.");
        }
      }

    } catch (NoRouteToHostException e) {
      logger.error("No route to host.");
      code = HttpURLConnection.HTTP_BAD_REQUEST;
      SharedMeasure.numOfUnsuccessfulRequests.incrementAndGet();
    } catch (IOException e) {
      logger.error("IO Exception triggered.");
    } finally {
      if (connection != null) {
        connection.disconnect();
      }

    }
    return code;
  }

  private void putInBlockQueue(long startTimestamp, long latencyInMillis, int code) {
    SingleThreadMeasure threadMeasure = new SingleThreadMeasure(startTimestamp, "POST", latencyInMillis, code);
    // add to blocking queue
    try {
      queue.put(threadMeasure);
    } catch (InterruptedException e) {
      logger.info("Blocking queue interrupted.");
      e.printStackTrace();
    }
  }


}
