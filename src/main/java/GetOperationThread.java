import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.HttpRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;

public class GetOperationThread implements Runnable {

  public static final Logger logger = LogManager.getLogger(ClientThread.class.getName());

  private String targetUrl;
  private BlockingQueue<SingleThreadMeasure> queue;

  public GetOperationThread(String targetUrl, BlockingQueue<SingleThreadMeasure> queue) {
    this.targetUrl = targetUrl;
    this.queue = queue;
  }

  @Override
  public void run() {
    long startTimestampGet = System.currentTimeMillis();
    int codeGet = executeGetConnection(targetUrl);
    long endTimestampGet = System.currentTimeMillis();
    long latencyInMillisGet = endTimestampGet - startTimestampGet;
    putInBlockQueue(startTimestampGet, latencyInMillisGet, codeGet, "GET");
  }

  private int executeGetConnection(String targetUrl) {
    HttpRequest httpRequest = Unirest.get(targetUrl)
            .header("Content-Type", "application/json;charset=UTF-8");
    try {
      HttpResponse httpResponse = httpRequest.asString();
      int code = httpResponse.getStatus();
      if (code == 200) {
        SharedMeasure.numOfSuccessfulRequests.incrementAndGet();
      } else {
        SharedMeasure.numOfUnsuccessfulRequests.incrementAndGet();
        if (code == 404) {
          logger.info("Resource not found.");
        } else if (code == 500) {
          logger.error("Internal server error.");
        }
      }
      return code;
    } catch (UnirestException e) {
      e.printStackTrace();
    }
    return 500;

  }

  private void putInBlockQueue(long startTimestamp, long latencyInMillis, int code, String requestType) {
    SingleThreadMeasure threadMeasure = new SingleThreadMeasure(startTimestamp, requestType, latencyInMillis, code);
    // add to blocking queue
    try {
      queue.put(threadMeasure);
    } catch (InterruptedException e) {
      logger.info("Blocking queue interrupted.");
      e.printStackTrace();
    }
  }
}
