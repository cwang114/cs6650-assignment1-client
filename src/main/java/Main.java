import com.opencsv.CSVWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

public class Main {
  public static final Logger logger = LogManager.getLogger(Main.class.getName());


  public static void main(String args[]) throws IOException {
    UserInputUtil.askForParams();
    BlockingQueue<SingleThreadMeasure> queue = new LinkedBlockingQueue<>();
    // test phases begin
    long startTime = System.nanoTime();

    initiateTest(1, Constant.MAXIMUM_THREADS/4, 1, 90,
            Constant.START_UP_RUN_FACTOR, queue, new CountDownLatch(Constant.START_UP_CRITERIA));

//    // phase 2
//    countDownForNextPhaseLatch = new CountDownLatch((int) Math.ceil(Constant.MAXIMUM_THREADS * 0.1));
//    logger.info("Test phase 2 initiated");
//
//    TestPhase testPhase2 = new TestPhase(2, Constant.MAXIMUM_THREADS, 91, 360, factor, queue, countDownForNextPhaseLatch);
//    testPhase2.execute();
//
//    //phase 3
//
//    logger.info("Test phase 3 initiated");
//    factor = (int) (Constant.NUM_OF_SKI_LIFTS_FOR_EACH_SKIER_EACH_DAY * 0.1);
//    TestPhase testPhase3 = new TestPhase(3, Constant.MAXIMUM_THREADS / 4, 361, 420, factor, queue, countDownForNextPhaseLatch);
//    testPhase3.execute();


//    thread starts, local structure: latencies and sucess
//    use a block queue to write to file when all the threads want to visit the file.
    long endTime = System.nanoTime();
    long totalWallTime = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
    logger.info("In the Main class, all tasks finished.");
    logger.info("Duration of three phases is " + totalWallTime + "ms.");
    int success = SharedMeasure.numOfSuccessfulRequests.get();
    int unsuccess = SharedMeasure.numOfUnsuccessfulRequests.get();
    logger.info("The total number of successful requests is " + success);
    logger.info("The total number of unsuccessful requests is " + unsuccess);
    int throughput = (int)((double) success / (double) totalWallTime * 1000);
    logger.info("The throughput is " + throughput + " request/s.");

    // release the blocking queue
    List<Long> finalList = new ArrayList<>();
    writeMeasuresIntoFile(queue, finalList);
    Collections.sort(finalList);
    LongSummaryStatistics stats = finalList.stream()
            .mapToLong((x) -> x)
            .summaryStatistics();
    long maxTime = stats.getMax();
    long meanTime = stats.getSum() / stats.getCount();
    long medianTime = finalList.get(finalList.size()/2);
    long percentile99 = finalList.get((int) (finalList.size() * 0.99));
    logger.info("The max time is " + maxTime + "ms.");
    logger.info("The mean time is " + meanTime + "ms.");
    logger.info("The median time is " + medianTime + "ms.");
    logger.info("The 99% response time is " + percentile99 + "ms.");


  }

  private static void initiateTest(int id, int numThreads, int startT, int endT, int factor,
                                   BlockingQueue<SingleThreadMeasure> queue,
                                   CountDownLatch countDownLatch) {
    logger.info("Test phase " + id + " initiated.");

    int numOfSkiersInOneThread = Constant.NUM_OF_SKIERS / numThreads;     // 133/25 = 5
    int remainingNumOfSkiers = Constant.NUM_OF_SKIERS % numThreads;       // 133%25 = 8
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    CyclicBarrier endCyclicBarrierForAllThreads = new CyclicBarrier(numThreads+1);

    int extra = 0;
    for (int i = 0; i < numThreads; i++) {
      int start = numOfSkiersInOneThread * i + 1 + extra; // 13
      int end = numOfSkiersInOneThread * (i + 1) + extra; // 17
      if (i < remainingNumOfSkiers) {
        end++; // 18
        extra++;  //3
      }
      executorService.execute(new ClientThread(id, start, end, startT, endT, factor, endCyclicBarrierForAllThreads, queue,
              countDownLatch));
    }
    try {
      countDownLatch.await();
      logger.info("Count down at " + id + " goes zero.");
      // when latch goes to zero
      if (id == 1) {
        initiateTest(2, Constant.MAXIMUM_THREADS, 91, 360,
                Constant.PEAK_RUN_FACTOR, queue, new CountDownLatch(Constant.PEAK_CRITERIA));
      } else if (id == 2) {
        initiateTest(3, Constant.MAXIMUM_THREADS / 4, 361, 420,
                Constant.COOLDOWN_RUN_FACTOR, queue, new CountDownLatch(Constant.COOLDOWN_CRITERIA));

      }

    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    logger.info("Phase " + id + " has ended.");
    try {
      // when all threads from all executors come back
      endCyclicBarrierForAllThreads.await();
    } catch (InterruptedException | BrokenBarrierException e) {
      e.printStackTrace();
    }
    executorService.shutdown();
  }

  private static void writeMeasuresIntoFile(BlockingQueue<SingleThreadMeasure> queue,
                                            List<Long> finalList) throws IOException {

    try (
      Writer writer = Files.newBufferedWriter(Paths.get(Constant.CSV_FILE_PATH));

      CSVWriter csvWriter = new CSVWriter(writer,
              CSVWriter.DEFAULT_SEPARATOR,
              CSVWriter.NO_QUOTE_CHARACTER,
              CSVWriter.DEFAULT_ESCAPE_CHARACTER,
              CSVWriter.DEFAULT_LINE_END);
    ) {
      while (!queue.isEmpty()) {
        try {
          SingleThreadMeasure measure = queue.take();
          finalList.add(measure.getLatency());
          csvWriter.writeNext(new String[]{Long.toString(measure.getStartTime()),
                  measure.getRequestType(),
                  Long.toString(measure.getLatency()),
                  Integer.toString(measure.getResponseCode())});
        } catch (InterruptedException e) {
          logger.error("Interrupted from taking from blocked queue.");
          e.printStackTrace();
        }
      }
    }
  }
}


