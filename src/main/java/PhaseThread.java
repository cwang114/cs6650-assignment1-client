import java.util.concurrent.*;

public class PhaseThread implements Runnable {
  private int id;
  private int numThreads;
  private int startT;
  private int endT;
  private int factor;
  private BlockingQueue<SingleThreadMeasure> queue;
  private CountDownLatch countDownLatch;
  public PhaseThread(int id, int numThreads, int startT, int endT, int factor,
                     BlockingQueue<SingleThreadMeasure> queue,
                     CountDownLatch countDownLatch) {
    this.id = id;
    this.numThreads = numThreads;
    this.startT = startT;
    this.endT = endT;
    this.factor = factor;
    this.queue = queue;
    this.countDownLatch = countDownLatch;

  }

  @Override
  public void run() {
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
      // when all threads from all executors come back
      endCyclicBarrierForAllThreads.await();
    } catch (InterruptedException | BrokenBarrierException e) {
      e.printStackTrace();
    }
    executorService.shutdown();
  }
}
