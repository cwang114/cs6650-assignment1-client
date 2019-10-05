import java.util.concurrent.atomic.AtomicInteger;

public class SharedMeasure {
  static AtomicInteger numOfSuccessfulRequests = new AtomicInteger();
  static AtomicInteger numOfUnsuccessfulRequests = new AtomicInteger();

}
