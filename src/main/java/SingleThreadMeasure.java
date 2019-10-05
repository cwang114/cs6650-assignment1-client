public class SingleThreadMeasure {
  long startTime;
  String requestType;
  long latency;
  int responseCode;

  public SingleThreadMeasure(long startTime, String requestType, long latency, int responseCode) {
    this.startTime = startTime;
    this.requestType = requestType;
    this.latency = latency;
    this.responseCode = responseCode;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public String getRequestType() {
    return requestType;
  }

  public void setRequestType(String requestType) {
    this.requestType = requestType;
  }

  public long getLatency() {
    return latency;
  }

  public void setLatency(long latency) {
    this.latency = latency;
  }

  public int getResponseCode() {
    return responseCode;
  }

  public void setResponseCode(int responseCode) {
    this.responseCode = responseCode;
  }

  @Override
  public String toString() {
    return "SingleThreadMeasure{" +
            "startTime=" + startTime +
            ", requestType='" + requestType + '\'' +
            ", latency=" + latency +
            ", responseCode=" + responseCode +
            '}';
  }
}
