import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Scanner;

public class UserInputUtil {
  public static final Logger logger = LogManager.getLogger(UserInputUtil.class);

  public static void askForParams() {
    Scanner in = new Scanner(System.in);
    String line;
    System.out.println("What is the maximum number of thread? (default 100, max 256)");
    line = in.nextLine();
    if (notEmptyLine(line)) {
      Constant.MAXIMUM_THREADS = Integer.parseInt(line);
    }

    System.out.println("What is the number of skiers? (default 20000, max 50000)");
    line = in.nextLine();
    if (notEmptyLine(line)) {
      Constant.NUM_OF_SKIERS = Integer.parseInt(line);
    }

    System.out.println("What is the number of ski lifts? (range 5-60, default 40)");
    line = in.nextLine();
    if (notEmptyLine(line)) {
      Constant.NUM_OF_SKI_LIFTS = Integer.parseInt(line);
    }

    System.out.println("What is the mean numbers of ski lifts each skier rides each day? (numRuns - default 20, max 20)");
    line = in.nextLine();
    if (notEmptyLine(line)) {
      Constant.NUM_OF_SKI_LIFTS_FOR_EACH_SKIER_EACH_DAY = Integer.parseInt(line);
    }

    System.out.println("What is the port number? (default 8080)");
    line = in.nextLine();
    if (notEmptyLine(line)) {
      Constant.PORT = Integer.parseInt(line);
    }

    logger.debug("Maximum threads: " + Constant.MAXIMUM_THREADS);
    logger.debug("Number of skiers: " + Constant.NUM_OF_SKIERS);
    logger.debug("Number of ski lifts: " + Constant.NUM_OF_SKI_LIFTS);
    logger.debug("Number of ski lift for each skier: "+Constant.NUM_OF_SKI_LIFTS_FOR_EACH_SKIER_EACH_DAY);
    logger.debug("Port number: " + Constant.PORT);
    Constant.START_UP_RUN_FACTOR = (int) (Constant.NUM_OF_SKI_LIFTS_FOR_EACH_SKIER_EACH_DAY * 0.1);
    Constant.PEAK_RUN_FACTOR = (int) (Constant.NUM_OF_SKI_LIFTS_FOR_EACH_SKIER_EACH_DAY * 0.8);
    Constant.COOLDOWN_RUN_FACTOR = (int) (Constant.NUM_OF_SKI_LIFTS_FOR_EACH_SKIER_EACH_DAY * 0.1);
    Constant.START_UP_CRITERIA = (int) Math.ceil(Constant.MAXIMUM_THREADS/4 * 0.1);
    Constant.PEAK_CRITERIA = (int) Math.ceil(Constant.MAXIMUM_THREADS * 0.1);
    Constant.COOLDOWN_CRITERIA = (int) Math.ceil(Constant.MAXIMUM_THREADS/4 * 0.1);

  }

  private static boolean notEmptyLine(String line) {
    return !line.equals("");
  }
}
