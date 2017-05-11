package org.kunicki.akka_streams_java8;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.vavr.collection.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public class RandomDataGenerator {

  private static final Logger logger = LoggerFactory.getLogger(RandomDataGenerator.class);

  private ActorSystem system = ActorSystem.create("random-data-generator");
  private ActorMaterializer materializer = ActorMaterializer.create(system);

  private Config config = ConfigFactory.load();
  private int numberOfFiles = config.getInt("generator.number-of-files");
  private int numberOfPairs = config.getInt("generator.number-of-pairs");
  private double invalidLineProbability = config.getDouble("generator.invalid-line-probability");

  private Random random = new Random();

  private String randomValue() {
    if (random.nextDouble() > invalidLineProbability) {
      return String.valueOf(random.nextDouble());
    } else {
      return "invalid_value";
    }
  }

  private CompletionStage<Void> generate() {
    logger.info("Starting generation");
    return Source.range(1, numberOfFiles)
        .mapAsyncUnordered(numberOfFiles, n -> {
          String fileName = UUID.randomUUID().toString();
          return Source.range(1, numberOfPairs).map(i -> {
            int id = random.nextInt(1_000_000);
            return Stream.of(randomValue(), randomValue())
                .map(v -> ByteString.fromString(id + ";" + v + "\n"))
                .foldLeft(ByteString.empty(), ByteString::concat);
          })
          .runWith(FileIO.toPath(Paths.get("data", fileName + ".csv")), materializer);
        })
        .runWith(Sink.ignore(), materializer)
        .thenAccept(d -> {
          logger.info("Generated random data");
          system.terminate();
        });
  }

  public static void main(String[] args) {
    new RandomDataGenerator().generate();
  }
}
