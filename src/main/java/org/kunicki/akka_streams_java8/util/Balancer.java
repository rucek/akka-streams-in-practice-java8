package org.kunicki.akka_streams_java8.util;

import akka.NotUsed;
import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Balance;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Merge;

import java.util.stream.IntStream;

public class Balancer {

  public static <In, Out> Graph<FlowShape<In, Out>, NotUsed> create(int numberOfWorkers, Flow<In, Out, NotUsed> worker) {
    return GraphDSL.create(builder -> {
      UniformFanOutShape<In, In> balance = builder.add(Balance.<In>create(numberOfWorkers));
      UniformFanInShape<Out, Out> merge = builder.add(Merge.<Out>create(numberOfWorkers));

      IntStream.range(0, numberOfWorkers).forEach(i -> {
        FlowShape<In, Out> workerStage = builder.add(worker);
        builder.from(balance.out(i)).via(workerStage).toInlet(merge.in(i));
      });

      return FlowShape.of(balance.in(), merge.out());
    });
  }
}
