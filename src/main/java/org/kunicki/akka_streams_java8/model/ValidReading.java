package org.kunicki.akka_streams_java8.model;

public class ValidReading implements Reading {

  private final int id;

  private final double value;

  public ValidReading(int id, double value) {
    this.id = id;
    this.value = value;
  }

  @Override
  public int getId() {
    return id;
  }

  public double getValue() {
    return value;
  }
}
