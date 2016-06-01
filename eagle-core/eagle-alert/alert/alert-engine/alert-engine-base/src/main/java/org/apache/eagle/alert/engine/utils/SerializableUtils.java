package org.apache.eagle.alert.engine.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

/**
 * Utilities for working with Serializables.
 *
 * Derived from "com.google.cloud.dataflow.sdk.util.SerializableUtils":
 * https://github.com/apache/incubator-beam/blob/master/sdks/java/core/src/main/java/com/google/cloud/dataflow/sdk/util/SerializableUtils.java
 */
public class SerializableUtils {
  /**
   * Serializes the argument into an array of bytes, and returns it.
   *
   * @throws IllegalArgumentException if there are errors when serializing
   */
  public static byte[] serializeToCompressedByteArray(Object value) {
    try {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(new SnappyOutputStream(buffer))) {
        oos.writeObject(value);
      }
      return buffer.toByteArray();
    } catch (IOException exn) {
      throw new IllegalArgumentException(
          "unable to serialize " + value,
          exn);
    }
  }

  /**
   * Serializes the argument into an array of bytes, and returns it.
   *
   * @throws IllegalArgumentException if there are errors when serializing
   */
  public static byte[] serializeToByteArray(Object value) {
    try {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(buffer)) {
        oos.writeObject(value);
      }
      return buffer.toByteArray();
    } catch (IOException exn) {
      throw new IllegalArgumentException("unable to serialize " + value, exn);
    }
  }

  /**
   * Deserializes an object from the given array of bytes, e.g., as
   * serialized using {@link #serializeToCompressedByteArray}, and returns it.
   *
   * @throws IllegalArgumentException if there are errors when
   * deserializing, using the provided description to identify what
   * was being deserialized
   */
  public static Object deserializeFromByteArray(byte[] encodedValue,
                                                          String description) {
    try {
      try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(encodedValue))) {
        return ois.readObject();
      }
    } catch (IOException | ClassNotFoundException exn) {
      throw new IllegalArgumentException(
          "unable to deserialize " + description,
          exn);
    }
  }

  /**
   * Deserializes an object from the given array of bytes, e.g., as
   * serialized using {@link #serializeToCompressedByteArray}, and returns it.
   *
   * @throws IllegalArgumentException if there are errors when
   * deserializing, using the provided description to identify what
   * was being deserialized
   */
  public static Object deserializeFromCompressedByteArray(byte[] encodedValue,
                                                          String description) {
    try {
      try (ObjectInputStream ois = new ObjectInputStream(
          new SnappyInputStream(new ByteArrayInputStream(encodedValue)))) {
        return ois.readObject();
      }
    } catch (IOException | ClassNotFoundException exn) {
      throw new IllegalArgumentException(
          "unable to deserialize " + description,
          exn);
    }
  }

  public static <T extends Serializable> T ensureSerializable(T value) {
    @SuppressWarnings("unchecked")
    T copy = (T) deserializeFromCompressedByteArray(serializeToCompressedByteArray(value),
        value.toString());
    return copy;
  }

  public static <T extends Serializable> T clone(T value) {
    @SuppressWarnings("unchecked")
    T copy = (T) deserializeFromCompressedByteArray(serializeToCompressedByteArray(value),
        value.toString());
    return copy;
  }
}