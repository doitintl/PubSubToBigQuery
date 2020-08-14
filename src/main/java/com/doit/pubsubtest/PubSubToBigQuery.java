package com.doit.pubsubtest;

import static org.apache.beam.sdk.coders.Coder.Context.*;

import com.google.api.services.bigquery.model.TableRow;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubToBigQuery {
  static final Logger logger = LoggerFactory.getLogger(PubSubToBigQuery.class);

  public interface Options extends PipelineOptions, StreamingOptions, GcpOptions {
    @Description("Table spec to write the output to")
    ValueProvider<String> getOutputTableSpec();

    void setOutputTableSpec(ValueProvider<String> value);

    @Description(
        "The Cloud Pub/Sub subscription to consume from. "
            + "The name should be in the format of "
            + "projects/<project-id>/subscriptions/<subscription-name>.")
    ValueProvider<String> getInputSubscription();

    void setInputSubscription(ValueProvider<String> value);
  }

  public static void main(String[] args) {
    final Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setStreaming(true);
    run(options);
  }

  public static PipelineResult run(final Options options) {
    final Pipeline pipeline = Pipeline.create(options);

    final PCollection<TableRow> messages =
        pipeline
            .apply(
                "ReadPubSubSubscription",
                PubsubIO.readMessagesWithAttributes()
                    .fromSubscription(options.getInputSubscription()))
            .apply("ConvertMessageToTableRow", ParDo.of(new PubsubMessageToString()));

    messages.apply(
        "WriteSuccessfulRecords",
        BigQueryIO.writeTableRows()
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withExtendedErrorInfo()
            .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
            .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
            .to(options.getOutputTableSpec()));

    return pipeline.run();
  }

  static class PubsubMessageToString extends DoFn<PubsubMessage, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      final PubsubMessage message = context.element();
      final String stringPayload = new String(message.getPayload(), StandardCharsets.UTF_8);
      final TableRow tableRow = convertJsonToTableRow(stringPayload);
      tableRow.set("event_timestamp", context.timestamp().toString());
      tableRow.set("dataflow_timestamp", Instant.now().toEpochMilli());
      logger.info("TableRow produced: {}", tableRow);
      context.output(tableRow);
    }
  }

  static TableRow convertJsonToTableRow(final String json) {
    try (final InputStream inputStream =
        new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
      return TableRowJsonCoder.of().decode(inputStream, OUTER);
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize json to table row: " + json, e);
    }
  }
}
