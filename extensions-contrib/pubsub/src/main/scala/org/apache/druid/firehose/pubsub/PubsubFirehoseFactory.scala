package org.apache.druid.firehose.pubsub

import java.io.File

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}
import com.google.pubsub.v1._
import com.typesafe.scalalogging.LazyLogging
import org.apache.druid.data.input.impl.InputRowParser
import org.apache.druid.data.input.{Firehose, FirehoseFactory, InputRow}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@JsonCreator
class PubsubFirehoseFactory(
                             @JsonProperty("config") pubsubConfig: PubsubFirehoseConfig
                           ) extends FirehoseFactory[InputRowParser[PubsubMessage]]
  with LazyLogging {

  def config(): PubsubFirehoseConfig = pubsubConfig

  override def connect(
                        parser: InputRowParser[PubsubMessage],
                        temporaryDirectory: File
                      ): Firehose = {
    val subscriberTransportSettings = SubscriberStubSettings
      .defaultGrpcTransportProviderBuilder
      .setMaxInboundMessageSize(20 << 20)
      .build
    val pubsubSubscriberSettings = SubscriberStubSettings
      .newBuilder
      .setTransportChannelProvider(subscriberTransportSettings)
      .build
    val pubsubSubscriber = GrpcSubscriberStub.create(pubsubSubscriberSettings)

    val pullRequest = PullRequest
      .newBuilder
      .setMaxMessages(pubsubConfig.batchNumber)
      .setReturnImmediately(true)
      .setSubscription(pubsubConfig.subscription)
      .build

    new Firehose {

      import scala.collection.JavaConverters._

      private val responsesBuffer: mutable.ArrayBuffer[ReceivedMessage] = ArrayBuffer[ReceivedMessage]()

      /**
        * Pull messages from pubsub if and only if the lccal pubsub response buffer is empty
        */
      override def hasMore: Boolean = {
        if (responsesBuffer.nonEmpty) true
        else pullMessage.nonEmpty
      }

      override def nextRow(): InputRow = {
        logger.debug("Getting next pubsub message")
        responsesBuffer
          .headOption
          .map(_.getMessage)
          .flatMap(parser.parseBatch(_).asScala)
          .headOption
          .orNull
      }

      /**
        * Send an ack request for all the messages previously pulled
        */
      override def commit(): Runnable = {
        () => {
          val ackId = responsesBuffer
            .map(_.getAckId)
          val ackRequest = AcknowledgeRequest
            .newBuilder
            .setSubscription(pubsubConfig.subscription)
            .addAllAckIds(ackId.asJava)
            .build

          // Clear the response buffer. All elements should have been process at this stage
          responsesBuffer.clear()

          logger.debug(s"Sending ack request $ackRequest for id $ackId")
          pubsubSubscriber.acknowledgeCallable()
            .call(ackRequest)
        }
      }

      override def close(): Unit = {
        logger.info("Closing connection to pubsub")
        pubsubSubscriber.close()
      }

      private def pullMessage: ArrayBuffer[ReceivedMessage] = {
        logger.debug(s"Pulling ${pubsubConfig.batchNumber} message(s) from pubsub sub: ${pubsubConfig.subscription}")
        val response = pubsubSubscriber
          .pullCallable
          .call(pullRequest)
        responsesBuffer ++= receivedMessageFromPullResponse(response)
        responsesBuffer
      }

      private def receivedMessageFromPullResponse(pullResponse: PullResponse): Seq[ReceivedMessage] = {
        pullResponse
          .getReceivedMessagesList
          .asScala
      }
    }
  }

}
