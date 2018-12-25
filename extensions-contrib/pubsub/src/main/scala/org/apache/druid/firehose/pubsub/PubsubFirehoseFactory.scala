package org.apache.druid.firehose.pubsub

import java.io.File

import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}
import com.google.pubsub.v1._
import com.typesafe.scalalogging.LazyLogging
import org.apache.druid.data.input.impl.InputRowParser
import org.apache.druid.data.input.{Firehose, FirehoseFactory, InputRow}

import scala.collection.mutable

//@todo pass a pubsub config object instead of sub + batch size ?
class PubsubFirehoseFactory(
                             subscription: String,
                             batchSize: Int
                           ) extends FirehoseFactory[InputRowParser[PubsubMessage]]
  with LazyLogging {

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
      .setMaxMessages(batchSize)
      .setReturnImmediately(true)
      .setSubscription(subscription)
      .build

    new Firehose {
      //@todo switch from stack to list (assigned to var ?)
      import scala.collection.JavaConverters._

      private val pubsubResponse = mutable.Stack[PullResponse]()

      /**
        * Pull messages from pubsub if and only if the lccal pubsub response buffer is empty
        */
      override def hasMore: Boolean = {
        if (pubsubResponse.nonEmpty) true
        else pullMessage.nonEmpty
      }

      override def nextRow(): InputRow = {
        logger.debug("Getting next pubsub message")
        receivedMessageFromPullResponse(pubsubResponse.top)
          .map(_.getMessage)
          .flatMap(parser.parseBatch(_).asScala)
          .headOption
          .orNull
      }

      override def commit(): Runnable = {
        () => {
          val ackId = receivedMessageFromPullResponse(pubsubResponse.pop)
            .map(_.getAckId)
            .toIterable
            .asJava
          val ackRequest = AcknowledgeRequest
            .newBuilder
            .setSubscription(subscription)
            .addAllAckIds(ackId)
            .build

          logger.debug(s"Sending ack request $ackRequest for id $ackId")
          pubsubSubscriber.acknowledgeCallable()
            .call(ackRequest)
        }
      }

      override def close(): Unit = {
        logger.info("Closing connection to pubsub")
        pubsubSubscriber.close()
      }

      private def pullMessage = {
        logger.debug(s"Pulling $batchSize message(s) from pubsub sub: $subscription")
        val response = pubsubSubscriber.pullCallable
          .call(pullRequest)
        pubsubResponse.push(response)
      }

      private def receivedMessageFromPullResponse(pullResponse: PullResponse): Option[ReceivedMessage] = {
        pullResponse
          .getReceivedMessagesList
          .asScala
          .headOption
      }
    }
  }

}
