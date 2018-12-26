package org.apache.druid.firehose.pubsub

case class PubsubFirehoseConfig(
                                 project: String, //@todo find a way to use it
                                 subscription: String,
                                 batchNumber: Int
                               )
