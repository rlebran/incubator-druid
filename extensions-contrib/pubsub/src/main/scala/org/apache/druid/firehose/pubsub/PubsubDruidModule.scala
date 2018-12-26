package org.apache.druid.firehose.pubsub

import java.util

import com.fasterxml.jackson.databind.Module
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.databind.module.SimpleModule
import com.google.inject.Binder
import org.apache.druid.initialization.DruidModule

class PubsubDruidModule extends DruidModule {
  override def getJacksonModules: util.List[_ <: Module] = {
    import scala.collection.JavaConverters._

    List(
      new SimpleModule("PubsubFirehoseModule")
        .registerSubtypes(
          new NamedType(classOf[PubsubFirehoseFactory], "pubsub")
        )
    ).asJava
  }

  override def configure(binder: Binder): Unit = ()
}
