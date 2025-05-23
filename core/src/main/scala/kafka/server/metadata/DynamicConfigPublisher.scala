/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server.metadata

import java.util.Properties
import kafka.server.ConfigAdminManager.toLoggableProps
import kafka.server.{ConfigHandler, KafkaConfig}
import kafka.utils.Logging
import org.apache.kafka.common.config.ConfigResource.Type.{BROKER, CLIENT_METRICS, GROUP, TOPIC}
import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.server.config.ConfigType
import org.apache.kafka.server.fault.FaultHandler


class DynamicConfigPublisher(
  conf: KafkaConfig,
  faultHandler: FaultHandler,
  dynamicConfigHandlers: Map[ConfigType, ConfigHandler],
  nodeType: String,
) extends Logging with org.apache.kafka.image.publisher.MetadataPublisher {
  logIdent = s"[${name()}] "

  override def name(): String = s"DynamicConfigPublisher $nodeType id=${conf.nodeId}"

  override def onMetadataUpdate(
    delta: MetadataDelta,
    newImage: MetadataImage,
    manifest: LoaderManifest
  ): Unit = {
    onMetadataUpdate(delta, newImage)
  }

  def onMetadataUpdate(
    delta: MetadataDelta,
    newImage: MetadataImage,
  ): Unit = {
    val deltaName = s"MetadataDelta up to ${newImage.highestOffsetAndEpoch().offset}"
    try {
      // Apply configuration deltas.
      Option(delta.configsDelta()).foreach { configsDelta =>
        configsDelta.changes().keySet().forEach { resource =>
          val props = newImage.configs().configProperties(resource)
          resource.`type`() match {
            case TOPIC =>
              dynamicConfigHandlers.get(ConfigType.TOPIC).foreach(topicConfigHandler =>
                try {
                  // Apply changes to a topic's dynamic configuration.
                  info(s"Updating topic ${resource.name()} with new configuration : " +
                    toLoggableProps(resource, props).mkString(","))
                  topicConfigHandler.processConfigChanges(resource.name(), props)
                } catch {
                  case t: Throwable => faultHandler.handleFault("Error updating topic " +
                    s"${resource.name()} with new configuration: ${toLoggableProps(resource, props).mkString(",")} " +
                    s"in $deltaName", t)
                }
              )
            case BROKER =>
              dynamicConfigHandlers.get(ConfigType.BROKER).foreach(nodeConfigHandler =>
                if (resource.name().isEmpty) {
                  try {
                    // Apply changes to "cluster configs" (also known as default BROKER configs).
                    // These are stored in KRaft with an empty name field.
                    info("Updating cluster configuration : " +
                      toLoggableProps(resource, props).mkString(","))
                    nodeConfigHandler.processConfigChanges(resource.name(), props)
                  } catch {
                    case t: Throwable => faultHandler.handleFault("Error updating " +
                      s"cluster with new configuration: ${toLoggableProps(resource, props).mkString(",")} " +
                      s"in $deltaName", t)
                  }
                } else if (resource.name() == conf.nodeId.toString) {
                  try {
                    // Apply changes to this node's dynamic configuration.
                    info(s"Updating node ${conf.nodeId} with new configuration : " +
                      toLoggableProps(resource, props).mkString(","))
                    nodeConfigHandler.processConfigChanges(resource.name(), props)
                    // When applying a per node config (not a cluster config), we also
                    // reload any associated file. For example, if the ssl.keystore is still
                    // set to /tmp/foo, we still want to reload /tmp/foo in case its contents
                    // have changed. This doesn't apply to topic configs or cluster configs.
                    reloadUpdatedFilesWithoutConfigChange(props)
                  } catch {
                    case t: Throwable => faultHandler.handleFault("Error updating " +
                      s"node with new configuration: ${toLoggableProps(resource, props).mkString(",")} " +
                      s"in $deltaName", t)
                  }
                }
              )
            case CLIENT_METRICS =>
              // Apply changes to client metrics subscription.
              dynamicConfigHandlers.get(ConfigType.CLIENT_METRICS).foreach(metricsConfigHandler =>
                try {
                  info(s"Updating client metrics ${resource.name()} with new configuration : " +
                    toLoggableProps(resource, props).mkString(","))
                  metricsConfigHandler.processConfigChanges(resource.name(), props)
                } catch {
                  case t: Throwable => faultHandler.handleFault("Error updating client metrics" +
                    s"${resource.name()} with new configuration: ${toLoggableProps(resource, props).mkString(",")} " +
                    s"in $deltaName", t)
                })
            case GROUP =>
              // Apply changes to a group's dynamic configuration.
              dynamicConfigHandlers.get(ConfigType.GROUP).foreach(groupConfigHandler =>
                try {
                  info(s"Updating group ${resource.name()} with new configuration : " +
                    toLoggableProps(resource, props).mkString(","))
                  groupConfigHandler.processConfigChanges(resource.name(), props)
                } catch {
                  case t: Throwable => faultHandler.handleFault("Error updating group " +
                    s"${resource.name()} with new configuration: ${toLoggableProps(resource, props).mkString(",")} " +
                    s"in $deltaName", t)
                })
            case _ => // nothing to do
          }
        }
      }
    } catch {
      case t: Throwable => faultHandler.handleFault("Uncaught exception while " +
        s"publishing dynamic configuration changes from $deltaName", t)
    }
  }

  def reloadUpdatedFilesWithoutConfigChange(props: Properties): Unit = {
    conf.dynamicConfig.reloadUpdatedFilesWithoutConfigChange(props)
  }
}
