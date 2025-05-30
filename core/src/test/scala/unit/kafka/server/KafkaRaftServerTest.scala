/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import java.io.File
import java.nio.file.Files
import java.util.{Optional, Properties}
import org.apache.kafka.common.{KafkaException, Uuid}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.metadata.bootstrap.{BootstrapDirectory, BootstrapMetadata}
import org.apache.kafka.metadata.properties.{MetaProperties, MetaPropertiesEnsemble, MetaPropertiesVersion, PropertiesUtils}
import org.apache.kafka.raft.{MetadataLogConfig, QuorumConfig}
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.server.config.{KRaftConfigs, ServerLogConfigs}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.storage.internals.log.UnifiedLog
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class KafkaRaftServerTest {
  private val clusterIdBase64 = "H3KKO4NTRPaCWtEmm3vW7A"

  @Test
  def testSuccessfulLoadMetaProperties(): Unit = {
    val clusterId = clusterIdBase64
    val nodeId = 0
    val metaProperties = new MetaProperties.Builder().
      setVersion(MetaPropertiesVersion.V1).
      setClusterId(clusterId).
      setNodeId(nodeId).
      build()

    val configProperties = new Properties
    configProperties.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker,controller")
    configProperties.put(KRaftConfigs.NODE_ID_CONFIG, nodeId.toString)
    configProperties.put(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://127.0.0.1:9092,SSL://127.0.0.1:9093")
    configProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"$nodeId@localhost:9093")
    configProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")

    val metaPropertiesEnsemble =
      invokeLoadMetaProperties(metaProperties, configProperties)._1

    val loadedMetaProperties = metaPropertiesEnsemble.logDirProps().values().iterator().next()
    assertEquals(metaProperties, new MetaProperties.Builder(loadedMetaProperties).
      setDirectoryId(Optional.empty[Uuid]).build())
    assertTrue(loadedMetaProperties.directoryId().isPresent)
  }

  @Test
  def testLoadMetaPropertiesWithInconsistentNodeId(): Unit = {
    val clusterId = clusterIdBase64
    val metaNodeId = 1
    val configNodeId = 0

    val metaProperties = new MetaProperties.Builder().
      setVersion(MetaPropertiesVersion.V1).
      setClusterId(clusterId).
      setNodeId(metaNodeId).
      build()
    val configProperties = new Properties

    configProperties.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    configProperties.put(KRaftConfigs.NODE_ID_CONFIG, configNodeId.toString)
    configProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"$configNodeId@localhost:9092")
    configProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "PLAINTEXT")

    assertThrows(classOf[RuntimeException], () =>
      invokeLoadMetaProperties(metaProperties, configProperties))
  }

  private def invokeLoadMetaProperties(
    metaProperties: MetaProperties,
    configProperties: Properties,
    metadataVersion: Option[MetadataVersion] = Some(MetadataVersion.latestTesting())
  ): (MetaPropertiesEnsemble, BootstrapMetadata) = {
    val tempLogDir = TestUtils.tempDirectory()
    try {
      writeMetaProperties(tempLogDir, metaProperties)
      metadataVersion.foreach(mv => writeBootstrapMetadata(tempLogDir, mv))
      configProperties.put(ServerLogConfigs.LOG_DIR_CONFIG, tempLogDir.getAbsolutePath)
      val config = KafkaConfig.fromProps(configProperties)
      KafkaRaftServer.initializeLogDirs(config, MetaPropertiesEnsemble.LOG, "")
    } finally {
      Utils.delete(tempLogDir)
    }
  }

  private def writeMetaProperties(
    logDir: File,
    metaProperties: MetaProperties
  ): Unit = {
    val metaPropertiesFile = new File(logDir.getAbsolutePath, MetaPropertiesEnsemble.META_PROPERTIES_NAME)
    PropertiesUtils.writePropertiesFile(metaProperties.toProperties, metaPropertiesFile.getAbsolutePath, false)
  }

  private def writeBootstrapMetadata(logDir: File, metadataVersion: MetadataVersion): Unit = {
    val bootstrapDirectory = new BootstrapDirectory(logDir.toString)
    bootstrapDirectory.writeBinaryFile(BootstrapMetadata.fromVersion(metadataVersion, "test"))
  }

  @Test
  def testStartupFailsIfMetaPropertiesMissingInSomeLogDir(): Unit = {
    val clusterId = clusterIdBase64
    val nodeId = 1

    // One log dir is online and has properly formatted `meta.properties`.
    // The other is online, but has no `meta.properties`.
    val logDir1 = TestUtils.tempDirectory()
    val logDir2 = TestUtils.tempDirectory()
    writeMetaProperties(logDir1, new MetaProperties.Builder().
      setVersion(MetaPropertiesVersion.V1).
      setClusterId(clusterId).
      setNodeId(nodeId).
      build())

    val configProperties = new Properties
    configProperties.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    configProperties.put(KRaftConfigs.NODE_ID_CONFIG, nodeId.toString)
    configProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"${nodeId + 1}@localhost:9092")
    configProperties.put(ServerLogConfigs.LOG_DIR_CONFIG, Seq(logDir1, logDir2).map(_.getAbsolutePath).mkString(","))
    configProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    val config = KafkaConfig.fromProps(configProperties)

    assertThrows(classOf[RuntimeException],
      () => KafkaRaftServer.initializeLogDirs(config, MetaPropertiesEnsemble.LOG, ""))
  }

  @Test
  def testStartupFailsIfMetaLogDirIsOffline(): Unit = {
    val clusterId = clusterIdBase64
    val nodeId = 1

    // One log dir is online and has properly formatted `meta.properties`
    val validDir = TestUtils.tempDirectory()
    writeMetaProperties(validDir, new MetaProperties.Builder().
      setVersion(MetaPropertiesVersion.V1).
      setClusterId(clusterId).
      setNodeId(nodeId).
      build())

    // Use a regular file as an invalid log dir to trigger an IO error
    val invalidDir = TestUtils.tempFile("blah")
    val configProperties = new Properties
    configProperties.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    configProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"${nodeId + 1}@localhost:9092")
    configProperties.put(KRaftConfigs.NODE_ID_CONFIG, nodeId.toString)
    configProperties.put(MetadataLogConfig.METADATA_LOG_DIR_CONFIG, invalidDir.getAbsolutePath)
    configProperties.put(ServerLogConfigs.LOG_DIR_CONFIG, validDir.getAbsolutePath)
    configProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    val config = KafkaConfig.fromProps(configProperties)

    assertThrows(classOf[RuntimeException],
      () => KafkaRaftServer.initializeLogDirs(config, MetaPropertiesEnsemble.LOG, ""))
  }

  @Test
  def testStartupDoesNotFailIfDataDirIsOffline(): Unit = {
    val clusterId = clusterIdBase64
    val nodeId = 1

    // One log dir is online and has properly formatted `meta.properties`
    val validDir = TestUtils.tempDirectory()
    writeMetaProperties(validDir, new MetaProperties.Builder().
      setVersion(MetaPropertiesVersion.V1).
      setClusterId(clusterId).
      setNodeId(nodeId).
      build())

    writeBootstrapMetadata(validDir, MetadataVersion.latestTesting())

    // Use a regular file as an invalid log dir to trigger an IO error
    val invalidDir = TestUtils.tempFile("blah")
    val configProperties = new Properties
    configProperties.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    configProperties.put(KRaftConfigs.NODE_ID_CONFIG, nodeId.toString)
    configProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"${nodeId + 1}@localhost:9092")
    configProperties.put(MetadataLogConfig.METADATA_LOG_DIR_CONFIG, validDir.getAbsolutePath)
    configProperties.put(ServerLogConfigs.LOG_DIR_CONFIG, invalidDir.getAbsolutePath)
    configProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    val config = KafkaConfig.fromProps(configProperties)

    val (metaPropertiesEnsemble, _) =
      KafkaRaftServer.initializeLogDirs(config, MetaPropertiesEnsemble.LOG, "")
    assertEquals(nodeId, metaPropertiesEnsemble.nodeId().getAsInt)
    assertEquals(invalidDir.getAbsolutePath,
      String.join(", ", metaPropertiesEnsemble.errorLogDirs()))
  }

  @Test
  def testStartupFailsIfUnexpectedMetadataDir(): Unit = {
    val nodeId = 1
    val clusterId = clusterIdBase64

    // Create two directories with valid `meta.properties`
    val metadataDir = TestUtils.tempDirectory()
    val dataDir = TestUtils.tempDirectory()

    Seq(metadataDir, dataDir).foreach { dir =>
      writeMetaProperties(dir, new MetaProperties.Builder().
        setVersion(MetaPropertiesVersion.V1).
        setClusterId(clusterId).
        setNodeId(nodeId).
        build())
    }

    // Create the metadata dir in the data directory
    Files.createDirectory(new File(dataDir, UnifiedLog.logDirName(KafkaRaftServer.MetadataPartition)).toPath)

    val configProperties = new Properties
    configProperties.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    configProperties.put(KRaftConfigs.NODE_ID_CONFIG, nodeId.toString)
    configProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"${nodeId + 1}@localhost:9092")
    configProperties.put(MetadataLogConfig.METADATA_LOG_DIR_CONFIG, metadataDir.getAbsolutePath)
    configProperties.put(ServerLogConfigs.LOG_DIR_CONFIG, dataDir.getAbsolutePath)
    configProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    val config = KafkaConfig.fromProps(configProperties)

    assertThrows(classOf[KafkaException],
      () => KafkaRaftServer.initializeLogDirs(config, MetaPropertiesEnsemble.LOG, ""))
  }

  @Test
  def testLoadPropertiesWithInconsistentClusterIds(): Unit = {
    val nodeId = 1
    val logDir1 = TestUtils.tempDirectory()
    val logDir2 = TestUtils.tempDirectory()

    // Create a random clusterId in each log dir
    Seq(logDir1, logDir2).foreach { dir =>
      writeMetaProperties(dir, new MetaProperties.Builder().
        setVersion(MetaPropertiesVersion.V1).
        setClusterId(Uuid.randomUuid().toString).
        setNodeId(nodeId).
        build())
    }

    val configProperties = new Properties
    configProperties.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    configProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"${nodeId + 1}@localhost:9092")
    configProperties.put(KRaftConfigs.NODE_ID_CONFIG, nodeId.toString)
    configProperties.put(ServerLogConfigs.LOG_DIR_CONFIG, Seq(logDir1, logDir2).map(_.getAbsolutePath).mkString(","))
    configProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    val config = KafkaConfig.fromProps(configProperties)

    assertThrows(classOf[RuntimeException],
      () => KafkaRaftServer.initializeLogDirs(config, MetaPropertiesEnsemble.LOG, ""))
  }

  @Test
  def testKRaftUpdateAt3_3_IV3(): Unit = {
    val clusterId = clusterIdBase64
    val nodeId = 0
    val metaProperties = new MetaProperties.Builder().
      setVersion(MetaPropertiesVersion.V1).
      setClusterId(clusterId).
      setNodeId(nodeId).
      setDirectoryId(Uuid.fromString("4jm0e-YRYeB6CCKBvwoS8w")).
      build()

    val configProperties = new Properties
    configProperties.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker,controller")
    configProperties.put(KRaftConfigs.NODE_ID_CONFIG, nodeId.toString)
    configProperties.put(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://127.0.0.1:9092,SSL://127.0.0.1:9093")
    configProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"$nodeId@localhost:9093")
    configProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")

    val (metaPropertiesEnsemble, bootstrapMetadata) =
      invokeLoadMetaProperties(metaProperties, configProperties, Some(MetadataVersion.IBP_3_3_IV3))

    assertEquals(metaProperties, metaPropertiesEnsemble.logDirProps().values().iterator().next())
    assertTrue(metaPropertiesEnsemble.errorLogDirs().isEmpty)
    assertTrue(metaPropertiesEnsemble.emptyLogDirs().isEmpty)
    assertEquals(bootstrapMetadata.metadataVersion(), MetadataVersion.IBP_3_3_IV3)
  }

  @Test
  def testKRaftUpdate(): Unit = {
    val clusterId = clusterIdBase64
    val nodeId = 0
    val metaProperties = new MetaProperties.Builder().
      setVersion(MetaPropertiesVersion.V1).
      setClusterId(clusterId).
      setNodeId(nodeId).
      setDirectoryId(Uuid.fromString("4jm0e-YRYeB6CCKBvwoS8w")).
      build()

    val logDir = TestUtils.tempDirectory()
    writeMetaProperties(logDir, metaProperties)

    val configProperties = new Properties
    configProperties.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker,controller")
    configProperties.put(KRaftConfigs.NODE_ID_CONFIG, nodeId.toString)
    configProperties.put(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://127.0.0.1:9092,SSL://127.0.0.1:9093")
    configProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"$nodeId@localhost:9093")
    configProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    configProperties.put(ServerLogConfigs.LOG_DIR_CONFIG, logDir.getAbsolutePath)

    val (metaPropertiesEnsemble, bootstrapMetadata) =
      invokeLoadMetaProperties(metaProperties, configProperties, None)

    assertEquals(metaProperties, metaPropertiesEnsemble.logDirProps().values().iterator().next())
    assertTrue(metaPropertiesEnsemble.errorLogDirs().isEmpty)
    assertTrue(metaPropertiesEnsemble.emptyLogDirs().isEmpty)
    assertEquals(bootstrapMetadata.metadataVersion(), MetadataVersion.latestProduction())
  }
}
