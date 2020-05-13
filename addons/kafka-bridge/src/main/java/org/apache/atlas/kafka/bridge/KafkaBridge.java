/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.kafka.bridge;

import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.kafka.model.KafkaDataTypes;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class KafkaBridge {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaBridge.class);

    private static final int    EXIT_CODE_SUCCESS          = 0;
    private static final int    EXIT_CODE_FAILED           = 1;
    private static final String ATLAS_ENDPOINT             = "atlas.rest.address";
    private static final String DEFAULT_ATLAS_URL          = "http://localhost:21000/";
    private static final String CLUSTER_NAME_KEY           = "atlas.cluster.name";
    private static final String KAFKA_METADATA_NAMESPACE   = "atlas.metadata.namespace";
    private static final String DEFAULT_CLUSTER_NAME       = "primary";
    private static final String ATTRIBUTE_QUALIFIED_NAME   = "qualifiedName";
    private static final String DESCRIPTION_ATTR           = "description";
    private static final String PARTITION_COUNT            = "partitionCount";
    private static final String NAME                       = "name";
    private static final String URI                        = "uri";
    private static final String CLUSTERNAME                = "clusterName";
    private static final String TOPIC                      = "topic";
    private static final String TOPIC_OWNER                = "topicOwner";
    private static final String KAKFKA_CLUSTER             = "kafkaCluster";
    private static final String CONSUMER_GROUPS            = "consumerGroups";

    private static final String FORMAT_KAKFA_TOPIC_QUALIFIED_NAME       = "%s@%s";
    private static final String ZOOKEEPER_CONNECT                       = "atlas.kafka.zookeeper.connect";
    private static final String ZOOKEEPER_CONNECTION_TIMEOUT_MS         = "atlas.kafka.zookeeper.connection.timeout.ms";
    private static final String ZOOKEEPER_SESSION_TIMEOUT_MS            = "atlas.kafka.zookeeper.session.timeout.ms";
    private static final String DEFAULT_ZOOKEEPER_CONNECT               = "localhost:2181";
    private static final int    DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS    = 10 * 1000;
    private static final int    DEFAULT_ZOOKEEPER_CONNECTION_TIMEOUT_MS = 10 * 1000;
    private static final String KAFKA_BOOTSTRAP_SERVERS                 = "atlas.kafka.bootstrap.servers";


    private final List<String>  availableTopics;
    private final String        metadataNamespace;
    private final AtlasClientV2 atlasClientV2;
    private final ZkUtils       zkUtils;
    private final AdminClient client;
    public Map<String, String> ClusterIdToName = new HashMap<>();


    public static void main(String[] args) {
        int exitCode = EXIT_CODE_FAILED;
        AtlasClientV2 atlasClientV2 = null;

        try {
            Options options = new Options();
            options.addOption("t","topic", true, "topic");
            options.addOption("f", "filename", true, "filename");

            CommandLineParser parser        = new BasicParser();
            CommandLine       cmd           = parser.parse(options, args);
            String            topicToImport = cmd.getOptionValue("t");
            String            fileToImport  = cmd.getOptionValue("f");
            Configuration     atlasConf     = ApplicationProperties.get();
            String[]          urls          = atlasConf.getStringArray(ATLAS_ENDPOINT);

            if (urls == null || urls.length == 0) {
                urls = new String[] { DEFAULT_ATLAS_URL };
            }


            if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
                String[] basicAuthUsernamePassword = AuthenticationUtil.getBasicAuthenticationInput();

                atlasClientV2 = new AtlasClientV2(urls, basicAuthUsernamePassword);
            } else {
                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

                atlasClientV2 = new AtlasClientV2(ugi, ugi.getShortUserName(), urls);
            }

            KafkaBridge importer = new KafkaBridge(atlasConf, atlasClientV2);
            importer.InitKafkaIdToName();

            if (StringUtils.isNotEmpty(fileToImport)) {
                File f = new File(fileToImport);

                if (f.exists() && f.canRead()) {
                    BufferedReader br   = new BufferedReader(new FileReader(f));
                    String         line = null;

                    while((line = br.readLine()) != null) {
                        topicToImport = line.trim();

                        importer.importTopic(topicToImport);
                    }

                    exitCode = EXIT_CODE_SUCCESS;
                } else {
                    LOG.error("Failed to read the file");
                }
            } else {
                importer.importTopic(topicToImport);

                exitCode = EXIT_CODE_SUCCESS;
            }
            importer.printMap();
        } catch(ParseException e) {
            LOG.error("Failed to parse arguments. Error: ", e.getMessage());
            printUsage();
        } catch(Exception e) {
            System.out.println("ImportKafkaEntities failed. Please check the log file for the detailed error message");
            e.printStackTrace();
            LOG.error("ImportKafkaEntities failed", e);
        } finally {
            if (atlasClientV2 != null) {
                atlasClientV2.close();
            }
        }

        System.exit(exitCode);
    }

    public KafkaBridge(Configuration atlasConf, AtlasClientV2 atlasClientV2) throws Exception {
        String   zookeeperConnect    = getZKConnection(atlasConf);
        int      sessionTimeOutMs    = atlasConf.getInt(ZOOKEEPER_SESSION_TIMEOUT_MS, DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS) ;
        int      connectionTimeOutMs = atlasConf.getInt(ZOOKEEPER_CONNECTION_TIMEOUT_MS, DEFAULT_ZOOKEEPER_CONNECTION_TIMEOUT_MS);
        ZkClient zkClient            = new ZkClient(zookeeperConnect, sessionTimeOutMs, connectionTimeOutMs, ZKStringSerializer$.MODULE$);

        this.atlasClientV2     = atlasClientV2;
        this.metadataNamespace = getMetadataNamespace(atlasConf);
        this.zkUtils           = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), JaasUtils.isZkSecurityEnabled());
        this.availableTopics   = scala.collection.JavaConversions.seqAsJavaList(zkUtils.getAllTopics());

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBootstrapServers(atlasConf));
        client = AdminClient.create(props);
    }

    private String getKafkaBootstrapServers(Configuration config) {
        return config.getString(KAFKA_BOOTSTRAP_SERVERS);
    }

    private String getMetadataNamespace(Configuration config) {
        return config.getString(KAFKA_METADATA_NAMESPACE, getClusterName(config));
    }

    private String getClusterName(Configuration config) {
        return config.getString(CLUSTER_NAME_KEY, DEFAULT_CLUSTER_NAME);
    }

    public void importTopic(String topicToImport) throws Exception {
        List<String> topics = availableTopics;

        if (StringUtils.isNotEmpty(topicToImport)) {
            List<String> topics_subset = new ArrayList<>();
            for(String topic : topics) {
                if (Pattern.compile(topicToImport).matcher(topic).matches()) {
                    topics_subset.add(topic);
                }
            }
            topics = topics_subset;
        }

        if (CollectionUtils.isNotEmpty(topics)) {
            for(String topic : topics) {
                createOrUpdateTopic(topic);
            }
        }
    }

    @VisibleForTesting
    AtlasEntityWithExtInfo createOrUpdateTopic(String topic) throws Exception {
        String clusterId                          = zkUtils.getClusterId().get().toLowerCase();
        String                 topicQualifiedName = getTopicQualifiedName(metadataNamespace, clusterId + "#" + topic);
        AtlasEntityWithExtInfo topicEntity        = findTopicEntityInAtlas(topicQualifiedName);

        if (topicEntity == null) {
            System.out.println("Adding Kafka topic " + topic);
            LOG.info("Importing Kafka topic: {}", topicQualifiedName);

            AtlasEntity entity = getTopicEntity(topic, null);

            topicEntity = createEntityInAtlas(new AtlasEntityWithExtInfo(entity));
        } else {
            System.out.println("Updating Kafka topic "  + topic);
            LOG.info("Kafka topic {} already exists in Atlas. Updating it..", topicQualifiedName);

            AtlasEntity entity = getTopicEntity(topic, topicEntity.getEntity());

            topicEntity.setEntity(entity);

            topicEntity = updateEntityInAtlas(topicEntity);
        }

        return topicEntity;
    }

    @VisibleForTesting
    AtlasEntity getTopicEntity(String topic, AtlasEntity topicEntity) {
        final AtlasEntity ret;
        String topicOwner = null;
        String clusterId = zkUtils.getClusterId().get().toLowerCase();
        String qualifiedName = getTopicQualifiedName(metadataNamespace, clusterId + "#"+ topic);

        if (topicEntity == null) {
            ret = new AtlasEntity(KafkaDataTypes.KAFKA_TOPIC.getName());
        } else {
            ret = topicEntity;
            topicOwner = topicEntity.getAttribute(TOPIC_OWNER) == null ?
                    null : topicEntity.getAttribute(TOPIC_OWNER).toString();
            qualifiedName = topicEntity.getAttribute(ATTRIBUTE_QUALIFIED_NAME) == null ?
                    qualifiedName : topicEntity.getAttribute(ATTRIBUTE_QUALIFIED_NAME).toString();
        }

        ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, qualifiedName);
        ret.setAttribute(CLUSTERNAME, metadataNamespace);
        ret.setAttribute(TOPIC, topic);
        ret.setAttribute(NAME,topic);
        ret.setAttribute(DESCRIPTION_ATTR, topic);
        ret.setAttribute(URI, topic);
        ret.setAttribute(PARTITION_COUNT, (Integer) zkUtils.getTopicPartitionCount(topic).get());

        if (topicOwner == null || topicOwner.isEmpty()) {
            try {
                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                topicOwner = ugi.getShortUserName();
            } catch (Exception e) {
                LOG.error("Failed to get current user. " + e);
            }
        }
        ret.setAttribute(TOPIC_OWNER, topicOwner);

        ret.setAttribute(KAKFKA_CLUSTER, ClusterIdToName.getOrDefault(clusterId, "NotInInitCluster"));

        String consumerGroups = String.join(",", getGroupsForTopic(topic));
        ret.setAttribute(CONSUMER_GROUPS, consumerGroups);

        return ret;
    }

    private List<String> getGroupsForTopic(String topic) {
        final List<String> filteredGroups = new ArrayList<>();
        try {
            List<String> allGroups = client.listConsumerGroups()
                    .valid()
                    .get(10, TimeUnit.SECONDS)
                    .stream()
                    .map(ConsumerGroupListing::groupId)
                    .collect(Collectors.toList());

            Map<String, ConsumerGroupDescription> allGroupDetails =
                    client.describeConsumerGroups(allGroups).all().get(50, TimeUnit.SECONDS);

            allGroupDetails.entrySet().forEach(entry -> {
                String groupId = entry.getKey();
                ConsumerGroupDescription description = entry.getValue();
                boolean topicSubscribed = description.members().stream().map(MemberDescription::assignment)
                        .map(MemberAssignment::topicPartitions)
                        .map(tps -> tps.stream().map(TopicPartition::topic).collect(Collectors.toSet()))
                        .anyMatch(tps -> tps.contains(topic));
                if (topicSubscribed)
                    filteredGroups.add(groupId);
            });
        } catch (Exception e) {
            LOG.error("Failed to get consumer group. " + e);
        }
        return filteredGroups;
    }

    @VisibleForTesting
    static String getTopicQualifiedName(String metadataNamespace, String topic) {
        return String.format(FORMAT_KAKFA_TOPIC_QUALIFIED_NAME, topic.toLowerCase(), metadataNamespace);
    }

    private AtlasEntityWithExtInfo findTopicEntityInAtlas(String topicQualifiedName) {
        AtlasEntityWithExtInfo ret = null;

        try {
            ret = findEntityInAtlas(KafkaDataTypes.KAFKA_TOPIC.getName(), topicQualifiedName);
            clearRelationshipAttributes(ret);
        } catch (Exception e) {
            ret = null; // entity doesn't exist in Atlas
        }

        return ret;
    }

    @VisibleForTesting
     AtlasEntityWithExtInfo findEntityInAtlas(String typeName, String qualifiedName) throws Exception {
        Map<String, String> attributes = Collections.singletonMap(ATTRIBUTE_QUALIFIED_NAME, qualifiedName);

        return atlasClientV2.getEntityByAttribute(typeName, attributes);
    }

    @VisibleForTesting
    AtlasEntityWithExtInfo createEntityInAtlas(AtlasEntityWithExtInfo entity) throws Exception {
        AtlasEntityWithExtInfo  ret      = null;
        EntityMutationResponse  response = atlasClientV2.createEntity(entity);
        List<AtlasEntityHeader> entities = response.getCreatedEntities();

        if (CollectionUtils.isNotEmpty(entities)) {
            AtlasEntityWithExtInfo getByGuidResponse = atlasClientV2.getEntityByGuid(entities.get(0).getGuid());

            ret = getByGuidResponse;

            LOG.info("Created {} entity: name={}, guid={}", ret.getEntity().getTypeName(), ret.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME), ret.getEntity().getGuid());
        }

        return ret;
    }

    @VisibleForTesting
    AtlasEntityWithExtInfo updateEntityInAtlas(AtlasEntityWithExtInfo entity) throws Exception {
        AtlasEntityWithExtInfo ret      = null;
        EntityMutationResponse response = atlasClientV2.updateEntity(entity);

        if (response != null) {
            List<AtlasEntityHeader> entities = response.getUpdatedEntities();

            if (CollectionUtils.isNotEmpty(entities)) {
                AtlasEntityWithExtInfo getByGuidResponse = atlasClientV2.getEntityByGuid(entities.get(0).getGuid());

                ret = getByGuidResponse;

                LOG.info("Updated {} entity: name={}, guid={} ", ret.getEntity().getTypeName(), ret.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME), ret.getEntity().getGuid());
            } else {
                LOG.info("Entity: name={} ", entity.toString() + " not updated as it is unchanged from what is in Atlas" );

                ret = entity;
            }
        } else {
            LOG.info("Entity: name={} ", entity.toString() + " not updated as it is unchanged from what is in Atlas" );

            ret = entity;
        }

        return ret;
    }

    private static  void printUsage(){
        System.out.println("Usage 1: import-kafka.sh");
        System.out.println("Usage 2: import-kafka.sh [-t <topic regex> OR --topic <topic regex>]");
        System.out.println("Usage 3: import-kafka.sh [-f <filename>]" );
        System.out.println("   Format:");
        System.out.println("        topic1 OR topic1 regex");
        System.out.println("        topic2 OR topic2 regex");
        System.out.println("        topic3 OR topic3 regex");
    }


    private void clearRelationshipAttributes(AtlasEntityWithExtInfo entity) {
        if (entity != null) {
            clearRelationshipAttributes(entity.getEntity());

            if (entity.getReferredEntities() != null) {
                clearRelationshipAttributes(entity.getReferredEntities().values());
            }
        }
    }

    private void clearRelationshipAttributes(Collection<AtlasEntity> entities) {
        if (entities != null) {
            for (AtlasEntity entity : entities) {
                clearRelationshipAttributes(entity);
            }
        }
    }

    private void clearRelationshipAttributes(AtlasEntity entity) {
        if (entity != null && entity.getRelationshipAttributes() != null) {
            entity.getRelationshipAttributes().clear();
        }
    }

    private String getStringValue(String[] vals) {
        String ret = null;
        for(String val:vals) {
            ret = (ret == null) ? val : ret + "," + val;
        }
        return  ret;
    }

    private String getZKConnection(Configuration atlasConf) {
        String ret = null;
        ret = getStringValue(atlasConf.getStringArray(ZOOKEEPER_CONNECT));
        if (StringUtils.isEmpty(ret) ) {
            ret = DEFAULT_ZOOKEEPER_CONNECT;
        }
        return ret;
    }

    public void InitKafkaIdToName() {
        Map<String, String> kafkaCluster = new HashMap<>();
        kafkaCluster.put("123", "456");  // 脱敏

        for (Map.Entry<String, String> entry : kafkaCluster.entrySet()) {
            try {
                Properties propsIn = new Properties();
                propsIn.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, entry.getValue());
                propsIn.put("retries", 2);
                propsIn.put("request.timeout.ms", 50000);
                AdminClient clientIn = AdminClient.create(propsIn);
                String clusterId = clientIn.describeCluster().clusterId().get().toLowerCase();
                ClusterIdToName.put(clusterId, entry.getKey());
                clientIn.close();
                System.out.println("Success to init: " + entry.getKey());
            } catch (Exception e) {
                System.out.println("Failed to init kafkaClusters. " + e);
            }
        }
        printMap();
    }

    public void printMap() {
        System.out.println("Success to init all clusters name: ");
        System.out.println("Size: " + ClusterIdToName.size());
        System.out.println(ClusterIdToName.toString());
    }
}
