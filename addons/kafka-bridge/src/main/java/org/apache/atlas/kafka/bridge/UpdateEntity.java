package org.apache.atlas.kafka.bridge;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class UpdateEntity {
    private static final String ATLAS_ENDPOINT             = "atlas.rest.address";
    private static final String DEFAULT_ATLAS_URL          = "http://localhost:21000/";

    private final AtlasClientV2 atlasClientV2;

    public UpdateEntity(Configuration atlasConf, AtlasClientV2 atlasClientV2) throws Exception {
        this.atlasClientV2     = atlasClientV2;
    }

    public static void main(String[] args) {

        AtlasClientV2 atlasClientV2 = null;

        try {
            Configuration atlasConf     = ApplicationProperties.get();
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

            UpdateEntity updateEntity = new UpdateEntity(atlasConf, atlasClientV2);
            updateEntity.updateKafkaTopic(atlasClientV2, args[0]);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void updateKafkaTopic(AtlasClientV2 atlasClientV2, String newAtlasAttributeDef) throws AtlasServiceException {
        //根据实体类型定义名获取实体类型定义
        AtlasEntityDef kafkaTopic = atlasClientV2.getEntityDefByName("kafka_topic");
        //添加一个新的属性newAtlasAttributeDef
        kafkaTopic.getAttributeDefs().add(new AtlasStructDef.AtlasAttributeDef(newAtlasAttributeDef, "string", true,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, 1, 1, false, false, false,null));
        //将实体类型定义设置到一个新的AtlasTypeDef中
        AtlasTypesDef atlasTypesDef = new AtlasTypesDef();
        atlasTypesDef.getEntityDefs().add(kafkaTopic);
        atlasClientV2.updateAtlasTypeDefs(atlasTypesDef);
    }
}
