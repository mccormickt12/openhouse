package com.linkedin.openhouse.javaclient.s3;

import com.linkedin.openhouse.client.ssl.TablesApiClientFactory;
import com.linkedin.openhouse.tables.client.api.TableApi;
import com.linkedin.openhouse.tables.client.model.GetTableAccessTokenResponseBody;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.Optional;
import javax.net.ssl.SSLException;
import org.apache.iceberg.aws.AssumeRoleAwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;


public class S3OpenHouseAwsClientProvider extends AssumeRoleAwsClientFactory {

  public static final String OH_S3_PROVIDER_URI_CONFIG = "uri";

  public static final String OH_S3_PROVIDER_TRUST_STORE_CONFIG = "trust-store";

  public static final String OH_S3_PROVIDER_AUTH_TOKEN = "auth-token";

  public static final String OH_S3_PROVIDER_DB = "db";

  public static final String OH_S3_PROVIDER_TABLE = "table";

  AwsProperties awsProperties;

  Map<String, String> properties;

  @Override
  public S3Client s3() {

    String truststore = properties.getOrDefault(OH_S3_PROVIDER_TRUST_STORE_CONFIG, "");
    String token = properties.getOrDefault(OH_S3_PROVIDER_AUTH_TOKEN, null);
    String uri = properties.getOrDefault(OH_S3_PROVIDER_URI_CONFIG, "http://localhost:443/");
    String db = properties.get(OH_S3_PROVIDER_DB);
    String table = properties.get(OH_S3_PROVIDER_TABLE);

    try {
      TableApi tableApi = new TableApi(TablesApiClientFactory.getInstance().createApiClient(uri, token, truststore));

      System.out.println("S3OpenHouseAwsClientProvider is being created with "
          + truststore + " " + token + " " + uri + " " + db + " " + table);

      return S3Client.builder()
          .applyMutation(awsProperties::applyHttpClientConfigurations)
          .applyMutation(awsProperties::applyS3EndpointConfigurations)
          .applyMutation(awsProperties::applyS3ServiceConfigurations)
          .applyMutation(awsProperties::applyS3SignerConfiguration)
          .credentialsProvider(new BetterS3OpenHouseAwsClientProvider(db, table, tableApi))
          .build();

    } catch (MalformedURLException | SSLException e) {
      throw new RuntimeException(e);
    }
  }

  public void initialize(Map<String, String> properties) {
    this.awsProperties = new AwsProperties(properties);
    this.properties = properties;

    String truststore = properties.getOrDefault(OH_S3_PROVIDER_TRUST_STORE_CONFIG, "");
    String token = properties.getOrDefault(OH_S3_PROVIDER_AUTH_TOKEN, null);
    String uri = properties.getOrDefault(OH_S3_PROVIDER_URI_CONFIG, "http://localhost:443/");
    String db = properties.get(OH_S3_PROVIDER_DB);
    String table = properties.get(OH_S3_PROVIDER_TABLE);

    if (db == null || db.isEmpty() || table == null || table.isEmpty()) {
      throw new RuntimeException("bad db or table");
    }

    super.initialize(properties);
  }

  static class BetterS3OpenHouseAwsClientProvider implements AwsCredentialsProvider {

    private final String db;

    private final String table;

    private final TableApi tableApi;

    private String cacheCredential;

    public BetterS3OpenHouseAwsClientProvider(
        String db,
        String table,
        TableApi tableApi) {
      this.db = db;
      this.table = table;
      this.tableApi = tableApi;
    }

    @Override
    public AwsCredentials resolveCredentials() {
      if (cacheCredential == null || cacheCredential.isEmpty()) {
        System.out.println("Data access token is fetched");

        Optional<String> accessToken =
            tableApi
                .getTableAccessTokenV0(db, table)
                .mapNotNull(GetTableAccessTokenResponseBody::getAccessToken)
                .blockOptional();

        String accessTokenStr = accessToken.get();
        cacheCredential = accessTokenStr;
        try {
          System.out.println("Access token fetched: " + accessTokenStr);
          System.out.println("Print host name: " + InetAddress.getLocalHost().getHostName());
        } catch (Exception e) {
          System.out.println("Failed to read host name with: " + e.getMessage());
        }
      } else {
        System.out.println("Data access token is cached");
      }
      // [0]=Access Key ID, [1]=Secret
      String[] accessTokenParts = cacheCredential.split("\\.");
      System.out.println("Access token split fetched: " + accessTokenParts);

      return AwsBasicCredentials.create(accessTokenParts[0], accessTokenParts[1]);
    }
  }



}
