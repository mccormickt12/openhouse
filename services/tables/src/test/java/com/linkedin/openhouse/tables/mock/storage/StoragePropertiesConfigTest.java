package com.linkedin.openhouse.tables.mock.storage;

import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import com.linkedin.openhouse.cluster.storage.selector.impl.RegexStorageSelector;
import com.linkedin.openhouse.tables.mock.properties.CustomClusterPropertiesInitializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = CustomClusterPropertiesInitializer.class)
public class StoragePropertiesConfigTest {
  @Autowired private StorageProperties storageProperties;

  @MockBean private StorageManager storageManager;
  private static final String DEFAULT_TYPE = "hdfs";

  private static final String DEFAULT_ENDPOINT = "file:///";

  private static final String ANOTHER_TYPE = "local";

  private static final String NON_EXISTING_TYPE = "non-existing-type";

  @Test
  public void testDefaultType() {
    Assertions.assertEquals(DEFAULT_TYPE, storageProperties.getDefaultType());
  }

  @Test
  public void testStorageTypeEndpoint() {
    Assertions.assertEquals(
        DEFAULT_ENDPOINT, storageProperties.getTypes().get(DEFAULT_TYPE).getEndpoint());
  }

  @Test
  public void testStorageTypeLookup() {
    Assertions.assertEquals(
        DEFAULT_ENDPOINT, storageProperties.getTypes().get(ANOTHER_TYPE).getEndpoint());
  }

  @Test
  public void testStorageTypeVariableProperties() {
    Assertions.assertFalse(
        storageProperties.getTypes().get(DEFAULT_TYPE).getParameters().isEmpty());
  }

  @Test
  public void testUnsetPropertiesAreNull() {
    Assertions.assertNull(storageProperties.getTypes().get(NON_EXISTING_TYPE));
  }

  @Test
  public void testStorageSelector() {
    Assertions.assertNotNull(storageProperties.getStorageSelector());
    Assertions.assertEquals(
        storageProperties.getStorageSelector().getName(),
        RegexStorageSelector.class.getSimpleName());
    Assertions.assertNotNull(storageProperties.getStorageSelector().getParameters());
    Assertions.assertEquals(storageProperties.getStorageSelector().getParameters().size(), 2);
    Assertions.assertEquals(
        storageProperties.getStorageSelector().getParameters().get("regex"),
        "local_db\\.[a-zA-Z0-9_]+$");
    Assertions.assertEquals(
        storageProperties.getStorageSelector().getParameters().get("storage-type"), "local");
  }

  @AfterAll
  static void unsetSysProp() {
    System.clearProperty("OPENHOUSE_CLUSTER_CONFIG_PATH");
  }
}
