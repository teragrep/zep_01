/*
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

package com.teragrep.zep_01.interpreter.launcher;

import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import com.teragrep.zep_01.conf.ZeppelinConfiguration;
import com.teragrep.zep_01.interpreter.recovery.RecoveryStorage;
import com.teragrep.zep_01.interpreter.remote.RemoteInterpreterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.Properties;

/**
 * Spark specific launcher.
 */
public class SparkInterpreterLauncher extends StandardInterpreterLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkInterpreterLauncher.class);
  public static final String SPARK_MASTER_KEY = "spark.master";
  private static final String DEFAULT_MASTER = "local[*]";
  Optional<String> sparkMaster = Optional.empty();

  public SparkInterpreterLauncher(ZeppelinConfiguration zConf, RecoveryStorage recoveryStorage) {
    super(zConf, recoveryStorage);
  }

  @Override
  public Map<String, String> buildEnvFromProperties(InterpreterLaunchContext context) throws IOException {
    Map<String, String> env = super.buildEnvFromProperties(context);
    Properties sparkProperties = new Properties();
    String spMaster = getSparkMaster();
    if (spMaster != null) {
      sparkProperties.put(SPARK_MASTER_KEY, spMaster);
    }
    for (String key : properties.stringPropertyNames()) {
      String propValue = properties.getProperty(key);
      if (RemoteInterpreterUtils.isEnvString(key) && !StringUtils.isBlank(propValue)) {
        env.put(key, propValue);
      }
      if (isSparkConf(key, propValue)) {
        sparkProperties.setProperty(key, propValue);
      }
    }

    // set spark.app.name if it is not set or empty
    if (!sparkProperties.containsKey("spark.app.name") ||
            StringUtils.isBlank(sparkProperties.getProperty("spark.app.name"))) {
      sparkProperties.setProperty("spark.app.name", context.getInterpreterGroupId());
    }

    setupPropertiesForPySpark(sparkProperties);

    String condaEnvName = context.getProperties().getProperty("zeppelin.interpreter.conda.env.name");
    if (StringUtils.isNotBlank(condaEnvName)) {
      if (!isYarnCluster()) {
        throw new IOException("zeppelin.interpreter.conda.env.name only works for yarn-cluster mode");
      }
      sparkProperties.setProperty("spark.pyspark.python", condaEnvName + "/bin/python");
    }

    if (isYarnMode() && getDeployMode().equals("cluster")) {
      env.put("ZEPPELIN_SPARK_YARN_CLUSTER", "true");
      sparkProperties.setProperty("spark.yarn.submit.waitAppCompletion", "false");
    } else if (zConf.isOnlyYarnCluster()){
      throw new IOException("Only yarn-cluster mode is allowed, please set " +
              ZeppelinConfiguration.ConfVars.ZEPPELIN_SPARK_ONLY_YARN_CLUSTER.getVarName() +
              " to false if you want to use other modes.");
    }

    if (isYarnMode() && getDeployMode().equals("cluster")) {
      if (sparkProperties.containsKey("spark.files")) {
        sparkProperties.put("spark.files", sparkProperties.getProperty("spark.files") + "," +
            zConf.getConfDir() + "/log4j_yarn_cluster.properties");
      } else {
        sparkProperties.put("spark.files", zConf.getConfDir() + "/log4j_yarn_cluster.properties");
      }
      sparkProperties.put("spark.yarn.maxAppAttempts", "1");
    }

    if (isYarnMode()
        && getDeployMode().equals("cluster")) {

      String scalaVersion = null;
      try {
        scalaVersion = detectSparkScalaVersion(getEnv("SPARK_HOME"), env);
        LOGGER.info("Scala version: {}", scalaVersion);
        context.getProperties().put("zeppelin.spark.scala.version", scalaVersion);
      } catch (Exception e) {
        throw new IOException("Fail to detect scala version, the reason is:"+ e.getMessage());
      }

      try {
        List<String> additionalJars = new ArrayList<>();

        Path scalaFolder =  Paths.get(zConf.getZeppelinHome(), "/interpreter/spark/scala-" + scalaVersion);
        if (!scalaFolder.toFile().exists()) {
          throw new IOException("spark scala folder " + scalaFolder.toFile() + " doesn't exist");
        }
        try (DirectoryStream<Path> scalaStream = Files.newDirectoryStream(scalaFolder, Files::isRegularFile)) {
          List<String> scalaJars = StreamSupport.stream(scalaStream.spliterator(),
                false)
                .map(jar -> jar.toAbsolutePath().toString()).collect(Collectors.toList());
          additionalJars.addAll(scalaJars);
        }
        // add zeppelin-interpreter-shaded
        Path interpreterFolder = Paths.get(zConf.getZeppelinHome(), "/interpreter");
        try (DirectoryStream<Path> interpreterStream = Files.newDirectoryStream(interpreterFolder, Files::isRegularFile)) {
          List<String> interpreterJars = StreamSupport.stream(interpreterStream.spliterator(),
                false)
                .filter(jar -> jar.toFile().getName().startsWith("zep_01.zeppelin-interpreter-shaded")
                        && jar.toFile().getName().endsWith(".jar"))
                .map(jar -> jar.toAbsolutePath().toString())
                .collect(Collectors.toList());
          if (interpreterJars.isEmpty()) {
            throw new IOException("zeppelin-interpreter-shaded jar is not found");
          } else if (interpreterJars.size() > 1) {
            throw new IOException("more than 1 zeppelin-interpreter-shaded jars are found: "
                    + StringUtils.join(interpreterJars, ","));
          }
          additionalJars.addAll(interpreterJars);
        }
        Path dplFolder =  Paths.get(zConf.getZeppelinHome(), "/interpreter/dpl");
        try (DirectoryStream<Path> dplFolderStream = Files.newDirectoryStream(dplFolder, Files::isRegularFile)) {
          List<String> dplJars = StreamSupport.stream(dplFolderStream.spliterator(),
                          false)
                  .filter(jar -> jar.toFile().getName().startsWith("pth_07")
                          && jar.toFile().getName().endsWith(".jar"))
                  .map(jar -> jar.toAbsolutePath().toString()).collect(Collectors.toList());
          additionalJars.addAll(dplJars);
        }
        additionalJars.add("/opt/teragrep/pth_10/lib/pth_10-shaded.jar");

        if (sparkProperties.containsKey("spark.jars")) {
          sparkProperties.put("spark.jars", sparkProperties.getProperty("spark.jars") + "," +
                  StringUtils.join(additionalJars, ","));
        } else {
          sparkProperties.put("spark.jars", StringUtils.join(additionalJars, ","));
        }
        LOGGER.debug("Added the following additional jars: <{}>", additionalJars);
      } catch (Exception e) {
        throw new IOException("Fail to set additional jars for spark interpreter", e);
      }
    }

    StringJoiner sparkConfSJ = new StringJoiner("|");
    if (context.getOption().isUserImpersonate() && zConf.getZeppelinImpersonateSparkProxyUser()) {
      sparkConfSJ.add("--proxy-user");
      sparkConfSJ.add(context.getUserName());
      sparkProperties.remove("spark.yarn.keytab");
      sparkProperties.remove("spark.yarn.principal");
    }

    for (String name : sparkProperties.stringPropertyNames()) {
      sparkConfSJ.add("--conf");
      sparkConfSJ.add(name + "=" + sparkProperties.getProperty(name) + "");
    }

    env.put("ZEPPELIN_SPARK_CONF", sparkConfSJ.toString());

    // set these env in the order of
    // 1. interpreter-setting
    // 2. zeppelin-env.sh
    // It is encouraged to set env in interpreter setting, but just for backward compatibility,
    // we also fallback to zeppelin-env.sh if it is not specified in interpreter setting.
    for (String envName : new String[]{"SPARK_HOME", "SPARK_CONF_DIR", "HADOOP_CONF_DIR"})  {
      String envValue = getEnv(envName);
      if (!StringUtils.isBlank(envValue)) {
        env.put(envName, envValue);
      }
    }

    String keytab = properties.getProperty("spark.yarn.keytab",
            zConf.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_SERVER_KERBEROS_KEYTAB));
    String principal = properties.getProperty("spark.yarn.principal",
            zConf.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_SERVER_KERBEROS_PRINCIPAL));

    if (!StringUtils.isBlank(keytab) && !StringUtils.isBlank(principal)) {
      env.put("ZEPPELIN_SERVER_KERBEROS_KEYTAB", keytab);
      env.put("ZEPPELIN_SERVER_KERBEROS_PRINCIPAL", principal);
      LOGGER.info("Run Spark under secure mode with keytab: {}, principal: {}",keytab, principal);
    } else {
      LOGGER.info("Run Spark under non-secure mode as no keytab and principal is specified");
    }

    env.put("PYSPARK_PIN_THREAD", "true");

    // ZEPPELIN_INTP_CLASSPATH
    String sparkConfDir = getEnv("SPARK_CONF_DIR");
    if (StringUtils.isBlank(sparkConfDir)) {
      String sparkHome = getEnv("SPARK_HOME");
      sparkConfDir = sparkHome + "/conf";
    }
    Properties sparkDefaultProperties = new Properties();
    File sparkDefaultFile = new File(sparkConfDir, "spark-defaults.conf");
    if (sparkDefaultFile.exists()) {
      sparkDefaultProperties.load(new FileInputStream(sparkDefaultFile));
      String driverExtraClassPath = sparkDefaultProperties.getProperty("spark.driver.extraClassPath");
      if (!StringUtils.isBlank(driverExtraClassPath)) {
        env.put("ZEPPELIN_INTP_CLASSPATH", driverExtraClassPath);
      }
    } else {
      LOGGER.warn("spark-defaults.conf doesn't exist: {}", sparkDefaultFile.getAbsolutePath());
    }

    if (isYarnMode()) {
      boolean runAsLoginUser = Boolean.parseBoolean(context
              .getProperties()
              .getProperty("zeppelin.spark.run.asLoginUser", "true"));
      String userName = context.getUserName();
      if (runAsLoginUser && !"anonymous".equals(userName)) {
        env.put("HADOOP_USER_NAME", userName);
      }
    }
    LOGGER.info("buildEnvFromProperties: {}", env);
    return env;
  }

  private String detectSparkScalaVersion(String sparkHome, Map<String, String> env) throws Exception {
    LOGGER.info("Detect scala version from SPARK_HOME: {}", sparkHome);
    ProcessBuilder builder = new ProcessBuilder(sparkHome + "/bin/spark-submit", "--version");
    builder.environment().putAll(env);
    Process process = builder.start();
    process.waitFor();
    String processOutput;
    try(InputStream inputStream = process.getErrorStream()) {
      processOutput = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    }
    Pattern pattern = Pattern.compile(".*Using Scala version (.*),.*");
    Matcher matcher = pattern.matcher(processOutput);
    if (matcher.find()) {
      String scalaVersion = matcher.group(1);
      if (scalaVersion.startsWith("2.10")) {
        return "2.10";
      } else if (scalaVersion.startsWith("2.11")) {
        return "2.11";
      } else if (scalaVersion.startsWith("2.12")) {
        return "2.12";
      } else {
        throw new Exception("Unsupported scala version: " + scalaVersion);
      }
    } else {
      return detectSparkScalaVersionByReplClass(sparkHome);
    }
  }

  private String detectSparkScalaVersionByReplClass(String sparkHome) throws Exception {
    File sparkLibFolder = new File(sparkHome + "/lib");
    if (sparkLibFolder.exists()) {
      // spark 1.6 if spark/lib exists
      File[] sparkAssemblyJars = new File(sparkHome + "/lib").listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.contains("spark-assembly");
        }
      });
      if (sparkAssemblyJars.length == 0) {
        throw new Exception("No spark assembly file found in SPARK_HOME: " + sparkHome);
      }
      if (sparkAssemblyJars.length > 1) {
        throw new Exception("Multiple spark assembly file found in SPARK_HOME: " + sparkHome);
      }
      try (URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{sparkAssemblyJars[0].toURI().toURL()});){
        urlClassLoader.loadClass("org.apache.spark.repl.SparkCommandLine");
        return "2.10";
      } catch (ClassNotFoundException e) {
        return "2.11";
      }
    } else {
      // spark 2.x if spark/lib doesn't exists
      File sparkJarsFolder = new File(sparkHome + "/jars");
      boolean sparkRepl211Exists =
              Stream.of(sparkJarsFolder.listFiles()).anyMatch(file -> file.getName().contains("spark-repl_2.11"));
      if (sparkRepl211Exists) {
        return "2.11";
      } else {
        return "2.10";
      }
    }
  }

  /**
   * get environmental variable in the following order
   *
   * 1. interpreter setting
   * 2. zeppelin-env.sh
   *
   */
  private String getEnv(String envName) {
    String env = properties.getProperty(envName);
    if (env == null) {
      env = System.getenv(envName);
    }
    return env;
  }

  private boolean isSparkConf(String key, String value) {
    return !StringUtils.isEmpty(key) && key.startsWith("spark.") && !StringUtils.isEmpty(value);
  }

  private void setupPropertiesForPySpark(Properties sparkProperties) {
    if (isYarnMode()) {
      sparkProperties.setProperty("spark.yarn.isPython", "true");
    }
  }

  private void mergeSparkProperty(Properties sparkProperties, String propertyName,
                                  String propertyValue) {
    if (sparkProperties.containsKey(propertyName)) {
      String oldPropertyValue = sparkProperties.getProperty(propertyName);
      sparkProperties.setProperty(propertyName, oldPropertyValue + "," + propertyValue);
    } else {
      sparkProperties.setProperty(propertyName, propertyValue);
    }
  }

  /**
   * Returns cached Spark Master value if it's present, or calculate it
   *
   * Order to look for spark master
   * 1. master in interpreter setting
   * 2. spark.master interpreter setting
   * 3. use local[*]
   * @return Spark Master string
   */
  private String getSparkMaster() {
    if (!sparkMaster.isPresent()) {
      String master = properties.getProperty(SPARK_MASTER_KEY);
      if (master == null) {
        master = properties.getProperty("master");
        if (master == null) {
          String masterEnv = System.getenv("SPARK_MASTER");
          master = (masterEnv == null ? DEFAULT_MASTER : masterEnv);
        }
        properties.put(SPARK_MASTER_KEY, master);
      }
      sparkMaster = Optional.of(master);
    }
    return sparkMaster.get();
  }

  private String getDeployMode() {
    if (getSparkMaster().equals("yarn-client")) {
      return "client";
    } else if (getSparkMaster().equals("yarn-cluster")) {
      return "cluster";
    } else if (getSparkMaster().startsWith("local")) {
      return "client";
    } else {
      String deployMode = properties.getProperty("spark.submit.deployMode");
      if (deployMode == null) {
        throw new RuntimeException("master is set as yarn, but spark.submit.deployMode " +
            "is not specified");
      }
      if (!deployMode.equals("client") && !deployMode.equals("cluster")) {
        throw new RuntimeException("Invalid value for spark.submit.deployMode: " + deployMode);
      }
      return deployMode;
    }
  }

  private boolean isYarnMode() {
    return getSparkMaster().startsWith("yarn");
  }

  private boolean isYarnCluster() {
    return isYarnMode() && "cluster".equalsIgnoreCase(getDeployMode());
  }

  private boolean isYarnClient() {
    return isYarnMode() && "client".equalsIgnoreCase(getDeployMode());
  }
}
