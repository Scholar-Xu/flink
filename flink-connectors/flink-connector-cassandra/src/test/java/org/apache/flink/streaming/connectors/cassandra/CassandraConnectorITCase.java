/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.cassandra;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraOutputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoOutputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraRowOutputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraTupleOutputFormat;
import org.apache.flink.batch.connectors.cassandra.CustomCassandraAnnotatedPojo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;
import org.apache.flink.streaming.runtime.operators.WriteAheadSinkTestBase;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.types.Row;
import org.apache.flink.util.DockerImageVersions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.testcontainers.containers.CassandraContainer;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertTrue;

/** IT cases for all cassandra sinks. */
@SuppressWarnings("serial")
@Ignore(value = "Flaky test")
public class CassandraConnectorITCase
        extends WriteAheadSinkTestBase<
                Tuple3<String, Integer, Integer>,
                CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>>> {

    @ClassRule
    public static final CassandraContainer CASSANDRA_CONTAINER = createCassandraContainer();

    private static final int PORT = 9042;

    private static Cluster cluster;
    private static Session session;

    private final ClusterBuilder builderForReading =
            createBuilderWithConsistencyLevel(ConsistencyLevel.ONE);
    // Lower consistency level ANY is only available for writing.
    private final ClusterBuilder builderForWriting =
            createBuilderWithConsistencyLevel(ConsistencyLevel.ANY);

    private ClusterBuilder createBuilderWithConsistencyLevel(ConsistencyLevel consistencyLevel) {
        return new ClusterBuilder() {
            @Override
            protected Cluster buildCluster(Cluster.Builder builder) {
                return builder.addContactPointsWithPorts(
                                new InetSocketAddress(
                                        CASSANDRA_CONTAINER.getHost(),
                                        CASSANDRA_CONTAINER.getMappedPort(PORT)))
                        .withQueryOptions(
                                new QueryOptions()
                                        .setConsistencyLevel(consistencyLevel)
                                        .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL))
                        .withoutJMXReporting()
                        .withoutMetrics()
                        .build();
            }
        };
    }

    private static final String TABLE_NAME_PREFIX = "flink_";
    private static final String TABLE_NAME_VARIABLE = "$TABLE";
    private static final String CREATE_KEYSPACE_QUERY =
            "CREATE KEYSPACE flink WITH replication= {'class':'SimpleStrategy', 'replication_factor':1};";
    private static final String CREATE_TABLE_QUERY =
            "CREATE TABLE flink."
                    + TABLE_NAME_VARIABLE
                    + " (id text PRIMARY KEY, counter int, batch_id int);";
    private static final String INSERT_DATA_QUERY =
            "INSERT INTO flink."
                    + TABLE_NAME_VARIABLE
                    + " (id, counter, batch_id) VALUES (?, ?, ?)";
    private static final String SELECT_DATA_QUERY =
            "SELECT * FROM flink." + TABLE_NAME_VARIABLE + ';';

    private static final Random random = new Random();
    private int tableID;

    private static final ArrayList<Tuple3<String, Integer, Integer>> collection =
            new ArrayList<>(20);
    private static final ArrayList<Row> rowCollection = new ArrayList<>(20);

    private static final TypeInformation[] FIELD_TYPES = {
        BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO
    };

    static {
        for (int i = 0; i < 20; i++) {
            collection.add(new Tuple3<>(UUID.randomUUID().toString(), i, 0));
            rowCollection.add(Row.of(UUID.randomUUID().toString(), i, 0));
        }
    }

    public static CassandraContainer createCassandraContainer() {
        CassandraContainer cassandra = new CassandraContainer(DockerImageVersions.CASSANDRA_3);
        cassandra.withJmxReporting(false);
        return cassandra;
    }

    @BeforeClass
    public static void startAndInitializeCassandra() {
        CASSANDRA_CONTAINER.start();
        cluster = CASSANDRA_CONTAINER.getCluster();
        session = cluster.connect();
        session.execute(CREATE_KEYSPACE_QUERY);
        session.execute(
                CREATE_TABLE_QUERY.replace(TABLE_NAME_VARIABLE, TABLE_NAME_PREFIX + "initial"));
    }

    @Before
    public void createTable() {
        tableID = random.nextInt(Integer.MAX_VALUE);
        session.execute(injectTableName(CREATE_TABLE_QUERY));
    }

    @AfterClass
    public static void closeCassandra() {
        if (session != null) {
            session.close();
        }
        if (cluster != null) {
            cluster.close();
        }
        CASSANDRA_CONTAINER.stop();
    }

    // ------------------------------------------------------------------------
    //  Exactly-once Tests
    // ------------------------------------------------------------------------

    @Override
    protected CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> createSink()
            throws Exception {
        return new CassandraTupleWriteAheadSink<>(
                injectTableName(INSERT_DATA_QUERY),
                TypeExtractor.getForObject(new Tuple3<>("", 0, 0))
                        .createSerializer(new ExecutionConfig()),
                builderForReading,
                new CassandraCommitter(builderForReading));
    }

    @Override
    protected TupleTypeInfo<Tuple3<String, Integer, Integer>> createTypeInfo() {
        return TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class, Integer.class);
    }

    @Override
    protected Tuple3<String, Integer, Integer> generateValue(int counter, int checkpointID) {
        return new Tuple3<>(UUID.randomUUID().toString(), counter, checkpointID);
    }

    @Override
    protected void verifyResultsIdealCircumstances(
            CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink) {

        ResultSet result = session.execute(injectTableName(SELECT_DATA_QUERY));
        ArrayList<Integer> list = new ArrayList<>();
        for (int x = 1; x <= 60; x++) {
            list.add(x);
        }

        for (com.datastax.driver.core.Row s : result) {
            list.remove(new Integer(s.getInt("counter")));
        }
        Assert.assertTrue(
                "The following ID's were not found in the ResultSet: " + list.toString(),
                list.isEmpty());
    }

    @Override
    protected void verifyResultsDataPersistenceUponMissedNotify(
            CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink) {

        ResultSet result = session.execute(injectTableName(SELECT_DATA_QUERY));
        ArrayList<Integer> list = new ArrayList<>();
        for (int x = 1; x <= 60; x++) {
            list.add(x);
        }

        for (com.datastax.driver.core.Row s : result) {
            list.remove(new Integer(s.getInt("counter")));
        }
        Assert.assertTrue(
                "The following ID's were not found in the ResultSet: " + list.toString(),
                list.isEmpty());
    }

    @Override
    protected void verifyResultsDataDiscardingUponRestore(
            CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink) {

        ResultSet result = session.execute(injectTableName(SELECT_DATA_QUERY));
        ArrayList<Integer> list = new ArrayList<>();
        for (int x = 1; x <= 20; x++) {
            list.add(x);
        }
        for (int x = 41; x <= 60; x++) {
            list.add(x);
        }

        for (com.datastax.driver.core.Row s : result) {
            list.remove(new Integer(s.getInt("counter")));
        }
        Assert.assertTrue(
                "The following ID's were not found in the ResultSet: " + list.toString(),
                list.isEmpty());
    }

    @Override
    protected void verifyResultsWhenReScaling(
            CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink,
            int startElementCounter,
            int endElementCounter) {

        // IMPORTANT NOTE:
        //
        // for cassandra we always have to start from 1 because
        // all operators will share the same final db

        ArrayList<Integer> expected = new ArrayList<>();
        for (int i = 1; i <= endElementCounter; i++) {
            expected.add(i);
        }

        ArrayList<Integer> actual = new ArrayList<>();
        ResultSet result = session.execute(injectTableName(SELECT_DATA_QUERY));

        for (com.datastax.driver.core.Row s : result) {
            actual.add(s.getInt("counter"));
        }

        Collections.sort(actual);
        Assert.assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void testCassandraCommitter() throws Exception {
        String jobID = new JobID().toString();
        CassandraCommitter cc1 = new CassandraCommitter(builderForReading, "flink_auxiliary_cc");
        cc1.setJobId(jobID);
        cc1.setOperatorId("operator");

        CassandraCommitter cc2 = new CassandraCommitter(builderForReading, "flink_auxiliary_cc");
        cc2.setJobId(jobID);
        cc2.setOperatorId("operator");

        CassandraCommitter cc3 = new CassandraCommitter(builderForReading, "flink_auxiliary_cc");
        cc3.setJobId(jobID);
        cc3.setOperatorId("operator1");

        cc1.createResource();

        cc1.open();
        cc2.open();
        cc3.open();

        Assert.assertFalse(cc1.isCheckpointCommitted(0, 1));
        Assert.assertFalse(cc2.isCheckpointCommitted(1, 1));
        Assert.assertFalse(cc3.isCheckpointCommitted(0, 1));

        cc1.commitCheckpoint(0, 1);
        Assert.assertTrue(cc1.isCheckpointCommitted(0, 1));
        // verify that other sub-tasks aren't affected
        Assert.assertFalse(cc2.isCheckpointCommitted(1, 1));
        // verify that other tasks aren't affected
        Assert.assertFalse(cc3.isCheckpointCommitted(0, 1));

        Assert.assertFalse(cc1.isCheckpointCommitted(0, 2));

        cc1.close();
        cc2.close();
        cc3.close();

        cc1 = new CassandraCommitter(builderForReading, "flink_auxiliary_cc");
        cc1.setJobId(jobID);
        cc1.setOperatorId("operator");

        cc1.open();

        // verify that checkpoint data is not destroyed within open/close and not reliant on
        // internally cached data
        Assert.assertTrue(cc1.isCheckpointCommitted(0, 1));
        Assert.assertFalse(cc1.isCheckpointCommitted(0, 2));

        cc1.close();
    }

    // ------------------------------------------------------------------------
    //  At-least-once Tests
    // ------------------------------------------------------------------------

    @Test
    public void testCassandraTupleAtLeastOnceSink() throws Exception {
        CassandraTupleSink<Tuple3<String, Integer, Integer>> sink =
                new CassandraTupleSink<>(injectTableName(INSERT_DATA_QUERY), builderForWriting);
        try {
            sink.open(new Configuration());
            for (Tuple3<String, Integer, Integer> value : collection) {
                sink.send(value);
            }
        } finally {
            sink.close();
        }

        ResultSet rs = session.execute(injectTableName(SELECT_DATA_QUERY));
        Assert.assertEquals(20, rs.all().size());
    }

    @Test
    public void testCassandraRowAtLeastOnceSink() throws Exception {
        CassandraRowSink sink =
                new CassandraRowSink(
                        FIELD_TYPES.length, injectTableName(INSERT_DATA_QUERY), builderForWriting);
        try {
            sink.open(new Configuration());
            for (Row value : rowCollection) {
                sink.send(value);
            }
        } finally {
            sink.close();
        }

        ResultSet rs = session.execute(injectTableName(SELECT_DATA_QUERY));
        Assert.assertEquals(20, rs.all().size());
    }

    @Test
    public void testCassandraPojoAtLeastOnceSink() throws Exception {
        session.execute(CREATE_TABLE_QUERY.replace(TABLE_NAME_VARIABLE, "test"));

        CassandraPojoSink<Pojo> sink = new CassandraPojoSink<>(Pojo.class, builderForWriting);
        try {
            sink.open(new Configuration());
            for (int x = 0; x < 20; x++) {
                sink.send(new Pojo(UUID.randomUUID().toString(), x, 0));
            }
        } finally {
            sink.close();
        }

        ResultSet rs = session.execute(SELECT_DATA_QUERY.replace(TABLE_NAME_VARIABLE, "test"));
        Assert.assertEquals(20, rs.all().size());
    }

    @Test
    public void testCassandraPojoNoAnnotatedKeyspaceAtLeastOnceSink() throws Exception {
        session.execute(
                CREATE_TABLE_QUERY.replace(TABLE_NAME_VARIABLE, "testPojoNoAnnotatedKeyspace"));

        CassandraPojoSink<PojoNoAnnotatedKeyspace> sink =
                new CassandraPojoSink<>(PojoNoAnnotatedKeyspace.class, builderForWriting, "flink");
        try {
            sink.open(new Configuration());
            for (int x = 0; x < 20; x++) {
                sink.send(new PojoNoAnnotatedKeyspace(UUID.randomUUID().toString(), x, 0));
            }

        } finally {
            sink.close();
        }
        ResultSet rs =
                session.execute(
                        SELECT_DATA_QUERY.replace(
                                TABLE_NAME_VARIABLE, "testPojoNoAnnotatedKeyspace"));
        Assert.assertEquals(20, rs.all().size());
    }

    @Test
    public void testCassandraTableSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Row> source = env.fromCollection(rowCollection);

        tEnv.createTemporaryView("testFlinkTable", source);
        ((TableEnvironmentInternal) tEnv)
                .registerTableSinkInternal(
                        "cassandraTable",
                        new CassandraAppendTableSink(
                                        builderForWriting, injectTableName(INSERT_DATA_QUERY))
                                .configure(
                                        new String[] {"f0", "f1", "f2"},
                                        new TypeInformation[] {
                                            Types.STRING, Types.INT, Types.INT
                                        }));

        tEnv.sqlQuery("select * from testFlinkTable").executeInsert("cassandraTable").await();

        ResultSet rs = session.execute(injectTableName(SELECT_DATA_QUERY));

        // validate that all input was correctly written to Cassandra
        List<Row> input = new ArrayList<>(rowCollection);
        List<com.datastax.driver.core.Row> output = rs.all();
        for (com.datastax.driver.core.Row o : output) {
            Row cmp = new Row(3);
            cmp.setField(0, o.getString(0));
            cmp.setField(1, o.getInt(2));
            cmp.setField(2, o.getInt(1));
            Assert.assertTrue(
                    "Row " + cmp + " was written to Cassandra but not in input.",
                    input.remove(cmp));
        }
        Assert.assertTrue(
                "The input data was not completely written to Cassandra", input.isEmpty());
    }

    @Test
    public void testCassandraBatchPojoFormat() throws Exception {

        session.execute(
                CREATE_TABLE_QUERY.replace(
                        TABLE_NAME_VARIABLE, CustomCassandraAnnotatedPojo.TABLE_NAME));

        OutputFormat<CustomCassandraAnnotatedPojo> sink =
                new CassandraPojoOutputFormat<>(
                        builderForWriting,
                        CustomCassandraAnnotatedPojo.class,
                        () -> new Mapper.Option[] {Mapper.Option.saveNullFields(true)});

        List<CustomCassandraAnnotatedPojo> customCassandraAnnotatedPojos =
                IntStream.range(0, 20)
                        .mapToObj(
                                x ->
                                        new CustomCassandraAnnotatedPojo(
                                                UUID.randomUUID().toString(), x, 0))
                        .collect(Collectors.toList());
        try {
            sink.configure(new Configuration());
            sink.open(0, 1);
            for (CustomCassandraAnnotatedPojo customCassandraAnnotatedPojo :
                    customCassandraAnnotatedPojos) {
                sink.writeRecord(customCassandraAnnotatedPojo);
            }
        } finally {
            sink.close();
        }
        ResultSet rs =
                session.execute(
                        SELECT_DATA_QUERY.replace(
                                TABLE_NAME_VARIABLE, CustomCassandraAnnotatedPojo.TABLE_NAME));
        Assert.assertEquals(20, rs.all().size());

        InputFormat<CustomCassandraAnnotatedPojo, InputSplit> source =
                new CassandraPojoInputFormat<>(
                        SELECT_DATA_QUERY.replace(TABLE_NAME_VARIABLE, "batches"),
                        builderForReading,
                        CustomCassandraAnnotatedPojo.class);
        List<CustomCassandraAnnotatedPojo> result = new ArrayList<>();

        try {
            source.configure(new Configuration());
            source.open(null);
            while (!source.reachedEnd()) {
                CustomCassandraAnnotatedPojo temp = source.nextRecord(null);
                result.add(temp);
            }
        } finally {
            source.close();
        }

        Assert.assertEquals(20, result.size());
        result.sort(Comparator.comparingInt(CustomCassandraAnnotatedPojo::getCounter));
        customCassandraAnnotatedPojos.sort(
                Comparator.comparingInt(CustomCassandraAnnotatedPojo::getCounter));

        assertThat(result, samePropertyValuesAs(customCassandraAnnotatedPojos));
    }

    @Test
    public void testCassandraBatchTupleFormat() throws Exception {
        OutputFormat<Tuple3<String, Integer, Integer>> sink =
                new CassandraOutputFormat<>(injectTableName(INSERT_DATA_QUERY), builderForWriting);
        try {
            sink.configure(new Configuration());
            sink.open(0, 1);
            for (Tuple3<String, Integer, Integer> value : collection) {
                sink.writeRecord(value);
            }
        } finally {
            sink.close();
        }

        sink =
                new CassandraTupleOutputFormat<>(
                        injectTableName(INSERT_DATA_QUERY), builderForWriting);
        try {
            sink.configure(new Configuration());
            sink.open(0, 1);
            for (Tuple3<String, Integer, Integer> value : collection) {
                sink.writeRecord(value);
            }
        } finally {
            sink.close();
        }

        InputFormat<Tuple3<String, Integer, Integer>, InputSplit> source =
                new CassandraInputFormat<>(injectTableName(SELECT_DATA_QUERY), builderForReading);
        List<Tuple3<String, Integer, Integer>> result = new ArrayList<>();
        try {
            source.configure(new Configuration());
            source.open(null);
            while (!source.reachedEnd()) {
                result.add(source.nextRecord(new Tuple3<String, Integer, Integer>()));
            }
        } finally {
            source.close();
        }

        Assert.assertEquals(20, result.size());
    }

    @Test
    public void testCassandraBatchRowFormat() throws Exception {
        OutputFormat<Row> sink =
                new CassandraRowOutputFormat(injectTableName(INSERT_DATA_QUERY), builderForWriting);
        try {
            sink.configure(new Configuration());
            sink.open(0, 1);
            for (Row value : rowCollection) {
                sink.writeRecord(value);
            }
        } finally {

            sink.close();
        }

        ResultSet rs = session.execute(injectTableName(SELECT_DATA_QUERY));
        List<com.datastax.driver.core.Row> rows = rs.all();
        Assert.assertEquals(rowCollection.size(), rows.size());
    }

    private String injectTableName(String target) {
        return target.replace(TABLE_NAME_VARIABLE, TABLE_NAME_PREFIX + tableID);
    }

    @Test
    public void testCassandraScalaTupleAtLeastOnceSinkBuilderDetection() throws Exception {
        Class<scala.Tuple1<String>> c =
                (Class<scala.Tuple1<String>>) new scala.Tuple1<>("hello").getClass();
        Seq<TypeInformation<?>> typeInfos =
                JavaConverters.asScalaBufferConverter(
                                Collections.<TypeInformation<?>>singletonList(
                                        BasicTypeInfo.STRING_TYPE_INFO))
                        .asScala();
        Seq<String> fieldNames =
                JavaConverters.asScalaBufferConverter(Collections.singletonList("_1")).asScala();

        CaseClassTypeInfo<scala.Tuple1<String>> typeInfo =
                new CaseClassTypeInfo<scala.Tuple1<String>>(c, null, typeInfos, fieldNames) {
                    @Override
                    public TypeSerializer<scala.Tuple1<String>> createSerializer(
                            ExecutionConfig config) {
                        return null;
                    }
                };

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<scala.Tuple1<String>> input =
                env.fromElements(new scala.Tuple1<>("hello")).returns(typeInfo);

        CassandraSink.CassandraSinkBuilder<scala.Tuple1<String>> sinkBuilder =
                CassandraSink.addSink(input);
        assertTrue(sinkBuilder instanceof CassandraSink.CassandraScalaProductSinkBuilder);
    }

    @Test
    public void testCassandraScalaTupleAtLeastSink() throws Exception {
        CassandraScalaProductSink<scala.Tuple3<String, Integer, Integer>> sink =
                new CassandraScalaProductSink<>(
                        injectTableName(INSERT_DATA_QUERY), builderForWriting);

        List<scala.Tuple3<String, Integer, Integer>> scalaTupleCollection = new ArrayList<>(20);
        for (int i = 0; i < 20; i++) {
            scalaTupleCollection.add(new scala.Tuple3<>(UUID.randomUUID().toString(), i, 0));
        }
        try {
            sink.open(new Configuration());
            for (scala.Tuple3<String, Integer, Integer> value : scalaTupleCollection) {
                sink.invoke(value, SinkContextUtil.forTimestamp(0));
            }
        } finally {
            sink.close();
        }

        ResultSet rs = session.execute(injectTableName(SELECT_DATA_QUERY));
        List<com.datastax.driver.core.Row> rows = rs.all();
        Assert.assertEquals(scalaTupleCollection.size(), rows.size());

        for (com.datastax.driver.core.Row row : rows) {
            scalaTupleCollection.remove(
                    new scala.Tuple3<>(
                            row.getString("id"), row.getInt("counter"), row.getInt("batch_id")));
        }
        Assert.assertEquals(0, scalaTupleCollection.size());
    }

    @Test
    public void testCassandraScalaTuplePartialColumnUpdate() throws Exception {
        CassandraSinkBaseConfig config =
                CassandraSinkBaseConfig.newBuilder().setIgnoreNullFields(true).build();
        CassandraScalaProductSink<scala.Tuple3<String, Integer, Integer>> sink =
                new CassandraScalaProductSink<>(
                        injectTableName(INSERT_DATA_QUERY), builderForWriting, config);

        String id = UUID.randomUUID().toString();
        Integer counter = 1;
        Integer batchId = 0;

        // Send partial records across multiple request
        scala.Tuple3<String, Integer, Integer> scalaTupleRecordFirst =
                new scala.Tuple3<>(id, counter, null);
        scala.Tuple3<String, Integer, Integer> scalaTupleRecordSecond =
                new scala.Tuple3<>(id, null, batchId);

        try {
            sink.open(new Configuration());
            sink.invoke(scalaTupleRecordFirst, SinkContextUtil.forTimestamp(0));
            sink.invoke(scalaTupleRecordSecond, SinkContextUtil.forTimestamp(0));
        } finally {
            sink.close();
        }

        ResultSet rs = session.execute(injectTableName(SELECT_DATA_QUERY));
        List<com.datastax.driver.core.Row> rows = rs.all();
        Assert.assertEquals(1, rows.size());
        // Since nulls are ignored, we should be reading one complete record
        for (com.datastax.driver.core.Row row : rows) {
            Assert.assertEquals(
                    new scala.Tuple3<>(id, counter, batchId),
                    new scala.Tuple3<>(
                            row.getString("id"), row.getInt("counter"), row.getInt("batch_id")));
        }
    }
}
