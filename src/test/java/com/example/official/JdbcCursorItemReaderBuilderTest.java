package com.example.official;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseFactory;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.util.ReflectionTestUtils;

import javax.sql.DataSource;
import java.sql.Types;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 스프링 배치 공식 예제
 * https://github.com/spring-projects/spring-batch/blob/main/spring-batch-infrastructure/src/test/java/org/springframework/batch/item/database/builder/JdbcCursorItemReaderBuilderTests.java
 */

public class JdbcCursorItemReaderBuilderTest {

    private DataSource dataSource;
    private ConfigurableApplicationContext context;

    @BeforeEach
    void setUp() {
        this.context = new AnnotationConfigApplicationContext(TestDataSourceConfiguration.class);
        this.dataSource = (DataSource) context.getBean("dataSource");
    }

    @AfterEach
    void tearDown() {
        if(this.context != null) {
            this.context.close();
        }
    }

    @Test
    void Foo데이터를_조회한다() throws Exception {
        JdbcCursorItemReader<Foo> reader = new JdbcCursorItemReaderBuilder<Foo>()
                .name("fooReader")
                .dataSource(this.dataSource)
                .sql("SELECT * FROM FOO ORDER BY FIRST")
                .rowMapper((rs, rowNum) -> {
                    Foo foo = new Foo();
                    foo.setFirst(rs.getInt("FIRST"));
                    foo.setSecond(rs.getString("SECOND"));
                    foo.setThird(rs.getString("THIRD"));

                    return foo;
                }).build();

        ExecutionContext executionContext = new ExecutionContext();
        reader.open(executionContext);

        validateFoo(reader.read(), 1, "2", "3");
        validateFoo(reader.read(), 4, "5", "6");
        validateFoo(reader.read(), 7, "8", "9");

        assertThat(reader.read()).isNull();
    }

    @Test
    void maxRows지정하여_원하는_결과수만큼_조회한다() throws Exception {
        JdbcCursorItemReader<Foo> reader = new JdbcCursorItemReaderBuilder<Foo>()
                .name("fooReader")
                .dataSource(this.dataSource)
                .maxRows(2)
                .saveState(false) // reader가 실패한 지점을 저장하지 못하게해, 무조건 처음부터 다시 읽도록 함, 멀티스레드 환경에서 필수
                .sql("SELECT * FROM FOO ORDER BY FIRST")
                .rowMapper((rs, rowNum) -> {
                    Foo foo = new Foo();
                    foo.setFirst(rs.getInt("FIRST"));
                    foo.setSecond(rs.getString("SECOND"));
                    foo.setThird(rs.getString("THIRD"));

                    return foo;
                }).build();

        ExecutionContext executionContext = new ExecutionContext();
        reader.open(executionContext);

        validateFoo(reader.read(), 1, "2", "3");
        validateFoo(reader.read(), 4, "5", "6");
        assertThat(reader.read()).isNull();

        assertThat(executionContext.size()).isZero(); // reader.close() 자동 해주는데
    }

    @Test
    void queryArguments로_조건파라미터_지정할수있다() throws Exception {
        JdbcCursorItemReader<Foo> reader = new JdbcCursorItemReaderBuilder<Foo>()
                .name("fooReader")
                .dataSource(this.dataSource)
                .sql("SELECT * FROM FOO WHERE FIRST > ? ORDER BY FIRST")
                .queryArguments(List.of(3))
                .rowMapper((rs, rowNum) -> {
                    Foo foo = new Foo();
                    foo.setFirst(rs.getInt("FIRST"));
                    foo.setSecond(rs.getString("SECOND"));
                    foo.setThird(rs.getString("THIRD"));

                    return foo;
                }).build();

        ExecutionContext executionContext = new ExecutionContext();
        reader.open(executionContext);

        validateFoo(reader.read(), 4, "5", "6");
        validateFoo(reader.read(), 7, "8", "9");
        assertThat(reader.read()).isNull();
    }

    @Test
    void queryArguments에_가변인자_지정할수있다() throws Exception {
        JdbcCursorItemReader<Foo> reader = new JdbcCursorItemReaderBuilder<Foo>()
                .name("fooReader")
                .dataSource(this.dataSource)
                .sql("SELECT * FROM FOO WHERE FIRST > ? ORDER BY FIRST")
                .queryArguments(3)
                .rowMapper((rs, rowNum) -> {
                    Foo foo = new Foo();
                    foo.setFirst(rs.getInt("FIRST"));
                    foo.setSecond(rs.getString("SECOND"));
                    foo.setThird(rs.getString("THIRD"));

                    return foo;
                }).build();

        ExecutionContext executionContext = new ExecutionContext();
        reader.open(executionContext);

        validateFoo(reader.read(), 4, "5", "6");
        validateFoo(reader.read(), 7, "8", "9");
        assertThat(reader.read()).isNull();
    }

    @Test
    void queryArguments에_타입과_인자를_Array로_지정할수있다() throws Exception {
        JdbcCursorItemReader<Foo> reader = new JdbcCursorItemReaderBuilder<Foo>()
                .name("fooReader")
                .dataSource(this.dataSource)
                .sql("SELECT * FROM FOO WHERE FIRST > ? ORDER BY FIRST")
                .queryArguments(new Integer[] {3}, new int[] {Types.BIGINT})
                .rowMapper((rs, rowNum) -> {
                    Foo foo = new Foo();
                    foo.setFirst(rs.getInt("FIRST"));
                    foo.setSecond(rs.getString("SECOND"));
                    foo.setThird(rs.getString("THIRD"));

                    return foo;
                }).build();

        ExecutionContext executionContext = new ExecutionContext();
        reader.open(executionContext);

        validateFoo(reader.read(), 4, "5", "6");
        validateFoo(reader.read(), 7, "8", "9");
        assertThat(reader.read()).isNull();
    }

    @Test
    void preparedStatementSetter로_직접_조건절_파라미터_지정할수있다() throws Exception {
        JdbcCursorItemReader<Foo> reader = new JdbcCursorItemReaderBuilder<Foo>()
                .name("fooReader")
                .dataSource(this.dataSource)
                .sql("SELECT * FROM FOO WHERE FIRST > ? ORDER BY FIRST")
                .preparedStatementSetter(ps -> ps.setInt(1, 3))
                .rowMapper((rs, rowNum) -> {
                    Foo foo = new Foo();
                    foo.setFirst(rs.getInt("FIRST"));
                    foo.setSecond(rs.getString("SECOND"));
                    foo.setThird(rs.getString("THIRD"));

                    return foo;
                }).build();

        ExecutionContext executionContext = new ExecutionContext();
        reader.open(executionContext);

        validateFoo(reader.read(), 4, "5", "6");
        validateFoo(reader.read(), 7, "8", "9");
        assertThat(reader.read()).isNull();
    }

    @Test
    void maxItemCount로_전체중_2개의_아이템만_읽는다() throws Exception {
        JdbcCursorItemReader<Foo> reader = new JdbcCursorItemReaderBuilder<Foo>()
                .name("fooReader")
                .dataSource(this.dataSource)
                .sql("SELECT * FROM FOO ORDER BY FIRST")
                .maxItemCount(2)
                .rowMapper((rs, rowNum) -> {
                    Foo foo = new Foo();
                    foo.setFirst(rs.getInt("FIRST"));
                    foo.setSecond(rs.getString("SECOND"));
                    foo.setThird(rs.getString("THIRD"));

                    return foo;
                }).build();

        ExecutionContext executionContext = new ExecutionContext();
        reader.open(executionContext);

        validateFoo(reader.read(), 1, "2", "3");
        validateFoo(reader.read(), 4, "5", "6");
        assertThat(reader.read()).isNull();
    }

    @Test
    void currentItemCount지정하면_특정수의_아이템을_건너뛴다() throws Exception {
        JdbcCursorItemReader<Foo> reader = new JdbcCursorItemReaderBuilder<Foo>()
                .name("fooReader")
                .dataSource(this.dataSource)
                .sql("SELECT * FROM FOO ORDER BY FIRST")
                .currentItemCount(1)
                .rowMapper((rs, rowNum) -> {
                    Foo foo = new Foo();
                    foo.setFirst(rs.getInt("FIRST"));
                    foo.setSecond(rs.getString("SECOND"));
                    foo.setThird(rs.getString("THIRD"));

                    return foo;
                }).build();

        ExecutionContext executionContext = new ExecutionContext();
        reader.open(executionContext);

        validateFoo(reader.read(), 4, "5", "6");
        validateFoo(reader.read(), 7, "8", "9");
        assertThat(reader.read()).isNull();
    }

    // https://docs.spring.io/spring-batch/docs/4.3.6/reference/html/readersAndWriters.html#readersAndWriters
    @Test
    void 기타_설정을_확인한다() {
        JdbcCursorItemReader<Foo> reader = new JdbcCursorItemReaderBuilder<Foo>()
                .dataSource(this.dataSource)
                .name("fooReader")
                .sql("SELECT * FROM FOO ORDER BY FIRST")
                .fetchSize(1) // DB에서 한번에 가져올 row 수 지정
                .queryTimeout(2) // n 초 초과시 DataAccessException
                .ignoreWarnings(true) // default true, SQL 경고를 기록할지, 예외를 발생시킬지 여부 결정
                .driverSupportsAbsolute(true) // default false, ResultSet의 absolute() 지원 여부, jdbc 드라이버의 경우 이 값을 true 추천
                .useSharedExtendedConnection(true) // default false, 각 커밋 후 연결이 닫히고 해제 되는 것을 방지하기 위해 데이터소스를 래핑한다
                .connectionAutoCommit(true)
                .beanRowMapper(Foo.class)
                .build();

        assertThat(ReflectionTestUtils.getField(reader, "fetchSize")).isEqualTo(1);
        assertThat(ReflectionTestUtils.getField(reader, "queryTimeout")).isEqualTo(2);
        assertThat(ReflectionTestUtils.getField(reader, "ignoreWarnings")).isEqualTo(true);
        assertThat(ReflectionTestUtils.getField(reader, "driverSupportsAbsolute")).isEqualTo(true);
        assertThat(ReflectionTestUtils.getField(reader, "useSharedExtendedConnection")).isEqualTo(true);
        assertThat(ReflectionTestUtils.getField(reader, "connectionAutoCommit")).isEqualTo(true);
    }

    /**
     * ResultSet이 RowMapper로 전달되기 대문에 사용자가 직접 ResultSet.next()를 호출할 수 있으며,
     * 이로 인해 reader의 내부 카운터에 문제가 발생 가능하다
     * 이 값을 true로 설정하면 RowMapper 호출 후 커서 위치가 이전과 동일하지 않으면 예외가 발생함
     * default true
     */
    @Test
    void verifyCursorPosition는_기본true이다() {
        JdbcCursorItemReader<Foo> reader = new JdbcCursorItemReaderBuilder<Foo>().dataSource(this.dataSource)
                .name("fooReader")
                .sql("SELECT * FROM FOO ORDER BY FIRST")
                .beanRowMapper(Foo.class)
                .build();
        assertThat(ReflectionTestUtils.getField(reader, "verifyCursorPosition")).isEqualTo(true);
    }

    @Nested
    @DisplayName("Builder 초기화시 유효성 검사")
    class BuilderValidation {

        @Test
        void saveState가_ture일때_name이없으면_IllegalArgumentException() {
            JdbcCursorItemReaderBuilder<Foo> builder = new JdbcCursorItemReaderBuilder<Foo>()
                    .saveState(true);

            assertThatThrownBy(builder::build)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("A name is required when saveState is set to true");
        }

        @Test
        void saveState가_false일때_query없으면_IllegalArgumentException() {
            JdbcCursorItemReaderBuilder<Foo> builder = new JdbcCursorItemReaderBuilder<Foo>()
                    .saveState(false);

            assertThatThrownBy(builder::build)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("A query is required");
        }

        @Test
        void datasource가없으면_IllegalArgumentException() {
            JdbcCursorItemReaderBuilder<Foo> builder = new JdbcCursorItemReaderBuilder<Foo>()
                    .saveState(false)
                    .sql("select 1");

            assertThatThrownBy(builder::build)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("A datasource is required");
        }

        @Test
        void rowmapper가없으면_IllegalArgumentException() {
            JdbcCursorItemReaderBuilder<Foo> builder = new JdbcCursorItemReaderBuilder<Foo>()
                    .saveState(false)
                    .sql("select 1")
                    .dataSource(dataSource);

            assertThatThrownBy(builder::build)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("A rowmapper is required");
        }


    }

    private void validateFoo(Foo item, int first, String second, String third) {
        assertThat(first).isEqualTo(item.getFirst());
        assertThat(second).isEqualTo(item.getSecond());
        assertThat(third).isEqualTo(item.getThird());
    }


    public static class Foo {
        private int first;
        private String second;
        private String third;

        public int getFirst() {
            return first;
        }

        public void setFirst(int first) {
            this.first = first;
        }

        public String getSecond() {
            return second;
        }

        public void setSecond(String second) {
            this.second = second;
        }

        public String getThird() {
            return third;
        }

        public void setThird(String third) {
            this.third = third;
        }
    }

    @TestConfiguration
    public static class TestDataSourceConfiguration {

        private static final String FOO_CREAT_SQL = "CREATE TABLE IF NOT EXISTS FOO(" +
                "ID BIGINT NOT NULL auto_increment, " +
                "FIRST BIGINT, SECOND VARCHAR(5) NOT NULL, " +
                "THIRD VARCHAR(5) NOT NULL, " +
                "primary key (ID));";

        private static final String FOO_INSERT_SQL = "INSERT INTO FOO (FIRST, SECOND, THIRD) VALUES (1, '2', '3'); " +
                "INSERT INTO FOO (FIRST, SECOND, THIRD) VALUES (4, '5', '6'); " +
                "INSERT INTO FOO (FIRST, SECOND, THIRD) VALUES (7, '8', '9'); ";

        @Bean
        public DataSource dataSource() {
            EmbeddedDatabaseFactory databaseFactory = new EmbeddedDatabaseFactory();
            databaseFactory.setDatabaseType(EmbeddedDatabaseType.H2);
            databaseFactory.setGenerateUniqueDatabaseName(true);

            return databaseFactory.getDatabase();
        }

        @Bean
        public DataSourceInitializer initializer(DataSource dataSource) {
            DataSourceInitializer dataSourceInitializer = new DataSourceInitializer();
            dataSourceInitializer.setDataSource(dataSource);

            Resource create = new ByteArrayResource(FOO_CREAT_SQL.getBytes());
            Resource insert = new ByteArrayResource(FOO_INSERT_SQL.getBytes());
            dataSourceInitializer.setDatabasePopulator(new ResourceDatabasePopulator(create, insert));

            return dataSourceInitializer;
        }

    }

}
