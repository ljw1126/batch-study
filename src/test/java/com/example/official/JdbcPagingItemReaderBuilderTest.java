package com.example.official;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 스프링 배치 공식 예제
 * https://github.com/spring-projects/spring-batch/blob/main/spring-batch-infrastructure/src/test/java/org/springframework/batch/item/database/builder/JdbcPagingItemReaderBuilderTests.java
 */
public class JdbcPagingItemReaderBuilderTest {
    private static final Map<String, Order> DESC_ID = Map.of("ID", Order.DESCENDING);
    private DataSource dataSource;
    private ConfigurableApplicationContext context;

    @BeforeEach
    void setUp() {
        this.context = new AnnotationConfigApplicationContext(TestDataSourceConfiguration.class);
        this.dataSource = (DataSource) this.context.getBean("dataSource");
    }

    @AfterEach
    void tearDown() {
        if(this.context != null) {
            this.context.close();
        }
    }


    @Test
    void sqlPagingQueryProviderFactoryBean사용하여_아이템을조회한다() throws Exception {
        SqlPagingQueryProviderFactoryBean provider = new SqlPagingQueryProviderFactoryBean();
        provider.setDataSource(this.dataSource);
        provider.setSelectClause("SELECT ID, FIRST, SECOND, THIRD");
        provider.setFromClause("FOO");
        provider.setSortKeys(DESC_ID);

        JdbcPagingItemReader<Foo> reader = new JdbcPagingItemReaderBuilder<Foo>()
                .name("fooReader")
                .currentItemCount(1) // 현재 위치, skip 용도
                .dataSource(this.dataSource)
                .queryProvider(provider.getObject())
                .fetchSize(2) // 조회시 2건을 데이터베이스에서 가져옴
                .maxItemCount(2) // 최대 2건 item 조회
                .rowMapper((rs, rowNum) -> new Foo(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getString(4)))
                .build();
        reader.afterPropertiesSet(); // 필수

        ExecutionContext executionContext = new ExecutionContext();
        reader.open(executionContext);

        Foo item = reader.read();
        assertThat(reader.read()).isNull(); // 2건을 fetch 로 조회했는데, skip 1(currentItemCount) 해서 null 반환

        reader.update(executionContext);
        reader.close();

        assertThat(item).extracting("id", "first", "second", "third")
                        .containsExactly(4, 10, "11", "12");

        assertThat(executionContext.size()).isEqualTo(2); // 현재 위치 나타내는 정보, 기타 필요한 상태 정보

        assertThat(ReflectionTestUtils.getField(reader, "fetchSize")).isEqualTo(2);
    }

    @Test
    void queryProvider없이_설정하여_데이터조회한다() throws Exception {
        JdbcPagingItemReader<Foo> reader = new JdbcPagingItemReaderBuilder<Foo>()
                .name("fooReader")
                .currentItemCount(1) // 현재 위치, skip 용도
                .dataSource(this.dataSource)
                .maxItemCount(2) // 최대 2건 item 조회
                .selectClause("SELECT ID, FIRST, SECOND, THIRD")
                .fromClause("FOO")
                .sortKeys(DESC_ID)
                .rowMapper((rs, rowNum) -> new Foo(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getString(4)))
                .build();
        reader.afterPropertiesSet(); // 필수

        reader.open(new ExecutionContext());

        Foo item = reader.read();
        assertThat(reader.read()).isNull(); // 2건을 fetch 로 조회했는데, skip 1(currentItemCount) 해서 null 반환

        assertThat(item).extracting("id", "first", "second", "third")
                .containsExactly(4, 10, "11", "12");
    }


    @Test
    void pageSize설정하여_데이터조회한다() throws Exception {
        JdbcPagingItemReader<Foo> reader = new JdbcPagingItemReaderBuilder<Foo>()
                .name("fooReader")
                .dataSource(this.dataSource)
                .maxItemCount(2) // 총 2건 item 조회
                .pageSize(1) // select 쿼리별로 1건만 조회, 그래서 총 2번 나감
                .selectClause("SELECT ID, FIRST, SECOND, THIRD")
                .fromClause("FOO")
                .sortKeys(DESC_ID)
                .rowMapper((rs, rowNum) -> new Foo(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getString(4)))
                .build();
        reader.afterPropertiesSet(); // 필수

        Foo item1 = reader.read();
        Foo item2 = reader.read();

        assertThat(reader.read()).isNull();

        assertThat(item1).extracting("id", "first", "second", "third")
                .containsExactly(5, 13, "14", "15");

        assertThat(item2).extracting("id", "first", "second", "third")
                .containsExactly(4, 10, "11", "12");
    }

    // 예외 발생시 처음부터 read 하기 위햇
    @Test
    void saveState를_false로_설정하면_ExecutionContext에_상태저장하지않는다() throws Exception {
        JdbcPagingItemReader<Foo> reader = new JdbcPagingItemReaderBuilder<Foo>()
                .name("fooReader")
                .dataSource(this.dataSource)
                .maxItemCount(2)
                .pageSize(1)
                .selectClause("SELECT ID, FIRST, SECOND, THIRD")
                .fromClause("FOO")
                .sortKeys(DESC_ID)
                .saveState(false) // 추가
                .rowMapper((rs, rowNum) -> new Foo(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getString(4)))
                .build();
        reader.afterPropertiesSet();

        ExecutionContext executionContext = new ExecutionContext();
        reader.open(executionContext);

        Foo item1 = reader.read();
        Foo item2 = reader.read();
        assertThat(reader.read()).isNull();

        reader.update(executionContext);
        reader.close();

        assertThat(executionContext.size()).isEqualTo(0);
    }

    @Test
    void parameterValues로_조건절_파라미터를_설정할수있다() throws Exception {
        Map<String, Object> parameterValues = Map.of("min", 1, "max", 10);

        JdbcPagingItemReader<Foo> reader = new JdbcPagingItemReaderBuilder<Foo>()
                .name("fooReader")
                .dataSource(this.dataSource)
                .maxItemCount(1)
                .pageSize(1)
                .selectClause("SELECT ID, FIRST, SECOND, THIRD")
                .fromClause("FOO")
                .whereClause("FIRST > :min AND FIRST < :max")
                .sortKeys(DESC_ID)
                .parameterValues(parameterValues)
                .rowMapper((rs, rowNum) -> new Foo(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getString(4)))
                .build();
        reader.afterPropertiesSet();

        reader.open(new ExecutionContext());
        Foo item = reader.read();

        assertThat(reader.read()).isNull();
        assertThat(item).extracting("id", "first", "second", "third")
                .containsExactly(3, 7, "8", "9");
    }

    @Test
    void beanRowMapper에_클래스지정하면_mapping된다() throws Exception {
        JdbcPagingItemReader<Foo> reader = new JdbcPagingItemReaderBuilder<Foo>()
                .name("fooReader")
                .dataSource(this.dataSource)
                .currentItemCount(1) // 첫번쨰 item을 skip한다
                .maxItemCount(2) // 최대 2개까지 item 조회한다
                .selectClause("SELECT ID, FIRST, SECOND, THIRD")
                .fromClause("FOO")
                .sortKeys(DESC_ID)
                .beanRowMapper(Foo.class) // 기본 생성자 필수, BeanPropertyRowMapper생성
                .build();
        reader.afterPropertiesSet();

        Foo item = reader.read();
        assertThat(reader.read()).isNull(); // 2건을 fetch 로 조회했는데, skip 1(currentItemCount) 해서 null 반환

        assertThat(item).extracting("id", "first", "second", "third")
                .containsExactly(5, 13, "14", "15");
    }

    @Nested
    @DisplayName("Builder 초기화시 유효성 검사")
    class BuilderValidation {

        @Test
        void pageSize가_0이하면_안된다() {
            JdbcPagingItemReaderBuilder<Foo> builder = new JdbcPagingItemReaderBuilder<Foo>()
                    .pageSize(-1);

            assertThatThrownBy(builder::build)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("pageSize must be greater than zero");
        }

        @Test
        void datasource_null허용하지않는다() {
            JdbcPagingItemReaderBuilder<Foo> builder = new JdbcPagingItemReaderBuilder<Foo>()
                    .pageSize(10);

            assertThatThrownBy(builder::build)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("dataSource is required");
        }

        @Test
        void saveState가_true일때_name이_null이면_안된다() {
            JdbcPagingItemReaderBuilder<Foo> builder = new JdbcPagingItemReaderBuilder<Foo>()
                    .pageSize(10)
                    .dataSource(dataSource);

            assertThatThrownBy(builder::build)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("A name is required when saveState is set to true");
        }

        @Test
        void queryProvider가_null인경우_selectClause_null이면안된다() {
            JdbcPagingItemReaderBuilder<Foo> builder = new JdbcPagingItemReaderBuilder<Foo>()
                    .saveState(false)
                    .pageSize(10)
                    .dataSource(dataSource);

            assertThatThrownBy(builder::build)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("selectClause is required when not providing a PagingQueryProvider");
        }

        @Test
        void queryProvider가_null인경우_fromClause_null이면안된다() {
            JdbcPagingItemReaderBuilder<Foo> builder = new JdbcPagingItemReaderBuilder<Foo>()
                    .saveState(false)
                    .pageSize(10)
                    .dataSource(dataSource)
                    .selectClause("select * ");

            assertThatThrownBy(builder::build)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("fromClause is required when not providing a PagingQueryProvider");
        }

        @Test
        void queryProvider가_null인경우_sortKey_empty이면안된다() {
            JdbcPagingItemReaderBuilder<Foo> builder = new JdbcPagingItemReaderBuilder<Foo>()
                    .saveState(false)
                    .pageSize(10)
                    .dataSource(dataSource)
                    .selectClause("select * ")
                    .fromClause("FOO");

            assertThatThrownBy(builder::build)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("sortKeys are required when not providing a PagingQueryProvider");
        }
    }

    public static class Foo {
        private int id;
        private int first;
        private String second;
        private String third;

        public Foo() {
        }

        public Foo(int id, int first, String second, String third) {
            this.id = id;
            this.first = first;
            this.second = second;
            this.third = third;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

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
                "FIRST BIGINT, " +
                "SECOND VARCHAR(5) NOT NULL, " +
                "THIRD VARCHAR(5) NOT NULL, " +
                "primary key (ID));";

        private static final String FOO_INSERT_SQL = "INSERT INTO FOO (FIRST, SECOND, THIRD) VALUES (1, '2', '3'); " +
                "INSERT INTO FOO (FIRST, SECOND, THIRD) VALUES (4, '5', '6'); " +
                "INSERT INTO FOO (FIRST, SECOND, THIRD) VALUES (7, '8', '9'); " +
                "INSERT INTO FOO (FIRST, SECOND, THIRD) VALUES (10, '11', '12'); " +
                "INSERT INTO FOO (FIRST, SECOND, THIRD) VALUES (13, '14', '15'); ";

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
