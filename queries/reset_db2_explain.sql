drop table SYSTOOLS.EXPLAIN_STATEMENT;

create table SYSTOOLS.EXPLAIN_STATEMENT
(
    EXPLAIN_REQUESTER VARCHAR(128)  not null,
    EXPLAIN_TIME      TIMESTAMP(6)  not null,
    SOURCE_NAME       VARCHAR(128)  not null,
    SOURCE_SCHEMA     VARCHAR(128)  not null,
    SOURCE_VERSION    VARCHAR(64)   not null,
    EXPLAIN_LEVEL     CHARACTER(1)  not null,
    STMTNO            INTEGER       not null,
    SECTNO            INTEGER       not null,
    QUERYNO           INTEGER       not null,
    QUERYTAG          CHARACTER(20) not null,
    STATEMENT_TYPE    CHARACTER(2)  not null,
    UPDATABLE         CHARACTER(1)  not null,
    DELETABLE         CHARACTER(1)  not null,
    TOTAL_COST        DOUBLE        not null,
    STATEMENT_TEXT    CLOB(2097152) not null,
    SNAPSHOT          BLOB(10485760),
    QUERY_DEGREE      INTEGER       not null,
    primary key (EXPLAIN_REQUESTER, EXPLAIN_TIME, SOURCE_NAME, SOURCE_SCHEMA, SOURCE_VERSION, EXPLAIN_LEVEL, STMTNO,
                 SECTNO),
    foreign key (EXPLAIN_REQUESTER, EXPLAIN_TIME, SOURCE_NAME, SOURCE_SCHEMA,
                 SOURCE_VERSION) references SYSTOOLS.EXPLAIN_INSTANCE
        on delete cascade
);

create index SYSTOOLS.STMT_I1
    on SYSTOOLS.EXPLAIN_STATEMENT (EXPLAIN_TIME, EXPLAIN_LEVEL, STMTNO, SECTNO);

