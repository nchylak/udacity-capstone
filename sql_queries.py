import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.config')

# DROP TABLES

staging_reviews_table_drop = "DROP TABLE IF EXISTS staging_reviews;"
staging_titles_table_drop = "DROP TABLE IF EXISTS staging_titles;"
staging_principals_table_drop = "DROP TABLE IF EXISTS staging_principals;"
staging_names_table_drop = "DROP TABLE IF EXISTS staging_names;"
reviews_table_drop = "DROP TABLE IF EXISTS reviews;"
titles_table_drop = "DROP TABLE IF EXISTS titles;"
principals_table_drop = "DROP TABLE IF EXISTS principals;"
dates_table_drop = "DROP TABLE IF EXISTS dates;"

# CREATE TABLES

staging_reviews_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_reviews (
        review_id char(9),
        reviewer varchar(100),
        movie varchar(250),
        rating int,
        review_summary varchar(max),
        review_date date,
        spoiler_tag int,
        review_detail varchar(max),
        helpful varchar(20)
    );
""")

staging_titles_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_titles (
        tconst char(9),
        titleType varchar(50),
        primaryTitle varchar(250),
        originalTitle varchar(250),
        isAdult bool,
        startYear int,
        endYear int,
        runtimeMinutes bigint,
        genres varchar(250)
    );
""")

staging_principals_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_principals (
        tconst char(9),
        ordering int,
        nconst char(9),
        category varchar(50),
        job varchar(50),
        characters varchar(150)
    );
""")

staging_names_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_names (
        nconst char(9),
        primaryName varchar(50),
        birthYear int,
        deathYear int,
        primaryProfession varchar(150),
        knownForTitles varchar(150)
    );
""")

reviews_table_create = ("""
    CREATE TABLE IF NOT EXISTS reviews (
        review_id char(9) PRIMARY KEY,
        title_id char(9) REFERENCES titles(title_id),
        review_date date REFERENCES dates(review_date) NOT NULL,
        rating int,
        review_text varchar(max))
        DISTSTYLE EVEN;
""")

titles_table_create = ("""
    CREATE TABLE IF NOT EXISTS titles (
        title_id char(9) PRIMARY KEY,
        title varchar(250) NOT NULL,
        title_type varchar(50),
        year int,
        length bigint,
        genres varchar(250))
        DISTSTYLE ALL;
""")

principals_table_create = ("""
    CREATE TABLE IF NOT EXISTS principals (
        title_id char(9) REFERENCES titles(title_id) NOT NULL,
        ordering int NOT NULL,
        name varchar(50) NOT NULL,
        category varchar(50),
        PRIMARY KEY (title_id, ordering))
        DISTSTYLE ALL;
""")

dates_table_create = ("""
    CREATE TABLE IF NOT EXISTS dates (
        review_date date PRIMARY KEY,
        day int,
        week int,
        month int,
        year int,
        weekday int)
        DISTSTYLE ALL;
""")

# # STAGING TABLES

staging_reviews_copy = (f"""
    COPY staging_reviews FROM {config['S3']['IMDB_REVIEWS']}
        CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
        REGION 'us-west-2'
        FORMAT as JSON 'auto'
        DATEFORMAT as 'auto'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""")

staging_titles_copy = (f"""
    COPY staging_titles FROM {config['S3']['IMDB_TITLES']}
        CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
        REGION 'us-west-2'
        FORMAT AS CSV
        quote as '*'
        DELIMITER AS '\t'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL IGNOREHEADER 1 MAXERROR 10000;
""")

staging_principals_copy = (f"""
    COPY staging_principals FROM {config['S3']['IMDB_PRINCIPALS']}
        CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
        REGION 'us-west-2'
        FORMAT AS CSV
        quote as '*'
        DELIMITER AS '\t'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL IGNOREHEADER 1 MAXERROR 10000;
""")

staging_names_copy = (f"""
    COPY staging_names FROM {config['S3']['IMDB_NAMES']}
        CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
        REGION 'us-west-2'
        FORMAT AS CSV
        quote as '*'
        DELIMITER AS '\t'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL IGNOREHEADER 1 MAXERROR 10000;
""")

# # FINAL TABLES

reviews_table_insert = ("""
    INSERT INTO reviews 
        (WITH reviews_w_title AS (
        SELECT 
            *, 
            REGEXP_SUBSTR(staging_reviews.movie, '^[^\(]+', 1, 1, 'i')::varchar(50) AS title, 
            REGEXP_SUBSTR(staging_reviews.movie, '(\()+[12][0-9]{3})', 1, 1, 'i') AS title_year
        FROM staging_reviews
    )
    SELECT
        r.review_id,
        t.title_id,
        r.review_date,
        r.rating,
        r.review_detail
    FROM reviews_w_title r 
    LEFT JOIN titles t
        ON t.title = r.title
        AND t."year"::char(4) = r.title_year);
""")

titles_table_insert = ("""
    INSERT INTO titles (
        SELECT t.tconst, t.primarytitle, t.titletype, t.startyear, t.runtimeminutes, t.genres 
        FROM (
            SELECT 
                st.tconst,
                st.primarytitle,
                st.titletype,
                st.startyear,
                st.runtimeminutes,
                st.genres,
                ROW_NUMBER() OVER (PARTITION BY st.primarytitle, st.startyear) AS title_row
            FROM staging_titles st
            WHERE NOT st.primarytitle IS NULL) AS t
        WHERE t.title_row = 1);
""")

principals_table_insert = ("""
    INSERT INTO principals
        (SELECT
        sp.tconst,
        sp."ordering",
        sn.primaryname,
        sp.category
        FROM staging_principals sp
        LEFT JOIN staging_names sn ON sn.nconst = sp.nconst
        WHERE NOT sn.primaryname IS NULL);
""")

dates_table_insert = ("""
    INSERT INTO dates
        (SELECT d.review_date, d.review_day, d.review_week, d.review_month, d.review_year, d.review_dow FROM (
            SELECT 
                review_date,
                DATE_PART('day', review_date) as review_day, 
                DATE_PART('week', review_date) as review_week, 
                DATE_PART('month', review_date) as review_month, 
                DATE_PART('year', review_date) as review_year, 
                DATE_PART('dow', review_date) as review_dow,
                ROW_NUMBER() OVER (PARTITION BY review_date) AS review_date_row
            FROM staging_reviews
            ) AS d
        WHERE d.review_date_row = 1);
""")

# QUERY LISTS

create_table_queries = [staging_reviews_table_create, staging_titles_table_create, staging_principals_table_create, staging_names_table_create, titles_table_create, principals_table_create, dates_table_create, reviews_table_create]
drop_table_queries = [staging_reviews_table_drop, staging_titles_table_drop, staging_principals_table_drop, staging_names_table_drop, reviews_table_drop, principals_table_drop, dates_table_drop, titles_table_drop]
copy_table_queries = [staging_reviews_copy, staging_titles_copy, staging_principals_copy, staging_names_copy]
insert_table_queries = [dates_table_insert, titles_table_insert, principals_table_insert, reviews_table_insert] 
