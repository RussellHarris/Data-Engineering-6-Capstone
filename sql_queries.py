# IMPORT
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
s3_bucket = config.get("S3", "BUCKET")
s3_raw_data = 's3://' + s3_bucket + '/raw'
s3_lookup_data = 's3://' + s3_bucket + '/lookup'
ARN_IAM_ROLE = config.get("IAM_ROLE", "arn")


# DROP TABLE QUERIES
staging_airport_codes_table_drop = "DROP TABLE IF EXISTS public.staging_airport_codes;"
airport_codes_table_drop = "DROP TABLE IF EXISTS public.airport_codes;"

staging_i94_immigration_table_drop = "DROP TABLE IF EXISTS public.staging_i94_immigration;"
i94_immigration_table_drop = "DROP TABLE IF EXISTS public.i94_immigration;"

staging_us_city_demographics_table_drop = "DROP TABLE IF EXISTS public.staging_us_city_demographics;"
us_city_demographics_table_drop = "DROP TABLE IF EXISTS public.us_city_demographics;"

staging_world_temperatures_table_drop = "DROP TABLE IF EXISTS public.staging_world_temperatures;"
world_temperatures_table_drop = "DROP TABLE IF EXISTS public.world_temperatures;"

i94addrl_table_drop = "DROP TABLE IF EXISTS public.i94addrl;"
i94cntyl_table_drop = "DROP TABLE IF EXISTS public.i94cntyl;"
i94model_table_drop = "DROP TABLE IF EXISTS public.i94model;"
i94prtl_table_drop = "DROP TABLE IF EXISTS public.i94prtl;"
i94visal_table_drop = "DROP TABLE IF EXISTS public.i94visal;"

us_state_visitor_demographics_drop = "DROP TABLE IF EXISTS public.us_state_visitor_demographics;"

# CREATE TABLE QUERIES
staging_airport_codes_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.staging_airport_codes (
        ident varchar(7),
        type varchar(14),
        name varchar(128),
        elevation_ft int2,
        continent char(2),
        iso_country char(2),
        iso_region varchar(7),
        municipality varchar(60),
        gps_code varchar(4),
        iata_code varchar(3),
        local_code varchar(7),
        coordinates varchar(43)
    );
""")

airport_codes_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.airport_codes (
        ident varchar(7) NOT NULL,
        type varchar(14) NOT NULL,
        name varchar(128) NOT NULL,
        elevation_ft int2,
        continent char(2) NOT NULL,
        iso_country char(2) NOT NULL,
        iso_region varchar(7) NOT NULL,
        state_code varchar(4) NOT NULL, --derived from iso_region
        municipality varchar(60) NOT NULL,
        gps_code varchar(4),
        iata_code varchar(3),
        local_code varchar(7),
        latitude float8 NOT NULL, --derived from coordinates
        longitude float8 NOT NULL, -- derived from coordinates
        CONSTRAINT pk_airport_codes PRIMARY KEY (
            ident
        )
    )
    DISTKEY (iso_region)
    COMPOUND SORTKEY (
        continent,iso_country,iso_region,municipality
    );
""")

staging_i94_immigration_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.staging_i94_immigration (
        cicid float8,
        i94yr float8,
        i94mon float8,
        i94cit float8,
        i94res float8,
        i94port varchar(3),
        arrdate float8,
        i94mode float8,
        i94addr char(2),
        depdate float8,
        i94bir float8,
        i94visa float8,
        count float8,
        dtadfile varchar(8),
        visapost varchar(3),
        occup varchar(3),
        entdepa char(1),
        entdepd char(1),
        entdepu char(1),
        matflag char(1),
        biryear float8,
        dtaddto varchar(8),
        gender char(1),
        insnum varchar(6),
        airline varchar(3),
        admnum float8,
        fltno varchar(5),
        visatype varchar(3)
    );
""")

i94_immigration_table_create = ("""
    CREATE TABLE "i94_immigration" (
        "cicid" int4 NOT NULL,
        "i94yr" int2 NOT NULL,
        "i94mon" int2 NOT NULL,
        "i94cit" int2 NOT NULL,
        "i94res" int2 NOT NULL,
        "i94port" varchar(3) NOT NULL,
        "arrdate" date NOT NULL, --derived
        "i94mode" int2 NOT NULL,
        "i94addr" char(2) NOT NULL,
        "depdate" date, --derived
        "i94bir" int2,
        "i94visa" int2 NOT NULL,
        "count" int2 NOT NULL,
        "dtadfile" date, --derived
        "visapost" char(3),
        "occup" char(3),
        "entdepa" char(1),
        "entdepd" char(1),
        "entdepu" char(1),
        "matflag" char(1),
        "biryear" int2,
        "dtaddto" date, --derived
        "gender" char(1),
        "insnum" varchar(6),
        "airline" varchar(3),
        "admnum" int8 NOT NULL,
        "fltno" varchar(5),
        "visatype" varchar(3) NOT NULL
    )
    DISTKEY (i94addr)
    COMPOUND SORTKEY (
        i94addr,i94yr,i94mon
    );
""")

staging_us_city_demographics_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.staging_us_city_demographics (
        city varchar(47),
        state varchar(20),
        median_age float4,
        male_population int4,
        female_population int4,
        total_population int4,
        number_of_veterans int4,
        foreign_born int4,
        average_household_size float4,
        state_code char(2),
        race varchar(33),
        count int4
    );
""")

us_city_demographics_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.us_city_demographics (
        state_code char(2) NOT NULL,
        city varchar(47) NOT NULL,
        state varchar(20) NOT NULL,
        median_age float4 NOT NULL,
        male_population int4,
        female_population int4,
        total_population int4 NOT NULL,
        american_indian_and_alaska_native int4,
        asian int4,
        black_or_african_american int4,
        hispanic_or_latino int4,
        white int4,
        number_of_veterans int4,
        foreign_born int4,
        average_household_size float4,
        CONSTRAINT pk_us_city_demographics PRIMARY KEY (
            state_code,city
        )
    )
    DISTKEY (state_code)
    COMPOUND SORTKEY (
        state_code,city
    );
""")

staging_world_temperatures_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.staging_world_temperatures (
        dt date,
        average_temperature float8,
        average_temperature_uncertainty float8,
        city varchar(25),
        country varchar(34),
        latitude varchar(6),
        longitude varchar(7)
    );
""")

world_temperatures_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.world_temperatures (
        country varchar(34) NOT NULL,
        city varchar(25) NOT NULL,
        latitude float8 NOT NULL,
        longitude float8 NOT NULL,
        dt date NOT NULL,
        avg_temp float8,
        avg_temp_uncert float8,
        CONSTRAINT pk_world_temperatures PRIMARY KEY (
            country,city,latitude,longitude,dt
        )
    )
    DISTKEY (city)
    COMPOUND SORTKEY (
        country,city,latitude,longitude,dt
    );
""")

i94addrl_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.i94addrl (
        id char(2) NOT NULL,
        name varchar(17) NOT NULL,
        CONSTRAINT pk_i94addrl PRIMARY KEY (
            id
        )
    )
    DISTSTYLE ALL
    SORTKEY (id);
""")

i94cntyl_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.i94cntyl (
        id int2 NOT NULL,
        name varchar(57) NOT NULL,
        CONSTRAINT pk_i94cntyl PRIMARY KEY (
            id
        )
    )
    DISTSTYLE ALL
    SORTKEY (id);
""")

i94model_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.i94model (
        id int2 NOT NULL,
        name varchar(12) NOT NULL,
        CONSTRAINT pk_i94model PRIMARY KEY (
            id
     )
    )
    DISTSTYLE ALL
    SORTKEY (id);
""")

i94prtl_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.i94prtl (
        id varchar(3) NOT NULL,
        name varchar(45) NOT NULL,
        us_city varchar(30),
        us_state_code char(2),
        source varchar(16) NOT NULL,
        CONSTRAINT pk_i94prtl PRIMARY KEY (
            id
        )
    )
    DISTSTYLE ALL
    SORTKEY (id);
""")

i94visal_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.i94visal (
        id int2 NOT NULL,
        name varchar(8) NOT NULL,
        CONSTRAINT pk_i94visal PRIMARY KEY (
            id
        )
    )
    DISTSTYLE ALL
    SORTKEY (id);
""")

us_state_visitor_demographics_create = ("""
    CREATE TABLE public.us_state_visitor_demographics (
        state_code char(2) NOT NULL,
        visit_year int2 NOT NULL,
        visit_median_age float4 NOT NULL,
        visit_male int4 NOT NULL,
        visit_female int4 NOT NULL,
        visit_total int4 NOT NULL,
        census_median_age float4 NOT NULL,
        census_male int4 NOT NULL,
        census_female int4 NOT NULL,
        census_total int4 NOT NULL,
        census_foreign_born int4,
        census_average_household_size float4,
        CONSTRAINT pk_us_state_visitor_demographics PRIMARY KEY (
            state_code
        )
    )
    DISTKEY (state_code)
    COMPOUND SORTKEY (
        state_code,visit_year
    );
""")


# STAGING TABLE QUERIES
staging_airport_codes_copy = ("""
    COPY staging_airport_codes
    FROM '{}'
    IAM_ROLE {}
    FORMAT AS CSV
    IGNOREHEADER 1;
""".format(s3_raw_data + '/airport_code_data/airport-codes_csv.csv', ARN_IAM_ROLE))

staging_i94_immigration_copy = ("""
    COPY staging_i94_immigration
    FROM '{}'
    IAM_ROLE {}
    FORMAT AS PARQUET;
""".format(s3_raw_data + '/i94_immigration_data', ARN_IAM_ROLE))

staging_us_city_demographics_copy = ("""
    COPY staging_us_city_demographics
    FROM '{}'
    IAM_ROLE {}
    DELIMITER ';'
    IGNOREHEADER 1;
""".format(s3_raw_data + '/us_city_demographic_data/us-cities-demographics.csv', ARN_IAM_ROLE))

staging_world_temperatures_copy = ("""
    COPY staging_world_temperatures
    FROM '{}'
    IAM_ROLE {}
    FORMAT AS CSV
    IGNOREHEADER 1;
""".format(s3_raw_data + '/world_temperature_data/GlobalLandTemperaturesByCity.csv', ARN_IAM_ROLE))

i94addrl_copy = ("""
    COPY i94addrl
    FROM '{}'
    IAM_ROLE {}
    FORMAT AS CSV;
""".format(s3_lookup_data + '/i94addrl.csv', ARN_IAM_ROLE))

i94cntyl_copy = ("""
    COPY i94cntyl
    FROM '{}'
    IAM_ROLE {}
    FORMAT AS CSV;
""".format(s3_lookup_data + '/i94cntyl.csv', ARN_IAM_ROLE))

i94model_copy = ("""
    COPY i94model
    FROM '{}'
    IAM_ROLE {}
    FORMAT AS CSV;
""".format(s3_lookup_data + '/i94model.csv', ARN_IAM_ROLE))

i94prtl_copy = ("""
    COPY i94prtl
    FROM '{}'
    IAM_ROLE {}
    FORMAT AS CSV
    IGNOREHEADER 1;
""".format(s3_lookup_data + '/i94prtl_enriched.csv', ARN_IAM_ROLE))

i94visal_copy = ("""
    COPY i94visal
    FROM '{}'
    IAM_ROLE {}
    FORMAT AS CSV;
""".format(s3_lookup_data + '/i94visal.csv', ARN_IAM_ROLE))


# INSERT TABLE QUERIES
airport_codes_table_insert = ("""
    INSERT INTO public.airport_codes (
        SELECT ident
             , type
             , name
             , elevation_ft
             , continent
             , iso_country
             , iso_region
             , RIGHT(iso_region, LEN(iso_region)-3) AS state_cd
             , municipality
             , gps_code
             , iata_code
             , local_code
             , CAST(SPLIT_PART(coordinates,',',2) AS float8) AS latitude
             , CAST(SPLIT_PART(coordinates,',',1) AS float8) AS longitude
          FROM public.staging_airport_codes
         WHERE iso_region <> 'US-U-A'
    );
""")

i94_immigration_table_insert = ("""
    INSERT INTO public.i94_immigration (
        SELECT cicid::int4
             , i94yr::int2
             , i94mon::int2
             --, i94cit::int2
             , (CASE WHEN c.id IS NULL
                     THEN 999::int2
                     ELSE i94cit::int2
               END) AS i94cit
             --, i94res::int2
             , (CASE WHEN r.id IS NULL
                     THEN 999::int2
                     ELSE i94res::int2
               END) AS i94res
             --, i94port
             , (CASE WHEN p.id IS NULL
                     THEN 'XXX'
                     ELSE i94port
                END) AS i94port
             , DATEADD(day, arrdate::int2, '1960/01/01')::date AS arrdate
             --, i94mode::int2
             , (CASE WHEN m.id IS NULL
                     THEN 9::int2
                     ELSE i94mode::int2
                END) AS i94mode
             --, i94addr
             , (CASE WHEN a.id IS NULL
                     THEN '99'
                     ELSE i94addr
                END) AS i94addr
             , DATEADD(day, depdate::int4, '1960/01/01')::date AS depdate
             , i94bir::int2
             --, i94visa::int2
             , (CASE WHEN v.id IS NULL
                     THEN 2::int2
                     ELSE i94visa::int2
                END) AS i94visa     
             , count::int2
             , to_date(dtadfile, 'YYYYMMDD') As dtadfile
             , visapost
             , occup
             , entdepa
             , entdepd
             , entdepu
             , matflag
             , biryear::int2
             --, dtaddto
             , to_date((CASE
                        WHEN TRIM(dtaddto) ~ '^\\d{8}$' 
                        THEN 
                            (CASE WHEN TRIM(dtaddto) = '00000000' OR TRIM(dtaddto) = '12319999'
                                  THEN NULL
                                  ELSE dtaddto
                             END)
                        ELSE NULL
                        END),  'MMDDYYYY') AS dtaddto
             , gender
             , insnum
             , airline
             , admnum::int8
             , fltno
             , visatype
          FROM staging_i94_immigration i
          LEFT JOIN i94cntyl c
                 ON i.i94cit = c.id
          LEFT JOIN i94cntyl r
                 ON i.i94res = r.id
          LEFT JOIN i94prtl p
                 ON i.i94port = p.id
          LEFT JOIN i94model m
                 ON i.i94mode = m.id
          LEFT JOIN i94addrl a
                 ON i.i94addr = a.id
          LEFT JOIN i94visal v
                 ON i.i94visa = v.id
    );
""")

us_city_demographics_table_insert = ("""
    INSERT INTO public.us_city_demographics (
        SELECT state_code
             , city
             , state
             , median_age
             , male_population
             , female_population
             , total_population
             , SUM(CASE WHEN race = 'American Indian and Alaska Native'
                        THEN count
                        ELSE NULL
                    END) AS american_indian_and_alaska_native
             , SUM(CASE WHEN race = 'Asian'
                        THEN count
                        ELSE NULL
                    END) AS asian
             , SUM(CASE WHEN race = 'Black or African-American'
                        THEN count
                        ELSE NULL
                    END) AS black_or_african_american
             , SUM(CASE WHEN race = 'Hispanic or Latino'
                        THEN count
                        ELSE NULL
                    END) AS hispanic_or_latino
             , SUM(CASE WHEN race = 'White'
                        THEN count
                        ELSE NULL
                    END) AS white
             , number_of_veterans
             , foreign_born
             , average_household_size
          FROM staging_us_city_demographics
         GROUP BY state_code
                , city
                , state
                , median_age
                , male_population
                , female_population
                , total_population
                , number_of_veterans
                , foreign_born
                , average_household_size
    );
""")

#us_state_demographics_table_insert = ("""
#    INSERT INTO public.us_state_demographics (
#        SELECT state_code
#             , state
#             , AVG(median_age) AS median_age
#             , SUM(male_population) AS male_population
#             , SUM(female_population) AS female_population
#             , SUM(total_population) AS total_population
#             , SUM(asian) AS asian
#             , SUM(black_or_african_american) AS black_or_african_american
#             , SUM(hispanic_or_latino) AS hispanic_or_latino
#             , SUM(white) AS white
#             , SUM(number_of_veterans) AS number_of_veterans
#             , SUM(foreign_born) AS foreign_born
#             , AVG(average_household_size) AS average_household_size
#        FROM us_city_demographics
#        GROUP BY state_code, state
#    );
#""")

world_temperatures_table_insert = ("""
    INSERT INTO public.world_temperatures (
        SELECT country
             , city
             , CASE
                    WHEN RIGHT(latitude, 1) = 'N'
                    THEN CAST(LEFT(latitude, LEN(latitude)-1) AS float8)
                    ELSE CAST(LEFT(latitude, LEN(latitude)-1) AS float8) * (-1)
                END AS latitude
             , CASE
                    WHEN RIGHT(longitude, 1) = 'E'
                    THEN CAST(LEFT(longitude, LEN(longitude)-1) AS float8)
                    ELSE CAST(LEFT(longitude, LEN(longitude)-1) AS float8) * (-1)
                END AS longitude
             , dt
             , average_temperature
             , average_temperature_uncertainty
          FROM public.staging_world_temperatures
    );
""")

us_state_visitor_demographics_insert = ("""
    INSERT INTO public.us_state_visitor_demographics (
        SELECT v.state_code
             , visit_year
             , visit_median_age
             , visit_male
             , visit_female
             , visit_total
             , census_median_age
             , census_male_population
             , census_female_population
             , census_total_population
             , census_foreign_born
             , census_average_household_size
          FROM (SELECT state_code
                     , ROUND(AVG(median_age), 1) AS census_median_age
                     , SUM(male_population) AS census_male_population
                     , SUM(female_population) AS census_female_population
                     , SUM(total_population) AS census_total_population
                     , SUM(foreign_born) AS census_foreign_born
                     , ROUND(AVG(average_household_size), 2) AS census_average_household_size
                  FROM us_city_demographics
                 GROUP BY state_code) c
          LEFT JOIN (SELECT i94addr AS state_code
                          , i94yr AS visit_year
                          , ROUND(MEDIAN(i94bir), 1) AS visit_median_age
                          , SUM(CASE WHEN gender = 'M'
                                     THEN 1
                                     ELSE 0
                                 END) AS visit_male
                          , SUM(CASE WHEN gender = 'F'
                                     THEN 1
                                     ELSE 0
                                 END) AS visit_female
                          , SUM(count) AS visit_total
                       FROM i94_immigration
                      GROUP BY i94addr, i94yr) v
                 ON c.state_code = v.state_code
    );
""")


# QUERY LISTS
drop_table_queries = [staging_airport_codes_table_drop
                     ,airport_codes_table_drop
                     ,staging_i94_immigration_table_drop
                     ,i94_immigration_table_drop
                     ,staging_us_city_demographics_table_drop
                     ,us_city_demographics_table_drop
                     ,staging_world_temperatures_table_drop
                     ,world_temperatures_table_drop
                     ,i94addrl_table_drop
                     ,i94cntyl_table_drop
                     ,i94model_table_drop
                     ,i94prtl_table_drop
                     ,i94visal_table_drop
                     ,us_state_visitor_demographics_drop]

create_table_queries = [staging_airport_codes_table_create
                       ,airport_codes_table_create
                       ,staging_i94_immigration_table_create
                       ,i94_immigration_table_create
                       ,staging_us_city_demographics_table_create
                       ,us_city_demographics_table_create
                       ,staging_world_temperatures_table_create
                       ,world_temperatures_table_create
                       ,i94addrl_table_create
                       ,i94cntyl_table_create
                       ,i94model_table_create
                       ,i94prtl_table_create
                       ,i94visal_table_create
                       ,us_state_visitor_demographics_create]

copy_table_queries = [staging_airport_codes_copy
                     ,staging_i94_immigration_copy
                     ,staging_us_city_demographics_copy
                     ,staging_world_temperatures_copy
                     ,i94addrl_copy
                     ,i94cntyl_copy
                     ,i94model_copy
                     ,i94prtl_copy
                     ,i94visal_copy]

insert_table_queries = [airport_codes_table_insert
                       ,i94_immigration_table_insert
                       ,us_city_demographics_table_insert
                       ,world_temperatures_table_insert
                       ,us_state_visitor_demographics_insert]

staging_checks = [
    {'check_sql': 'SELECT COUNT(*) FROM staging_airport_codes', 'expected_result': 55075},
    {'check_sql': 'SELECT COUNT(*) FROM staging_i94_immigration', 'expected_result': 40790529},
    {'check_sql': 'SELECT COUNT(*) FROM staging_us_city_demographics', 'expected_result': 2891},
    {'check_sql': 'SELECT COUNT(*) FROM staging_world_temperatures', 'expected_result': 8599212},
    {'check_sql': 'SELECT COUNT(*) FROM i94addrl', 'expected_result': 55},
    {'check_sql': 'SELECT COUNT(*) FROM i94cntyl', 'expected_result': 289},
    {'check_sql': 'SELECT COUNT(*) FROM i94model', 'expected_result': 4},
    {'check_sql': 'SELECT COUNT(*) FROM i94prtl', 'expected_result': 697},
    {'check_sql': 'SELECT COUNT(*) FROM i94visal', 'expected_result': 3}
]

insert_checks = [
    {'check_sql': 'SELECT COUNT(*) FROM airport_codes WHERE ident IS NULL', 'expected_result': 0},
    {'check_sql': 'SELECT COUNT(*) FROM i94_immigration', 'expected_result': 40790529},
    {'check_sql': 'SELECT COUNT(*) FROM us_city_demographics WHERE state_code IS NULL', 'expected_result': 0},
    {'check_sql': 'SELECT COUNT(*) FROM world_temperatures', 'expected_result': 8599212},
    {'check_sql': 'SELECT COUNT(*) FROM us_state_visitor_demographics WHERE state_code IS NULL', 'expected_result': 0}
]