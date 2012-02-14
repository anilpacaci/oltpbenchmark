-- Drop Tables
DROP TABLE IF EXISTS CONFIG_PROFILE;
DROP TABLE IF EXISTS CONFIG_HISTOGRAMS;
DROP TABLE IF EXISTS COUNTRY;
DROP TABLE IF EXISTS AIRPORT;
DROP TABLE IF EXISTS AIRPORT_DISTANCE;
DROP TABLE IF EXISTS AIRLINE;
DROP TABLE IF EXISTS CUSTOMER;
DROP TABLE IF EXISTS FREQUENT_FLYER;
DROP TABLE IF EXISTS FLIGHT;
DROP TABLE IF EXISTS RESERVATION;

-- 
-- CONFIG_PROFILE
--
CREATE TABLE CONFIG_PROFILE (
    CFP_SCALE_FACTOR            FLOAT NOT NULL,
    CFP_AIPORT_MAX_CUSTOMER     VARCHAR(10001) NOT NULL,
    CFP_FLIGHT_START            TIMESTAMP NOT NULL,
    CFP_FLIGHT_UPCOMING         TIMESTAMP NOT NULL,
    CFP_FLIGHT_PAST_DAYS        INT NOT NULL,
    CFP_FLIGHT_FUTURE_DAYS      INT NOT NULL,
    CFP_FLIGHT_OFFSET           INT,
    CFP_RESERVATION_OFFSET      INT,
    CFP_NUM_RESERVATIONS        BIGINT NOT NULL,
    CFP_CODE_IDS_XREFS          VARCHAR(16004) NOT NULL
);

--
-- CONFIG_HISTOGRAMS
--
CREATE TABLE CONFIG_HISTOGRAMS (
    CFH_NAME             VARCHAR(128) NOT NULL,
    CFH_DATA             VARCHAR(10005) NOT NULL,
    CFH_IS_AIRPORT       TINYINT DEFAULT 0,
    PRIMARY KEY (CFH_NAME)
);

-- 
-- COUNTRY
--
CREATE TABLE COUNTRY (
    CO_ID        BIGINT NOT NULL,
    CO_NAME      VARCHAR(64) NOT NULL,
    CO_CODE_2    VARCHAR(2) NOT NULL,
    CO_CODE_3    VARCHAR(3) NOT NULL,
    PRIMARY KEY (CO_ID)
);

--
-- AIRPORT
--
CREATE TABLE AIRPORT (
    AP_ID          BIGINT NOT NULL,
    AP_CODE        VARCHAR(3) NOT NULL,
    AP_NAME        VARCHAR(128) NOT NULL,
    AP_CITY        VARCHAR(64) NOT NULL,
    AP_POSTAL_CODE VARCHAR(12),
    AP_CO_ID       BIGINT NOT NULL REFERENCES COUNTRY (CO_ID),
    AP_LONGITUDE   FLOAT,
    AP_LATITUDE    FLOAT,
    AP_GMT_OFFSET  FLOAT,
    AP_WAC         BIGINT,
    AP_IATTR00     BIGINT,
    AP_IATTR01     BIGINT,
    AP_IATTR02     BIGINT,
    AP_IATTR03     BIGINT,
    AP_IATTR04     BIGINT,
    AP_IATTR05     BIGINT,
    AP_IATTR06     BIGINT,
    AP_IATTR07     BIGINT,
    AP_IATTR08     BIGINT,
    AP_IATTR09     BIGINT,
    AP_IATTR10     BIGINT,
    AP_IATTR11     BIGINT,
    AP_IATTR12     BIGINT,
    AP_IATTR13     BIGINT,
    AP_IATTR14     BIGINT,
    AP_IATTR15     BIGINT,
    PRIMARY KEY (AP_ID)
);

--
-- AIRPORT_DISTANCE
--
CREATE TABLE AIRPORT_DISTANCE (
    D_AP_ID0       BIGINT NOT NULL REFERENCES AIRPORT (AP_ID),
    D_AP_ID1       BIGINT NOT NULL REFERENCES AIRPORT (AP_ID),
    D_DISTANCE     FLOAT NOT NULL,
    PRIMARY KEY (D_AP_ID0, D_AP_ID1)
);

--
-- AIRLINE
--
CREATE TABLE AIRLINE (
    AL_ID          BIGINT NOT NULL,
    AL_IATA_CODE   VARCHAR(3),
    AL_ICAO_CODE   VARCHAR(3),
    AL_CALL_SIGN   VARCHAR(32),
    AL_NAME        VARCHAR(128) NOT NULL,
    AL_CO_ID       BIGINT NOT NULL REFERENCES COUNTRY (CO_ID),
    AL_IATTR00     BIGINT,
    AL_IATTR01     BIGINT,
    AL_IATTR02     BIGINT,
    AL_IATTR03     BIGINT,
    AL_IATTR04     BIGINT,
    AL_IATTR05     BIGINT,
    AL_IATTR06     BIGINT,
    AL_IATTR07     BIGINT,
    AL_IATTR08     BIGINT,
    AL_IATTR09     BIGINT,
    AL_IATTR10     BIGINT,
    AL_IATTR11     BIGINT,
    AL_IATTR12     BIGINT,
    AL_IATTR13     BIGINT,
    AL_IATTR14     BIGINT,
    AL_IATTR15     BIGINT,
    PRIMARY KEY (AL_ID)
);

--
-- CUSTOMER
--
CREATE TABLE CUSTOMER (
    C_ID           BIGINT NOT NULL,
    C_ID_STR       VARCHAR(64) UNIQUE NOT NULL,
    C_BASE_AP_ID   BIGINT REFERENCES AIRPORT (AP_ID),
    C_BALANCE      FLOAT NOT NULL,
    C_SATTR00      VARCHAR(32),
    C_SATTR01      VARCHAR(8),
    C_SATTR02      VARCHAR(8),
    C_SATTR03      VARCHAR(8),
    C_SATTR04      VARCHAR(8),
    C_SATTR05      VARCHAR(8),
    C_SATTR06      VARCHAR(8),
    C_SATTR07      VARCHAR(8),
    C_SATTR08      VARCHAR(8),
    C_SATTR09      VARCHAR(8),
    C_SATTR10      VARCHAR(8),
    C_SATTR11      VARCHAR(8),
    C_SATTR12      VARCHAR(8),
    C_SATTR13      VARCHAR(8),
    C_SATTR14      VARCHAR(8),
    C_SATTR15      VARCHAR(8),
    C_SATTR16      VARCHAR(8),
    C_SATTR17      VARCHAR(8),
    C_SATTR18      VARCHAR(8),
    C_SATTR19      VARCHAR(8),
    C_IATTR00      BIGINT,
    C_IATTR01      BIGINT,
    C_IATTR02      BIGINT,
    C_IATTR03      BIGINT,
    C_IATTR04      BIGINT,
    C_IATTR05      BIGINT,
    C_IATTR06      BIGINT,
    C_IATTR07      BIGINT,
    C_IATTR08      BIGINT,
    C_IATTR09      BIGINT,
    C_IATTR10      BIGINT,
    C_IATTR11      BIGINT,
    C_IATTR12      BIGINT,
    C_IATTR13      BIGINT,
    C_IATTR14      BIGINT,
    C_IATTR15      BIGINT,
    C_IATTR16      BIGINT,
    C_IATTR17      BIGINT,
    C_IATTR18      BIGINT,
    C_IATTR19      BIGINT,
    PRIMARY KEY (C_ID)
);

--
-- FREQUENT_FLYER
--
CREATE TABLE FREQUENT_FLYER (
    FF_C_ID        BIGINT NOT NULL REFERENCES CUSTOMER (C_ID),
    FF_AL_ID       BIGINT NOT NULL REFERENCES AIRLINE (AL_ID),
    FF_C_ID_STR    VARCHAR(64) NOT NULL,
    FF_SATTR00     VARCHAR(32),
    FF_SATTR01     VARCHAR(32),
    FF_SATTR02     VARCHAR(32),
    FF_SATTR03     VARCHAR(32),
    FF_IATTR00     BIGINT,
    FF_IATTR01     BIGINT,
    FF_IATTR02     BIGINT,
    FF_IATTR03     BIGINT,
    FF_IATTR04     BIGINT,
    FF_IATTR05     BIGINT,
    FF_IATTR06     BIGINT,
    FF_IATTR07     BIGINT,
    FF_IATTR08     BIGINT,
    FF_IATTR09     BIGINT,
    FF_IATTR10     BIGINT,
    FF_IATTR11     BIGINT,
    FF_IATTR12     BIGINT,
    FF_IATTR13     BIGINT,
    FF_IATTR14     BIGINT,
    FF_IATTR15     BIGINT,
   PRIMARY KEY (FF_C_ID, FF_AL_ID)
);
CREATE INDEX IDX_FF_CUSTOMER_ID ON FREQUENT_FLYER (FF_C_ID_STR);

--
-- FLIGHT
--
CREATE TABLE FLIGHT (
    F_ID            BIGINT NOT NULL,
    F_AL_ID         BIGINT NOT NULL REFERENCES AIRLINE (AL_ID),
    F_DEPART_AP_ID  BIGINT NOT NULL REFERENCES AIRPORT (AP_ID),
    F_DEPART_TIME   TIMESTAMP NOT NULL,
    F_ARRIVE_AP_ID  BIGINT NOT NULL REFERENCES AIRPORT (AP_ID),
    F_ARRIVE_TIME   TIMESTAMP NOT NULL,
    F_STATUS        BIGINT NOT NULL,
    F_BASE_PRICE    FLOAT NOT NULL,
    F_SEATS_TOTAL   BIGINT NOT NULL,
    F_SEATS_LEFT    BIGINT NOT NULL,
    F_IATTR00       BIGINT,
    F_IATTR01       BIGINT,
    F_IATTR02       BIGINT,
    F_IATTR03       BIGINT,
    F_IATTR04       BIGINT,
    F_IATTR05       BIGINT,
    F_IATTR06       BIGINT,
    F_IATTR07       BIGINT,
    F_IATTR08       BIGINT,
    F_IATTR09       BIGINT,
    F_IATTR10       BIGINT,
    F_IATTR11       BIGINT,
    F_IATTR12       BIGINT,
    F_IATTR13       BIGINT,
    F_IATTR14       BIGINT,
    F_IATTR15       BIGINT,
    F_IATTR16       BIGINT,
    F_IATTR17       BIGINT,
    F_IATTR18       BIGINT,
    F_IATTR19       BIGINT,
    F_IATTR20       BIGINT,
    F_IATTR21       BIGINT,
    F_IATTR22       BIGINT,
    F_IATTR23       BIGINT,
    F_IATTR24       BIGINT,
    F_IATTR25       BIGINT,
    F_IATTR26       BIGINT,
    F_IATTR27       BIGINT,
    F_IATTR28       BIGINT,
    F_IATTR29       BIGINT,
    PRIMARY KEY (F_ID)
);
create index F_DEPART_TIME_IDX on FLIGHT (F_DEPART_TIME);

--
-- RESERVATION
--
CREATE TABLE RESERVATION (
    R_ID            BIGINT NOT NULL,
    R_C_ID          BIGINT NOT NULL REFERENCES CUSTOMER (C_ID),
    R_F_ID          BIGINT NOT NULL REFERENCES FLIGHT (F_ID),
    R_SEAT          BIGINT NOT NULL,
    R_PRICE         FLOAT NOT NULL,
    R_IATTR00       BIGINT,
    R_IATTR01       BIGINT,
    R_IATTR02       BIGINT,
    R_IATTR03       BIGINT,
    R_IATTR04       BIGINT,
    R_IATTR05       BIGINT,
    R_IATTR06       BIGINT,
    R_IATTR07       BIGINT,
    R_IATTR08       BIGINT,
    UNIQUE (R_F_ID, R_SEAT),
    PRIMARY KEY (R_ID, R_C_ID, R_F_ID)
);