drop schema if exists retail cascade;
create schema retail;

CREATE TABLE CITIES (
               CITY_ID      DECIMAL(9,0),
               COUNTRY_CODE VARCHAR(2) UTF8,
               ZIP_CODE     VARCHAR(20) UTF8,
               CITY_NAME    VARCHAR(200) UTF8,
               DISTRICT     VARCHAR(50) UTF8,
               AREA         VARCHAR(50) UTF8,
               AREA_SHORT   VARCHAR(50) UTF8,
               LAT          DECIMAL(9,6),
               LON          DECIMAL(9,6)
           );

CREATE TABLE ARTICLE (
            ARTICLE_ID         DECIMAL(9,0),
            DESCRIPTION        VARCHAR(100) UTF8,
            EAN                DECIMAL(18,0),
            BASE_SALES_PRICE   DECIMAL(9,2),
            PURCHASE_PRICE     DECIMAL(9,2),
            PRODUCT_GROUP      DECIMAL(9,0),
            PRODUCT_CLASS      DECIMAL(9,0),
            QUANTITY_UNIT      VARCHAR(100) UTF8,
            TMP_OLD_NR         DECIMAL(9,0),
            PRODUCT_GROUP_DESC VARCHAR(100) UTF8,
            DISTRIBUTION_COST  DECIMAL(9,2)
          );

CREATE TABLE MARKETS (
            MARKET_ID   DECIMAL(9,0),
            LONGITUDE   DECIMAL(9,6),
            LATITUDE    DECIMAL(9,6),
            POSTAL_CODE CHAR(5) UTF8,
            CITY        VARCHAR(50) UTF8,
            AREA        VARCHAR(50) UTF8,
            CITY_ID     DECIMAL(9,0),
            POPULATION  DECIMAL(18,0)
           );

CREATE TABLE SALES (
            SALES_ID                DECIMAL(18,0),
            SALES_DATE              DATE,
            SALES_TIMESTAMP         TIMESTAMP,
            PRICE                   DECIMAL(9,2),
            MONEY_GIVEN             DECIMAL(9,2),
            RETURNED_CHANGE         DECIMAL(9,2),
            LOYALTY_ID              DECIMAL(18,0),
            MARKET_ID               DECIMAL(9,0),
            TERMINAL_ID             DECIMAL(9,0),
            EMPLOYEE_ID             DECIMAL(9,0),
            TERMINAL_DAILY_SALES_NR DECIMAL(9,0),
            DISTRIBUTE BY SALES_ID
          );

CREATE TABLE SALES_POSITIONS (
            SALES_ID    DECIMAL(18,0),
            POSITION_ID DECIMAL(9,0),
            ARTICLE_ID  DECIMAL(9,0),
            AMOUNT      DECIMAL(9,0),
            PRICE       DECIMAL(9,2),
            VOUCHER_ID  DECIMAL(9,0),
            CANCELED    BOOLEAN,
            DISTRIBUTE BY SALES_ID
          );

CREATE TABLE DIM_DATE (
           SALES_DATE DATE
          );
          
IMPORT INTO CITIES from local csv file 'retail_mini/CITIES.csv';
IMPORT INTO ARTICLE from local csv file 'retail_mini/ARTICLE.csv';
IMPORT INTO MARKETS from local csv file 'retail_mini/MARKETS.csv';
IMPORT INTO SALES from local csv file 'retail_mini/SALES.csv';
IMPORT INTO SALES_POSITIONS from local csv file 'retail_mini/SALES_POSITIONS.csv';
IMPORT INTO DIM_DATE from local csv file 'retail_mini/DIM_DATE.csv';