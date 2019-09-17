from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf,to_timestamp,hour,dayofmonth,weekofyear,month,year,dayofweek,date_format,row_number
from pyspark.sql import Window
import datetime
from datetime import timedelta
from pyspark.sql.functions import broadcast
from valuetocategory import *


def create_spark_session():
    """  
    This Function connects and creates a Spark Session.  
    Paramaters:     
       None  
    Returns:
       spark connection
    """
    spark = SparkSession.builder.\
    config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    config("spark.sql.broadcastTimeout", "36000")\
    .enableHiveSupport().getOrCreate()
    return spark

def read_data(spark,input_data):
    """
    This Function reads the input data file path using spark.read.format API.
    Paramaters:     
       spark: Spark Session connection.
       input data : Input Data set
    Returns:
       Output Main Data frame.
    """
    df_spark =spark.read.format('com.github.saurfang.sas.spark').load(input_data)
    
    return df_spark

def visitor_arrival_info(df_spark):    
    """
    This Function filters some of the Dimentional Information related to Visitor Mode of Arrivals.
    Paramaters:     
       df_spark: main data frame
    Returns:
       Visitor mode of Arrival Dataframe.
    """
        
    global udfvalueToCategory
    udfvalueToCategory = udf(valuetocategory, StringType())
    
    visitor_mod_of_arrival_df = df_spark.withColumn('i94mode',df_spark.i94mode.cast("int"))\
                                    .withColumn('category',udfvalueToCategory('i94mode'))\
                                    .withColumn('airline',df_spark.airline)\
                                    .withColumn('fltno',df_spark.fltno)

    visitor_mod_of_arrival_df = visitor_mod_of_arrival_df.withColumn('i94mode',visitor_mod_of_arrival_df.i94mode)\
                                                     .withColumn('category',visitor_mod_of_arrival_df.category)\
                                                     .withColumn('airline',visitor_mod_of_arrival_df.airline)\
                                                     .withColumn('fltno',visitor_mod_of_arrival_df.fltno)

    visitor_mod_arrival_table_df = visitor_mod_of_arrival_df[['i94mode','category','airline','fltno']].dropDuplicates()
    
    visitor_mod_arrival_table_df.createOrReplaceTempView("Visitor_Arrival_Modes")

    visitor_mod_arrival_table_df = visitor_mod_arrival_table_df.write.partitionBy("i94mode").parquet("Immigration-Warehouse/Visitor_Arrival_Modes/")
    
    return visitor_mod_arrival_table_df

def visitor_personal_info(df_spark):
    """
    This Function filters some of the Dimentional Information related to Visitor Personal.
    Paramaters:     
       df_spark: main data frame
    Returns:
       Visitor Personal Detail Dataframe.
    """
    visitor_personal_df = df_spark.withColumn('i94cit',df_spark.i94cit.cast("int"))\
                              .withColumn('travel_city',udfvalueToCategory('i94cit'))\
                              .withColumn('i94res',df_spark.i94res.cast("int"))\
                              .withColumn('residence',udfvalueToCategory('i94res'))\
                              .withColumn('visitor_age',df_spark.i94bir.cast("int"))\
                              .withColumn('visitor_gender',df_spark.gender)\

    visitor_personal_df = visitor_personal_df.withColumn('i94cit',visitor_personal_df.i94cit)\
                                         .withColumn('travel_city',visitor_personal_df.travel_city)\
                                         .withColumn('i94res',visitor_personal_df.i94res)\
                                         .withColumn('residence',visitor_personal_df.residence)\
                                         .withColumn('visitor_age',visitor_personal_df.visitor_age)\
                                         .withColumn('visitor_gender',visitor_personal_df.visitor_gender)\

    visitor_table_df = visitor_personal_df[['i94cit','travel_city','i94res','residence','visitor_age','visitor_gender']].dropDuplicates()
    
    visitor_table_df.createOrReplaceTempView("visitor_details")
    
    visitor_table_df = visitor_table_df.write.partitionBy("travel_city","visitor_gender").parquet("Immigration-Warehouse/Visitor_Details/")
    
    return visitor_table_df

def visitor_i94_info(df_spark):
    """
    This Function filters some of the Dimentional Information related to Visitor US Port of entry I94 cites and residences.
    Paramaters:     
       df_spark: main data frame
    Returns:
       Visitor Port of entry and residences Dataframe.
    """
    visitor_i94_df = df_spark.withColumn('i94_addr',df_spark.i94addr)\
                         .withColumn('i94_addr_description',udfvalueToCategory('i94addr'))\
                         .withColumn('i94_port',df_spark.i94port)\
                         .withColumn('i94_port_description',udfvalueToCategory('i94port'))\

    visitor_i94_df = visitor_i94_df.withColumn('i94_addr',visitor_i94_df.i94addr)\
                               .withColumn('i94_addr_description',visitor_i94_df.i94_addr_description)\
                               .withColumn('i94_port',visitor_i94_df.i94port)\
                               .withColumn('i94_port_description',visitor_i94_df.i94_port_description)\

    visitor_i94_codes_df = visitor_i94_df['i94_addr','i94_addr_description','i94_port','i94_port_description'].dropDuplicates()

    visitor_i94_codes_df = visitor_i94_codes_df.na.fill({'i94_addr':'OT'})
    
    visitor_i94_codes_df.createOrReplaceTempView("visitor_i94_codes")
    
    visitor_i94_codes_df = visitor_i94_codes_df.write.partitionBy("i94_addr_description","i94_port_description").parquet("Immigration-Warehouse/Visitor_I94_Codes/")
    
    return visitor_i94_codes_df

def visitor_visa_info(df_spark):
    """
    This Function filters some of the Dimentional Information related to Visitor Visas.
    Paramaters:     
       df_spark: main data frame
    Returns:
       Visitor Visa and types Dataframe.
    """
    visitor_visa_df = df_spark.withColumn('i94_visa',df_spark.i94visa.cast("int"))\
                              .withColumn('i94_visa_description',udfvalueToCategory('i94visa'))\
                              .withColumn('visa_type',df_spark.visatype)\

    visitor_visa_table_df = visitor_visa_df['i94_visa','i94_visa_description','visa_type'].dropDuplicates()
    
    visitor_visa_table_df.createOrReplaceTempView("visitor_visa")
    
    visitor_visa_table_df = visitor_visa_table_df.write.partitionBy("i94_visa").parquet("Immigration-Warehouse/Visitor_Visa_Details/")
    
    return visitor_visa_table_df

def visitor_time_info(spark,df_spark):
    """
    This Function filters some of the Dimentional Information related to Visitor Times.
    Paramaters: 
       spark: Spark Session connection.
       df_spark: main data frame
    Returns:
       All Visitor related Times Dataframe.
    """
    df_spark = df_spark.fillna({'depdate':0})

    time = spark.udf.register("get_ts", lambda x: (datetime.timedelta(days=x) + datetime.datetime(1960,1,1)).strftime('%Y-%m-%d'))

    visitor_time_df = df_spark.withColumn('i94_admission_num',df_spark.admnum)\
                          .withColumn('i94_year',df_spark.i94yr.cast("int"))\
                          .withColumn('i94_month',df_spark.i94mon.cast("int"))\
                          .withColumn('i94_arrival_date',time(df_spark.arrdate.cast("int")))\
                          .withColumn('i94_departure_date',time(df_spark.depdate.cast("int")))\
                          .withColumn('i94_date_added_on_file',to_date(unix_timestamp(df_spark.dtadfile, 'yyyyMMdd').cast("timestamp")))\
                          .withColumn('i94_date_admitted_until',to_date(unix_timestamp(df_spark.dtaddto, 'MMddyyyy').cast("timestamp")))\

    visitor_time_table_df = visitor_time_df['i94_admission_num','i94_year','i94_month','i94_arrival_date','i94_departure_date','i94_date_added_on_file','i94_date_admitted_until'].dropDuplicates()

    visitor_time_table_df = visitor_time_table_df.na.replace(['1960-01-01'], ['null'], 'i94_departure_date')
    
    visitor_time_table_df.createOrReplaceTempView("visitor_times")
    
    visitor_time_table_df = visitor_time_table_df.write.partitionBy("i94_year","i94_month").parquet("Immigration-Warehouse/Visitor_Times/")
    
    return visitor_time_table_df

    
def i94_immigration_data(df_spark,visitor_mod_arrival,visitor_table,visitor_i94_codes,visitor_visa,visitor_time):
    """
    This Function gets all the Dimesional Data frames and do join process to get the final Fact table(I94 Immigration Data).
    Paramaters:     
       df_spark: main data frame
       visitor_mod_arrival: Dimentional Information related to Visitor Mode of Arrivals.
       visitor_table:Dimentional Information related to Visitor Personal.
       visitor_i94_codes:Dimentional Information related to Visitor US Port of entry I94 cites and residences.
       visitor_visa:Dimentional Information related to Visitor Visas.
       visitor_time:Dimentional Information related to Visitor Times.
    Returns:
       None.
    """
    df_spark = df_spark.fillna({'i94addr':'OT'})
    
    i94_df = df_spark.join(broadcast(visitor_mod_arrival),(df_spark.i94mode.cast("int") == visitor_mod_arrival.i94mode)&(df_spark.airline == visitor_mod_arrival.airline)&(df_spark.fltno == visitor_mod_arrival.fltno),"inner")\
                 .join(broadcast(visitor_table),(df_spark.i94cit.cast("int") == visitor_table.i94cit)&(df_spark.i94res == visitor_table.i94res)&(df_spark.i94bir.cast("int") == visitor_table.visitor_age)&(df_spark.gender == visitor_table.visitor_gender),"inner")\
                 .join(broadcast(visitor_i94_codes),(df_spark.i94port == visitor_i94_codes.i94_port)&(df_spark.i94addr == visitor_i94_codes.i94_addr),"inner")\
                 .join(broadcast(visitor_visa),(df_spark.i94visa.cast("int") == visitor_visa.i94_visa)&(df_spark.visatype == visitor_visa.visa_type),"inner")\
                 .join(broadcast(visitor_time),(df_spark.admnum == visitor_time.i94_admission_num)&(df_spark.i94yr.cast("int") == visitor_time.i94_year)&(df_spark.i94mon.cast("int") == visitor_time.i94_month),"inner")\

    i94_df = i94_df.withColumn('i94_admission_num',df_spark.admnum)\
                   .withColumn('i94_arrival_date',i94_df.i94_arrival_date)\
                   .withColumn('visa_type',i94_df.visa_type)\
                   .withColumn('i94_date_admitted_until',i94_df.i94_date_admitted_until)\
                   .withColumn('i94_port_description',i94_df.i94_port_description)\
                   .withColumn('i94_addr_description',i94_df.i94_addr_description)\
                   .withColumn('travel_city',i94_df.travel_city)\
                   .withColumn('residence',i94_df.residence)\
                   .withColumn('category',i94_df.category)\
                   .withColumn('visitor_age',i94_df.visitor_age)\
                   .withColumn('birth_year',df_spark.biryear.cast("int"))\
                   .withColumn('visitor_gender',i94_df.visitor_gender)\
                   .withColumn('insurance_number',df_spark.insnum)
    
    i94_df = i94_df.withColumn('i94_admission_num',i94_df.i94_admission_num)\
                   .withColumn('i94_arrival_date',i94_df.i94_arrival_date)\
                   .withColumn('visa_type',i94_df.visa_type)\
                   .withColumn('i94_date_admitted_until',i94_df.i94_date_admitted_until)\
                   .withColumn('i94_port_description',i94_df.i94_port_description)\
                   .withColumn('i94_addr_description',i94_df.i94_addr_description)\
                   .withColumn('travel_city',i94_df.travel_city)\
                   .withColumn('residence',i94_df.residence)\
                   .withColumn('category',i94_df.category)\
                   .withColumn('visitor_age',i94_df.visitor_age)\
                   .withColumn('birth_year',i94_df.birth_year)\
                   .withColumn('visitor_gender',i94_df.visitor_gender)\
                   .withColumn('insurance_number',i94_df.insurance_number)

    i94_table = i94_df[['i94_admission_num','i94_arrival_date','visa_type','i94_date_admitted_until',
                        'i94_port_description','i94_addr_description','travel_city','residence',
                        'category','visitor_age','birth_year','visitor_gender','insurance_number']].dropDuplicates()

    i94_table.createOrReplaceTempView('i94_immigration_data')
    
    i94_table = i94_table.write.partitionBy("visa_type").parquet("Immigration-Warehouse/i94_immigration_data/")


def main():
    """
    This Main Function Creates a Spark Session from Pyspark API, Executes the child functions with the respective parameters provided.
    Paramaters:     
     None    
    Returns:
     None
    """
    spark = create_spark_session()
    data = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    df_spark = read_data(spark,data)
    
    visitor_mod_arrival = visitor_arrival_info(df_spark)
    visitor_table = visitor_personal_info(df_spark)
    visitor_i94_codes = visitor_i94_info(df_spark)
    visitor_visa = visitor_visa_info(df_spark)
    visitor_time = visitor_time_info(spark,df_spark)
    
    i94_immigration_data(df_spark,visitor_mod_arrival,visitor_table,visitor_i94_codes,visitor_visa,visitor_time)
                                                                

if __name__ == "__main__":
    main()