from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DateType, IntegerType, FloatType, DecimalType
from pyspark.sql.functions import col
from pyspark.sql.functions import isnull, when, count
import pyspark.sql.functions as f
import numpy as np

spark = SparkSession \
    .builder \
    .appName('MastersCST') \
    .getOrCreate()


def menuchoice():
    # while choice !="q":

    # global choice

    choice = ""
    while choice != 0:

        menu = ["Read data", "Show Schema", "show row and column count", "show null values", "Transform column type",
                "Remove null", "Rename column", "Remove column", "count distinct rows/values",
                "remove duplicate rows/values", "Locate outliers", "remove outliers by trimming",
                "remove outliers by capping", "learn about few operations"]

        for x in range(len(menu)):
            print("{} {}".format(x + 1, menu[x]))

        choice = int(input("choose option or press 0 to quit"))
        if choice == 1:
            read_data()
        elif choice == 2:
            show_schema()
        elif choice == 3:
            row_column_count()
        elif choice == 4:
            show_null()
        elif choice == 5:
            transform_column_type()
        elif choice == 6:
            remove_null()
        elif choice == 7:
            rename_column()
        elif choice == 8:
            remove_column()
        elif choice == 9:
            count_distinct()
        elif choice == 10:
            remove_duplicate()
        elif choice == 11:
            locate_outliers()
        elif choice == 12:
            remove_outliers_trimming()
        elif choice == 13:
            remove_outliers_capping()
        elif choice == 14:
            data_help()
        else:
            print("choose between 0 and", len(menu))


def read_data():
    global df1

    print("to read and process a data, this file and the data to be processed need to be in same folder/directory  ")
    go = int(input("press 0 to go back to menu or 1 to read data "))

    if go == 1:
        choice1 = input("input name of csv file")
        data_format = input("input format e.g csv")
        seperator = input("enter seperator. Do any special character separate each field in a row? e.g seperated by ; "
                          "or , or |")
        data = choice1 + "." + data_format

        df1 = (spark.read
               .format("csv")
               .options(delimiter=seperator, header=True)
               .load(data))
        df1.printSchema()
    else:
        menuchoice()


def show_schema():
    print("This is a data discovery operation to show the structure of the data. It shows the columns and data type")
    go = int(input("press 0 to go back to menu or 1 to show schema "))
    if go == 1:
        df1.printSchema()
    else:
        menuchoice()


def row_column_count():
    print("This is a data discovery operation to show the number of rows and column contained in the data")
    go = int(input("press 0 to go back to menu or 1 to show number of row and column"))

    if go == 1:
        row_count = df1.count()
        column_count = len(df1.columns)
        print("the number of rows are", row_count, "and the number of columns are", column_count)
    else:
        menuchoice()


def show_null():
    print("This is a data discovery operation to how many null values exist")
    go = int(input("press 0 to go back to menu or 1 to show number of null values"))

    if go == 1:
        z = df1.select([count(when(isnull(c), c)).alias(c) for c in df1.columns]).show()
        print(z)
        print("to remove null values, press 6")
    else:
        menuchoice()


def transform_column_type():
    # global df2
    print("This is a data structuring operation to change column data type")
    go = int(input("press 0 to go back to menu or 1 to change column data type"))
    if go == 1:
        df1.printSchema()

        column_change = input("what column do you want to change")
        print("press 1 for int, 2 for float, 3 for string, 4 for date type")

        if column_change in df1.columns:
            data_type = int(input("what data type do you want to change it to?"))
            if data_type == 1:
                df5 = df1.withColumn(column_change, col(column_change).cast(IntegerType()))
            elif data_type == 2:
                df5 = df1.withColumn(column_change, col(column_change).cast(FloatType()))
            elif data_type == 3:
                df5 = df1.withColumn(column_change, col(column_change).cast(StringType()))
            elif data_type == 4:
                df5 = df1.withColumn(column_change, col(column_change).cast(DateType()))

        else:
            print("column does not exist")
        df5.printSchema()

    else:
        menuchoice()


def remove_null():
    print("This is a data cleaning operation to remove null values")
    go = int(input("press 0 to go back to menu or 1 to remove null values"))

    if go == 1:
        # remove null values
        df1.dropna(how='any')
        print("all null values have been removed")
    else:
        menuchoice()


def rename_column():
    global df_renamed
    print("This is a data structuring operation to rename column")
    go = int(input("press 0 to go back to menu or 1 to rename column"))

    if go == 1:

        column_name = input("what column do you want to change?")
        new_name = input("enter new column name")
        all_columns = df1.columns
        if column_name in all_columns:
            df_renamed = df1.withColumnRenamed(column_name, new_name)
            df_renamed.printSchema()
        else:
            print("column does not exist")
    else:
        menuchoice()


def remove_column():
    print("This is a data structuring operation to remove column")
    go = int(input("press 0 to go back to menu or 1 to remove column"))

    if go == 1:
        column_name = input("what column do you want to remove?")
        all_columns = df1.columns
        renamed_columns = df_renamed.columns
        if column_name in all_columns:
            df1.drop(column_name).printSchema()
        elif column_name in renamed_columns:
            df_renamed.drop(column_name).printSchema()
        else:
            print("column name does not exist")
    else:
        menuchoice()


def count_distinct():
    print("This is a data discovery operation to count number of distinct columns i.e non-duplicate values")
    go = int(input("press 0 to go back to menu or 1 to count number of distinct column"))

    if go == 1:
        distinct = df1.distinct()
        print("Distinct count: " + str(distinct.count()))
        print("to remove duplicate values, press 10")
    else:
        menuchoice()


def remove_duplicate():
    print("This is a data cleaning operation to remove duplicate value")
    go = int(input("press 0 to go back to menu or 1 to rename duplicate"))

    if go == 1:
        drop_duplicate = df1.dropDuplicates()
        print("Distinct count from the data after dropping duplicates : " + str(drop_duplicate.count()))
    else:
        menuchoice()


def locate_outliers():
    # global df3, percentile25, percentile75, iqr, upper_limit, lower_limit
    # convert to pandas dataframe
    print("This is a data discovery operation to locate outliers. note: locate outliers for column with float/int type")
    go = int(input("press 0 to go back to menu or 1 to locate outliers"))
    if go == 1:

        column = input("enter column. note must be float/int type")
        if column in df1.columns:
            df3 = df1.withColumn(column, col(column).cast(FloatType()))
            df4 = df3.toPandas()

            percentile25 = df4[column].quantile(0.25)
            percentile75 = df4[column].quantile(0.75)
            iqr = percentile75 - percentile25

            # finding upper and lower limit
            upper_limit = percentile75 + 1.5 * iqr
            lower_limit = percentile25 - 1.5 * iqr
            outliers = df4[df4[column] > upper_limit]
            # outliers_count = outliers.shape[0]
            print("the outliers detected are : " + str(outliers.shape[0]))
            print("to remove outlier by trimming press 12 or 13 for capping")
        else:
            print("column does not exist")
    else:
        menuchoice()


def remove_outliers_trimming():
    print("This is a data cleaning operation to remove outliers by trimming")
    go = int(input("press 0 to go back to menu or 1 to remove outliers by trimming"))

    if go == 1:
        column = input("enter column. note must be float/int type")
        if column in df1.columns:
            df3 = df1.withColumn(column, col(column).cast(FloatType()))
            df4 = df3.toPandas()

            percentile25 = df4[column].quantile(0.25)
            percentile75 = df4[column].quantile(0.75)
            iqr = percentile75 - percentile25

            # finding upper and lower limit
            upper_limit = percentile75 + 1.5 * iqr
            lower_limit = percentile25 - 1.5 * iqr
            outliers = df4[df4[column] < upper_limit]
            print("the number of rows remaining after removing outliers by trimming are : " + str(outliers.shape[0]))
        else:
            print("column does not exist")
    else:
        menuchoice()


def remove_outliers_capping():
    print("This is a data cleaning operation to remove outliers by capping")
    go = int(input("press 0 to go back to menu or 1 to remove outliers by capping"))

    if go == 1:
        column = input("enter column. note must be float/int type")
        if column in df1.columns:
            df3 = df1.withColumn(column, col(column).cast(FloatType()))
            df4 = df3.toPandas()

            percentile25 = df4[column].quantile(0.25)
            percentile75 = df4[column].quantile(0.75)
            iqr = percentile75 - percentile25

            # finding upper and lower limit
            upper_limit = percentile75 + 1.5 * iqr
            lower_limit = percentile25 - 1.5 * iqr

            new_df_cap = df4.copy()
            new_df_cap[column] = np.where(new_df_cap[column] > upper_limit, upper_limit,
                                          np.where(new_df_cap[column] < lower_limit, lower_limit,
                                                   new_df_cap[column]))
            print("the number of rows remaining after removing outliers by capping are : " + str(new_df_cap.shape[0]))
        else:
            print("column does not exist")

    else:
        menuchoice()


def data_help():
    learn = int(input("to learn about how to use this press 0, data cleaning handling processes press 1"))

    if learn == 0:
        print(
            "this tool enables you the user to be able to prepare data without help. The author presented this follow the step by step guidline")

    elif learn == 1:
        print("Copied from:https://wellsr.com/python/outlier-data-handling-with-python/"
              "\n "
              "One of the ways to remove outliers from your dataset is by removing all the values above or below the "
              "\n "
              "upper and lower limits calculated with IQR ranges.Outlier trimming via the IQR range does not distort "
              "\n "
              "the default data distribution and therefore can be used when the dataset is not follow a normal ("
              "\n "
              "Gaussian) distribution.Let’s find the quartile one (q1) and quartile three (q3) values for the tips "
              "\n "
              "column of our dataset. These values will be used to find the IQR range.")
        print("\n")
        print("Copied from:https://wellsr.com/python/outlier-data-handling-with-python/"
              "\n "
              "Trimming outliers altogether may result in the removal of a large number of records from your dataset "
              "\n "
              "which isn’t desirable in some cases since columns other than the ones containing the outlier values "
              "\n "
              "may contain useful information.In such cases, you can use outlier capping to replace the outlier "
              "\n "
              "values with a maximum or minimum capped values. Be warned, this manipulates your data, but here’s how "
              "\n "
              "you do it.You can replace outlier values by the upper and lower limit calculated using the IQR range "
              "\n "
              "in the last section. Look at the following script for reference.")

    else:
        print("sorry!. press 1. we have only one help for now")


menuchoice()
