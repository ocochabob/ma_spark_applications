# ma_spark_applications

This repository contains sample CAN and US datasets in CSV format and sample spark applications to process data using either a local or a cloud spark cluster.

## overview

### data

The data folder contains an artficially created CAN dataset and a real-world US dataset in CSV format.

The CAN dataset consists of three columns:

    * timestamp:    timestamp of the respective CAN message
    * attribute:    number of the respective CAN message attribute (e.g. sensor data)
    * value:        value of the respective CAN message


The US dataset consists of two files.

The file record_us_data.csv contains ultrasonic sensor values and consists of three columns:

    * timestamp:            timestamp of the respective us sensor measurement
    * ui32ArduinoTimestamp: arduino timestamp of the device collecting the sensor data
    * f32Value:             us sensor value in cm (-1/400 indicate no object recognition)

The file record_test_data.csv contains data to validate the us dataset and consists of two columns:

    * timestamp:        timestamp of the respective us sensor measurement
    * objectRecognized: value indicating whether an object was present at the respective timestamp

### src

The folder src contains the source code of various spark scala applications to process the CAN and US datasets described previously as well as a simple application to convert data from CSV to the optimized PARQUET format.

### bin

The folder bin contains ready-to-go spark scala applications to process the datasets.

## build scala applications

To run adjusted or completely new spark scala applications, we need to build the applications using for example sbt. E.g. to build the csvToParquetApp, we need to run:

    `sbt package`

in the csvToParquetApp.

To build a new application we need to create a new build.sbt file and the respective .scala file which needs to be placed in 'src/main/scala/' and run:

    `sbt package`

The created .jar which is needed to run the respective application, is located in 'target/scala-x.xx/'.

## run scala applications

To run created spark scala applications we use the 'spark-submit' script which is located in the bin folder of a spark installation. We use the script name and necessary application parameters as parameters. To run the csvToParquetApp we use:

    `./spark-submit "path/to/app/csvToParquetApp.jar" "path/to/input/file.csv" "," "path/to/output/file.parquet"`

Which parameters to use is documented in the source code of the respective application.

Application and input files may need to be uploaded to the respective filesystem (HDFS, S3) before usage.