# Spark Point In Polygon

This repo is the result of a collaboration between the GFW technical staff and Mansour Raad.
Mansour developed this hadoop / point-in-polygon system - see his [original repo](http://github.com/mraad/spark-pip)
for more information. The original README is also available here in docs/technical_readme.md

On top of his original framework we've added a few GFW-specific summary processes- looking
up a tree cover density threshold from a tree cover value, for example, or multiplying a biomass
value by area to get emissions per pixel.

### Inputs

#### Points
Points are stored in TSV format, usually in an S3 directory
Here's an example from our loss data:

```
-89.945375      0.313625        12      769.305779374   0       4
```

This is a loss point at (-89.945375, 0.313625) with loss occurring in 2012,
a pixel area of 769.305779374 m2 (based on pixel latitude), a TCD value of 0
and a biomass value of 4.

Current loss point data is stored here: s3://gfw2-data/alerts-tsv/loss_2017/

#### Polygons
Polygons are also stored in TSV format in an S3 directory.

Example:
```
POLYGON ((139.772662922 -7.33645210899988,139.772136831 -7.33645210899988,139.772136831 -7.33519300899997,139.772662922 -7.33645210899988))     plantations__wood_fiber Unknown Clearing/ Very Young Plantation 1       1       IDN     23      15      2
```

This particular polygon is an intersection of our plantations and wood fiber data. The first column is the geometry,
then the name of the dataset, then some plantations specific info (Unknown species type, Clearing / young plantation size), then two placeholder fields (1, 1) for wood fiber info. After that we have the iso/adm1/adm2 column, and then a TSV count column (not actually used).

Current polygon data is stored here: s3://gfw2-data/alerts-tsv/country-pages/ten-by-ten-gadm36/

## Configuring the application

By default, the application will look for an application.properties file to get it's input and output datasets. See our example application.properties for sample inputs and commentary about the various parameters.

## Updating the code

General scala development is beyond the scope of this README, but provided you have scala, spark and [maven](https://maven.apache.org/) installed, you should be able to run and test code locally. I wouldn't recommend this on a Windows machine, but should be pretty straightforward to install on linux or mac. My workflow usually has the following steps:

1. Make changes to the code
2. Test to see if things compile using `mvn complile`
3. If they do, run `mvn package` - this will create the output `target/` folder
4. To test locally, update `local.properties` to point to TSVs on your machine, then run `./local.sh`.

To run on a hadoop cluster, copy this `target` folder to the master machine, then follow the `starting a job` steps below.

## Custom GFW code

If you are modifying the code, odds are you want to add a new analysis.type, or change one of the existing ones. Most of this logic is contained in Summarize.scala, with additional utilities in HansenUtils.scala.

It may seem complicated, but it's pretty straightforward. Most of what we're doing is taking the output attribute table (points + polygon attributes), grouping by the data we want to keep (polyname, loss_year, boundary info, GADM data), summing the other stuff (area, emissions) and saving to CSV.

## Starting a job

We don't start too many jobs manually (most repos that use this call `spark-submit` with a subprocess). If you do need to call it manually, this should get you started:

```
spark-submit --master yarn --executor-memory 20g --jars target/libs/jts-core-1.14.0.jar target/spark-pip-0.3.jar
```

This assumes that you've compiled the repo into a target folder as described above, and that you have a relatively beefy cluster that can handle 20 GB of memory per executor. For smaller clusters, we usually run `9g` of memory.

## Outputs

Outputs vary depending on the analysis.type specified (if any). If the analysis.type is loss, our output will look something like this:

| polyname           | bound1 | bound2 | bound3 | bound4 | iso | adm1 | adm2 | thresh | loss_year | area          | emissions     |
|--------------------|--------|--------|--------|--------|-----|------|------|--------|-----------|---------------|---------------|
| admin              | 1      | 1      | 1      | 1      | BRA | 12   | 9    | 0      | 16        | 5003144.59559 | 1875.79985949 |
| ifl_2013__landmark | 1      | 1      | 1      | 1      | BRA | 12   | 9    | 75     | 11        | 1028353.82939 | 24374.7669306 |
| ifl_2013__landmark | 1      | 1      | 1      | 1      | BRA | 12   | 9    | 30     | 3         | 761.110102908 | 3.88166152483 |

This output is grouped by polyname, our bound fields, iso/adm1/adm2, loss_year and threshold, summing area and emissions. This allows us to pull a relatively small file (< 1 GB) down for post-processing.

## GFW repos that use this process

#### GFW Annual Loss Processing
[This repo](https://github.com/wri/gfw-annual-loss-processing/) manages our entire yearly loss update. It uses the various custom analysis.type options - loss, gain, extent, biomass, etc - to tabulate yearly summary statistics.

#### Hadoop PIP
[This repo](https://github.com/wri/hadoop_pip) starts an EMR cluster from a user's local machines, waits for the cluster to be ready, then passes a config file to it to start a processing step. Hadoop_PIP can be used on it's own for one-off analysis, and is also called by our [country-pages code](https://github.com/wri/gfw-country-pages-analysis-2/) and [places-to-watch](https://github.com/wri/gfw-places-to-watch/) code.

It should be noted that in the case of hadoop_pip, we usually don't use a particular analysis.type property. For these applications, we write the entire joined (points + polygons) attribute table to HDFS storage, then read it into a dataframe and perform custom SQL to further summarize it. More information can be found in the [hadoop_pip repo](https://github.com/wri/hadoop_pip).
