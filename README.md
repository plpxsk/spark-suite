# Pyspark

Spark, with pyspark, is installed already.

But, still need `pyspark` python module, of the _same version_, via `pip`. See
`requirements.txt`

Also, on local, may need these variables:

```bash
export JAVA_HOME=/usr/local/Cellar/openjdk@11/11.0.15/libexec/openjdk.jdk/Contents/Home
export SPARK_HOME=/usr/local/Cellar/apache-spark/3.2.1/libexec/
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.3-src.zip:$PYTHONPATH
export PATH=$SPARK_HOME/python:$PATH
```


## Example

Check this works with `spark-submit src/example/run.py`

# Test

Run `make test` to run `pytest` tests with automatic discovery.

Can also try `unittest`, but inside `/src`: `python -m unittest`

