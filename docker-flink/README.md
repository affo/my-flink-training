This folder contains every file related to the deploy of Flink using Docker.

The `conf` folder is bound to the Jobmanager and to Taskmanagers as a volume.

The Dockerfile and the InfluxDB reporter are part of an experiment that tries
to add an external lib to `/opt/flink/lib` folder. The `docker-compose.yml`
file in the parent directory should be modified to `build: docker-flink`
instead of `image: flink`.

In order to use the reporter, you need to uncomment the last lines in the
configuration file `flink-conf.yml`

Everything works actually, except that, on job execution, runtime exceptions
are thrown. I suppose that Jamie Grier's code for InfluxDB reporter does not
support the version of Flink I am running...
