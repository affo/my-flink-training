## Training

For the training, follow the setup at http://training.data-artisans.com/devEnvSetup.html.

### TL;DR

Clone the `flink-training-exercises` repository in the main folder (next to `stream-processing`):

```
git clone https://github.com/dataArtisans/flink-training-exercises.git
cd flink-training-exercises
mvn clean install
```

And refresh your maven project (the POM depends on that module).  
Download the data set and place it in `stream-processing`:

```
wget http://training.data-artisans.com/trainingData/nycTaxiRides.gz
```

## Clustering and System Integration

For the clustering part, run `docker-compose up` in a separate terminal and
consider the examples contained in `stream-processing` project,
`me.affetti.flink.training.enrichment` package, by setting the correct main class in
the `pom.xml` file.

You can either decide to run the examples from the IDE (carefully setup the
constants in class `K`), or directly in the Flink cluster available at
`localhost:8081` (or something like `192.168.99.100:8081` if you are on MacOS).

Grafana is available on port 3000.

For producing/consuming to/from Kafka and Redis key-value updates consult the
README in `sources` folder.
