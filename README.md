# artefacts-2019_2023

Collection of some interesting pieces from my projects (2019 .. 2023). Spark, Scala, Python, sh

Two main categories:
- [Spark/Scala stuff](./etl-ml-pieces.scala/readme.md)
- [Python scripts, modules](./scripts.python/readme.md)

Plus one derivative:
- [Spark3 Scala stuff](./spark3-pieces.scala/readme.md)

For full description see docs inside.

## CI/CD

Manual cleanup: `find . -depth -type d \( -name target -or -name .bloop -or -name .bsp -or -name .metals -or -name metastore_db \) -exec echo rm -rfv {} \;`

Bash script, [build fat-jar for Spark](cicd/build_uber_jar.sh).
Example: `bash -xve ./cicd/build_uber_jar.sh ./etl-ml-pieces.scala/ /tmp/workdir/PACKAGES`

[Docker image](cicd/jarbuilder.Dockerfile), contains tools to build uber-jar.
Build docker image: `docker buildx build -f cicd/jarbuilder.Dockerfile --tag docker/jarbuilder:0.1.0 .`

Build uber-jar using docker container:
```s
docker run \
    -it --rm \
    --mount type=bind,src=./etl-ml-pieces.scala,dst=/sbtproject \
    --workdir /sbtproject \
    docker/jarbuilder:0.1.0 \
    sbt assembly && \
cp -v etl-ml-pieces.scala/target/scala-2.11/*.jar /tmp/
```

Run interactive sbt shell in docker container:
```s
docker run \
    -it --rm \
    --mount type=bind,src=./etl-ml-pieces.scala,dst=/sbtproject \
    --workdir /sbtproject \
    docker/jarbuilder:0.1.0 \
    sbt -v --mem 4096
```

Experiments
```s
# docker buildx build [OPTIONS] PATH | URL | -

docker buildx build -f cicd/jarbuilder.Dockerfile --tag docker/jarbuilder:0.1.0 .

# => => writing image sha256:d466806deb697cecf5ec011c0b19dc70e7d1c38f2fc34f5f01cf0022aa786ae8
# => => naming to docker.io/docker/jarbuilder:0.1.0

# docker container run [OPTIONS] IMAGE [COMMAND] [ARG...]

docker run \
    -it --rm \
    --mount type=bind,src=./etl-ml-pieces.scala,dst=/sbtproject \
    --workdir /sbtproject \
    docker/jarbuilder:0.1.0 \
    sbt assembly && \
cp -v etl-ml-pieces.scala/target/scala-2.11/*.jar /tmp/

```
snippets
