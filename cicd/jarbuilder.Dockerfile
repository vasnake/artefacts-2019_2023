# java + sbt
FROM openjdk:8-jdk-alpine
ENV PATH=/usr/local/sbt/bin:${JAVA_HOME}/bin:${PATH}
RUN apk add --no-cache wget tar bash shadow coreutils ncurses
RUN usermod --shell /bin/bash root

RUN mkdir -p "/usr/local/sbt" && \
  wget -qO - --no-check-certificate "https://github.com/sbt/sbt/releases/download/v1.10.1/sbt-1.10.1.tgz" | \
  tar xz -C /usr/local/sbt --strip-components=1 && \
  sbt -Dsbt.rootdir=true sbtVersion

# invoke dependencies downloading by building project
RUN --mount=type=bind,source=etl-ml-pieces.scala,target=sbtproject,rw \
    bash -c 'pushd sbtproject && \
    sbt -v --mem 4096 clean update compile test assembly && \
    popd'
