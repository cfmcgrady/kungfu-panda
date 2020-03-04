FROM registry.uc.host.dxy/dxy/maven-conda

# Define working directory.
WORKDIR /work

# Prepare download dependencies
ADD . /work/

ENV LANG C.UTF-8
ENV MALLOC_ARENA_MAX 4
ENV CONDA_PATH /opt/conda/bin/conda
ENV MLFLOW_TRACKING_URI http://mlflow.k8s.uc.host.dxy/

# package
RUN mvn -Psingle-jar -am -pl bamboo clean package -DskipTests

# clear maven cache.
RUN rm -rf ~/.m2

# Define default command.
ENTRYPOINT java -Xmx4g -XX:+UseG1GC -Dserver.port=8100 -cp bamboo/target/kungfu-panda-bamboo-1.0-shaded.jar org.panda.bamboo.service.Application
