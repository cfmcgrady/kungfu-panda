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
RUN mvn -gs settings.xml clean package -DskipTests -Passembly -Dassembly.target.module=kungfu-panda-bamboo -Dassembly.format=dir

# clear maven cache.
RUN rm -rf ~/.m2

# Define default command.
ENTRYPOINT bash kungfu-panda-bin-1.0/kungfu-panda/bootstrap.sh run -Xmx4g -XX:+UseG1GC -Dserver.port=8100 org.panda.bamboo.service.Application
