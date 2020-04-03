FROM registry.uc.host.dxy/library/maven-conda

# Define working directory.
WORKDIR /work

# Prepare download dependencies
ADD . /work/

# setup conda and pip proxy.
RUN echo 'default_channels:\n\
  - http://nexus.k8s.uc.host.dxy/repository/anaconda/pkgs/msys2/\n\
  - http://nexus.k8s.uc.host.dxy/repository/anaconda/pkgs/pro/\n\
  - http://nexus.k8s.uc.host.dxy/repository/anaconda/pkgs/r/\n\
  - http://nexus.k8s.uc.host.dxy/repository/anaconda/pkgs/free/\n\
  - http://nexus.k8s.uc.host.dxy/repository/anaconda/pkgs/main/' > ~/.condarc

RUN mkdir ~/.pip
RUN echo '[global]\n\
index-url = http://nexus.k8s.uc.host.dxy/repository/pypi-aliyun/simple\n\
trusted-host=nexus.k8s.uc.host.dxy' > ~/.pip/pip.conf

ENV LANG C.UTF-8
ENV MALLOC_ARENA_MAX 4
ENV CONDA_PATH /opt/conda/bin/conda
ENV MLFLOW_TRACKING_URI http://mlflow.k8s.uc.host.dxy/

RUN apt-get update && apt-get install -y git

# package
RUN mvn -gs settings.xml clean package -DskipTests -Passembly -Dassembly.target.module=kungfu-panda-bamboo -Dassembly.format=dir

# clear maven cache.
RUN rm -rf ~/.m2

# Define default command.
ENTRYPOINT bash kungfu-panda-bin-1.0/kungfu-panda/bootstrap.sh run -Xmx4g -XX:+UseG1GC -Dserver.port=8100 org.panda.bamboo.service.Application
