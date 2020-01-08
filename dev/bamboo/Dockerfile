FROM maven:3.6.3-jdk-8

# install miniconda
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-4.5.11-Linux-x86_64.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh && \
    /opt/conda/bin/conda clean -tipsy && \
    ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate base" >> ~/.bashrc

# Define working directory.
WORKDIR /work

# Prepare download dependencies
ADD . /work/

ENV LANG C.UTF-8
ENV MALLOC_ARENA_MAX 4
ENV CONDA_PATH /opt/conda/bin/conda

# package
RUN mvn -Psingle-jar -am -pl bamboo clean package -DskipTests

# clear maven cache.
RUN rm -rf ~/.m2

# Define default command.
ENTRYPOINT java -Xmx2g -XX:+UseG1GC -Dserver.port=8100 -cp bamboo/target/kungfu-panda-bamboo-1.0-shaded.jar org.panda.bamboo.service.Application
