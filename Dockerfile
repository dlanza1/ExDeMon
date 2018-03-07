FROM cern/cc7-base

# Dockerfile metadata
MAINTAINER daniel.lanza@cern.ch
LABEL description="Testbed for ExDeMon project"

# Install Apache Maven (for packing projects)
RUN curl https://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo > /etc/yum.repos.d/epel-apache-maven.repo
RUN sed -i s/\$releasever/7/g /etc/yum.repos.d/epel-apache-maven.repo
RUN yum install -y which java apache-maven

# Run Maven to download dependencies
ENV SHARED_DIR="/tmp/repository/"
COPY ./pom.xml $SHARED_DIR/
RUN (cd $SHARED_DIR/; mvn versions:set -DnewVersion=1-DOCKER_GENERATION)
RUN (cd $SHARED_DIR/; mvn package clean)

# Install make and rpmbuild (for building RPMs)
RUN yum install -y make rpm-build
RUN mkdir -p ~/rpmbuild/{BUILD,RPMS,SOURCES,SPECS,SRPMS}

# Install Koji (for releasing RPM)
RUN yum --disablerepo=extras -y install koji
COPY ./koji.conf /root/.koji/config

CMD /usr/bin/sleep infinity
