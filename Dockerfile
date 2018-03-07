FROM cern/cc7-base

# Dockerfile metadata
MAINTAINER daniel.lanza@cern.ch
LABEL description="Testbed for CASTOR TapeMonCompaction project"

# Install Apache Maven (for packing projects)
RUN curl https://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo > /etc/yum.repos.d/epel-apache-maven && sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo && yum install -y apache-maven java bc

# Run maven to download binaries and dependencies
ENV SHARED_DIR="/tmp/repository/"
COPY ./pom.xml $SHARED_DIR/
RUN (cd $SHARED_DIR/; mvn dependency:resolve)

# Install make and rpmbuild (for building RPMs)
RUN yum install -y make rpm-build
RUN mkdir -p ~/rpmbuild/{BUILD,RPMS,SOURCES,SPECS,SRPMS}

# Install Koji (for releasing RPM)
RUN yum --disablerepo=extras -y install koji
COPY ./koji.conf /root/.koji/config

CMD /usr/bin/sleep infinity
