FROM golang:1.9-stretch AS builder
MAINTAINER Hector Sanjuan <hector@protocol.ai>

# This build state just builds the cluster binaries

ENV GOPATH     /go
ENV SRC_PATH   $GOPATH/src/github.com/ipfs/ipfs-cluster

COPY . $SRC_PATH
WORKDIR $SRC_PATH
RUN make install

#------------------------------------------------------
FROM ipfs/go-ipfs
MAINTAINER Hector Sanjuan <hector@protocol.ai>

# This is the container which just puts the previously
# built binaries on the go-ipfs-container.

ENV GOPATH     /go
ENV SRC_PATH   /go/src/github.com/ipfs/ipfs-cluster
ENV IPFS_CLUSTER_PATH /data/ipfs-cluster

EXPOSE 9094
EXPOSE 9095
EXPOSE 9096

COPY --from=builder $GOPATH/bin/ipfs-cluster-service /usr/local/bin/ipfs-cluster-service
COPY --from=builder $GOPATH/bin/ipfs-cluster-service /usr/local/bin/ipfs-cluster-ctl
COPY --from=builder $SRC_PATH/docker/entrypoint.sh /usr/local/bin/start-daemons.sh

RUN mkdir -p $IPFS_CLUSTER_PATH && \
    chown 1000:100 $IPFS_CLUSTER_PATH

VOLUME $IPFS_CLUSTER_PATH
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/start-daemons.sh"]

CMD ["$IPFS_CLUSTER_OPTS"]
