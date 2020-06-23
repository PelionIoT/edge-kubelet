# edge-kubelet

----

edge-kubelet is a modified version of Kubernetes' kubelet.  The modifications enable kubelet to interact with the Pelion cloud services.  This repository contains everything necessary to compile edge-kubelet for an edge client gateway.  

----

# compiling edge-kubelet

In general, to compile edge-kubelet use a working [Go environment].

```
$ mkdir -p $GOPATH/src/k8s.io
$ git clone https://github.com/armPelionEdge/edge-kubelet.git $GOPATH/src/k8s.io/kubernetes
$ cd $GOPATH/src/k8s.io/kubernetes
$ make kubelet
```

* Note: Due to the modifications to Kubernetes, this version of edge-kubelet should not be used to compile anything other kubernetes programs as they will most likely not work as expected.
