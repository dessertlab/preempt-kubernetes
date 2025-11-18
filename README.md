# Preempt-Kubernetes (K8s)

This repository contains the code for Preempt-Kubernetes: a Kubernetes dowstream that provides SLO-aware orchestration times. 
The aim of this project is adapt the Kubernetes control plane to cope with soft real-time and latency-aware serverless/cloud native systems.
For more info, please refer to the paper ["SLO-aware Prioritization of Orchestration Times for Containerized Services"](https://dl.acm.org/doi/10.1145/3767329).

This repo modifies the upstream Kubernetes Kube-controller-manager and Kubelet to make them multi-priority and (optionally) synchronous. The Kube-apiserver remains unmodified, but during the experiments of the paper it was properly configured to have exempt flows. Please refer to the official Kubernetes doc for such configuration.


## Quick start

The following sections describe how to build and run the kube-controller-manager and the kubelet of Preempt-K8s.

### Kube-controller-manager
To compile and run the Preempt-K8s Kube-controller-manager, follow these steps:
* Compile the pods of the control plane pods with the source code in the periodic_controller branch to build the synchronous version of the kube-controller-manager, use the multi_prio onlybranch to have only the multi-priority features with the classic asynchronous behavior.
The fastest way to compile the control plane pods is by running:
```make quick-release-images```
This commands builds all the pods of the control plane components in a containerized environment. Therefore, it is suggested to run the command not in a container. There are no particular pre-requirements to run this commands apart from a container engine running on the system.
For additional options in the compilation, please refer to the Kubernetes documentation and to its Makefile. Other useful commands can be ```make release-in-a-container``` and ```make release```
The container images built in the compilation process can be found compressed in tar format in _output/release-images/{arch}/{component}.tar
* Copy the tar to the control plane node(s)
``` bash
scp {component}.tar {user}@{ip}:/{destination_path}/ 
```
* Import the container image into the container manager of the control plane.
In the case of containerd, this can be done via 
```
ctr -n k8s.io -i load /{destination_path}/{component}.tar
```
The k8s.io namespace is essential for Kubernetes to see the patched component image.
* Modify the manifest to use the Preempt-K8s component. 
In /etc/kubernetes/manifest/{component}.yaml rename the container image to the the name of the imported image.

The Preempt-K8s component should be running now!

### Kubelet
* The modified Kubelet is implemented in the multiprio_only branch. Checkout to that branch.
* Build the Kubelet
Since the Kubelet usually runs as a systemd daemon, it is not compiled together with the container images for control plane components. For this reason, it must be built separately.
It can be built in different ways. We advice using (on the compiling machine)
```make all WHAT=cmd/kubelet GOFLAGS=-v```
This command relies on the Go compiler installed in the environment, therefore take care of checking the GLIBC version used by the Go compiler and the one used in the target environemnt. It is adviced to create a building container with ad hoc packages and respective versions for this operation.
* Copy the Kubelet to the target worker node(s) and if needed set the correct permissions of the executable 
```
sudo chmod +x _output/{path}/kubelet 
scp _output/{path}/kubelet {user}@{ip}:/{destination_path}
```
* Modify the systemd daemon file to change the executable file (on the target worker node).
Usually in /usr/lib/systemd/system/kubelet.service, modify ExecStart line to {destination_path}/kubelet
* Configure timing parameters (see next section)
* Reload and restart the daemon.
```
sudo systemctl daemon-reload
sudo systemctl restart kubelet
```

## Parameter configuration

A pod is considered low-priority if it has not any label of criticality. The function used to distinguish the priority of a Pod is the following (for both Kube-controller-manager and Kubelet) 

```
func GetPodCriticality(pod *v1.Pod) int {
	criticalityValue := 0
	criticality, exist := pod.Labels["Criticality"]
	if exist {
		value, err := strconv.Atoi(criticality)
		if err == nil {
			criticalityValue = value
		}
	}
	
```

### Kube-controller-manager

The Kube-controller-manager has no configuration parameter exposed to the user in the brach multiprio_only (no timing is used here).
The number of queues with different priorities is defined by the constant ```const CRITICALITIES = 3``` in the file queue.go

In the branch periodic_controller, the controller has synchronous behavior. This is implemented through the split between 
```
func (rsc *ReplicaSetController) processNextWorkItemAsSoonAsPossible(ctx context.Context) bool {
``` 
and 
```
func (rsc *ReplicaSetController) processNextWorkItem(ctx context.Context) bool {
```
when processing the next element, for each controller. The ```processNextWorkItem``` function calls the ```WaitPeriod``` function of the periodManager, which is an object added to each controller. For example, see the following initialization line:
```
periodMan: controllerutil.NewPeriodManager(60,1000,1),
```
The parameters are, in order, ```minPeriod, maxPeriod, and workersNumber ```. The timing parameters are hardcoded since they are different for each controller. A clean configuration from user perspective has not been implemented yet, and a recompilation of the controller is necessary every time these parameter change. At the moment the period does not vary over time, and the minPeriod parameter is used to configure a ticker.

### Kubelet
The configuration parameters introduced by the Preempt-K8s kubelet are:
```
ReservedPodOpeningTime metav1.Duration
ReservedPodOpeningTimeReset metav1.Duration
ReservedPodOpeningTimeRescale float32
```
These parameters must be inserted in the Kubelet configuration file specified in the /usr/lib/systemd/system/kubelet.service. Alternatively, they can also be specified by command line arguments to the Kubelet.
* ReservedPodOpeningTime is the initial time the Kubelet waits before starting the creation of another non-high-priority Pod.
* ReservedPodOpeningTimeReset is the the time after which the timing parameters of the Kubelet are reset (i.e., the time between two Pod creation is reset to ReservedPodOpeningTime)
* ReservedPodOpeningTimeRescale is the parameter that defines the reduction of the time between two consecutive low-priority Pod creations. In other words, ReservedPodOpeningTime_t+1 := ReservedPodOpeningTime_t / ReservedPodOpeningTimeRescale




# Kuberentes Readme

[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/569/badge)](https://bestpractices.coreinfrastructure.org/projects/569) [![Go Report Card](https://goreportcard.com/badge/github.com/kubernetes/kubernetes)](https://goreportcard.com/report/github.com/kubernetes/kubernetes) ![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/kubernetes/kubernetes?sort=semver)

<img src="https://github.com/kubernetes/kubernetes/raw/master/logo/logo.png" width="100">

----

Kubernetes, also known as K8s, is an open source system for managing [containerized applications]
across multiple hosts. It provides basic mechanisms for the deployment, maintenance,
and scaling of applications.

Kubernetes builds upon a decade and a half of experience at Google running
production workloads at scale using a system called [Borg],
combined with best-of-breed ideas and practices from the community.

Kubernetes is hosted by the Cloud Native Computing Foundation ([CNCF]).
If your company wants to help shape the evolution of
technologies that are container-packaged, dynamically scheduled,
and microservices-oriented, consider joining the CNCF.
For details about who's involved and how Kubernetes plays a role,
read the CNCF [announcement].

----

## To start using K8s

See our documentation on [kubernetes.io].

Take a free course on [Scalable Microservices with Kubernetes].

To use Kubernetes code as a library in other applications, see the [list of published components](https://git.k8s.io/kubernetes/staging/README.md).
Use of the `k8s.io/kubernetes` module or `k8s.io/kubernetes/...` packages as libraries is not supported.

## To start developing K8s

The [community repository] hosts all information about
building Kubernetes from source, how to contribute code
and documentation, who to contact about what, etc.

If you want to build Kubernetes right away there are two options:

##### You have a working [Go environment].

```
mkdir -p $GOPATH/src/k8s.io
cd $GOPATH/src/k8s.io
git clone https://github.com/kubernetes/kubernetes
cd kubernetes
make
```

##### You have a working [Docker environment].

```
git clone https://github.com/kubernetes/kubernetes
cd kubernetes
make quick-release
```

For the full story, head over to the [developer's documentation].

## Support

If you need support, start with the [troubleshooting guide],
and work your way through the process that we've outlined.

That said, if you have questions, reach out to us
[one way or another][communication].

[announcement]: https://cncf.io/news/announcement/2015/07/new-cloud-native-computing-foundation-drive-alignment-among-container
[Borg]: https://research.google.com/pubs/pub43438.html
[CNCF]: https://www.cncf.io/about
[communication]: https://git.k8s.io/community/communication
[community repository]: https://git.k8s.io/community
[containerized applications]: https://kubernetes.io/docs/concepts/overview/what-is-kubernetes/
[developer's documentation]: https://git.k8s.io/community/contributors/devel#readme
[Docker environment]: https://docs.docker.com/engine
[Go environment]: https://go.dev/doc/install
[kubernetes.io]: https://kubernetes.io
[Scalable Microservices with Kubernetes]: https://www.udacity.com/course/scalable-microservices-with-kubernetes--ud615
[troubleshooting guide]: https://kubernetes.io/docs/tasks/debug/

## Community Meetings 

The [Calendar](https://www.kubernetes.dev/resources/calendar/) has the list of all the meetings in the Kubernetes community in a single location.

## Adopters

The [User Case Studies](https://kubernetes.io/case-studies/) website has real-world use cases of organizations across industries that are deploying/migrating to Kubernetes.

## Governance 

Kubernetes project is governed by a framework of principles, values, policies and processes to help our community and constituents towards our shared goals.

The [Kubernetes Community](https://github.com/kubernetes/community/blob/master/governance.md) is the launching point for learning about how we organize ourselves.

The [Kubernetes Steering community repo](https://github.com/kubernetes/steering) is used by the Kubernetes Steering Committee, which oversees governance of the Kubernetes project.

## Roadmap 

The [Kubernetes Enhancements repo](https://github.com/kubernetes/enhancements) provides information about Kubernetes releases, as well as feature tracking and backlogs.
