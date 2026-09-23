/*
Copyright The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permsssions and
limstations under the License.
*/

package controller

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextClientSet "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	listerv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	volcano "volcano.sh/apis/pkg/client/clientset/versioned"

	clientset "github.com/volcano-sh/kthena/client-go/clientset/versioned"
	informersv1alpha1 "github.com/volcano-sh/kthena/client-go/informers/externalversions"
	listerv1alpha1 "github.com/volcano-sh/kthena/client-go/listers/workload/v1alpha1"
	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/datastore"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/plugins"
	_ "github.com/volcano-sh/kthena/pkg/model-serving-controller/plugins/ranktable"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/podgroupmanager"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
)

const (
	// enqueueAfter is the time duration to wait to re-enqueue:
	enqueueAfter = 1 * time.Second
	// Role deletion is normally cache-driven. Recheck it explicitly and use a
	// narrowly scoped live lookup after repeated stale cache observations.
	roleDeletionRecheckDelay       = 1 * time.Second
	roleDeletionLiveCheckThreshold = 2
	modelServingFullSyncPeriod     = 5 * time.Minute

	GroupNameKey = "GroupName"
	RoleIDKey    = "RoleID"
)

// PodGroupManager is the interface for managing PodGroups.
// This interface allows for dependency injection in tests.
type PodGroupManager interface {
	CreateOrUpdatePodGroup(ctx context.Context, ms *workloadv1alpha1.ModelServing, pgName string) (error, time.Duration)
	DeletePodGroup(ctx context.Context, ms *workloadv1alpha1.ModelServing, servingGroupName string) error
	CleanupPodGroups(ctx context.Context, ms *workloadv1alpha1.ModelServing) error
	HasPodGroupCRD() bool
	GetPodGroupInformer() cache.SharedIndexInformer
	Run(parentCtx context.Context) error
	GenerateTaskName(roleName string, roleIndex int) string
	AnnotatePodWithPodGroup(pod *corev1.Pod, ms *workloadv1alpha1.ModelServing, groupName, taskName string)
}

type podGracePeriodKey struct {
	types.NamespacedName
	UID types.UID
}

func getPodGracePeriodKey(pod *corev1.Pod) podGracePeriodKey {
	return podGracePeriodKey{
		NamespacedName: utils.GetNamespaceName(pod),
		UID:            pod.UID,
	}
}

type ModelServingController struct {
	// Per-reconcile views share the queue/runtime, never the informer indexers.
	shared             *ModelServingController
	audit              *auditRuntime
	observation        *servingObservation
	servingState       *servingAuditState
	volcanoClient      volcano.Interface
	auditPeriod        time.Duration
	auditTimeout       time.Duration
	reconcileContext   context.Context
	kubeClientSet      kubernetes.Interface
	modelServingClient clientset.Interface

	syncHandler           func(ctx context.Context, msKey string) error
	podGroupManager       PodGroupManager
	podsLister            listerv1.PodLister
	podsInformer          cache.SharedIndexInformer
	servicesLister        listerv1.ServiceLister
	servicesInformer      cache.SharedIndexInformer
	configMapsLister      listerv1.ConfigMapLister
	configMapsInformer    cache.SharedIndexInformer
	modelServingLister    listerv1alpha1.ModelServingLister
	modelServingsInformer cache.SharedIndexInformer

	// nolint
	workqueue       workqueue.RateLimitingInterface
	store           datastore.Store
	graceMap        sync.Map    // key: podGracePeriodKey, value:time
	roleDeleteMap   sync.Map    // key: namespace/name/group/role/roleID, value:int
	initialSync     atomic.Bool // indicates whether initial keys have been enqueued
	fullSyncPeriod  time.Duration
	pluginsRegistry *plugins.Registry
	recorder        record.EventRecorder
}

func NewModelServingController(kubeClientSet kubernetes.Interface, modelServingClient clientset.Interface, volcanoClient volcano.Interface, apiextClient apiextClientSet.Interface) (*ModelServingController, error) {
	selector, err := labels.NewRequirement(workloadv1alpha1.GroupNameLabelKey, selection.Exists, nil)
	if err != nil {
		return nil, fmt.Errorf("cannot create label selector, err: %v", err)
	}

	// Register ModelServing types in the global scheme for event recording.
	if err := workloadv1alpha1.Install(scheme.Scheme); err != nil {
		return nil, fmt.Errorf("failed to register ModelServing API scheme: %v", err)
	}

	kubeInformerFactory := informers.NewSharedInformerFactoryWithOptions(
		kubeClientSet,
		0,
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = selector.String()
		}),
	)
	podsInformer := kubeInformerFactory.Core().V1().Pods()
	servicesInformer := kubeInformerFactory.Core().V1().Services()
	configMapsInformer := kubeInformerFactory.Core().V1().ConfigMaps()
	modelServingInformerFactory := informersv1alpha1.NewSharedInformerFactory(modelServingClient, 0)
	modelServingInformer := modelServingInformerFactory.Workload().V1alpha1().ModelServings()

	err = podsInformer.Informer().AddIndexers(cache.Indexers{
		GroupNameKey: utils.GroupNameIndexFunc,
		RoleIDKey:    utils.RoleIDIndexFunc,
	})
	if err != nil {
		return nil, fmt.Errorf("cannot create pod Informer Index, err: %v", err)
	}

	store := datastore.New()

	// setup event broadcaster & recorder
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartStructuredLogging(0)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{
		Interface: kubeClientSet.CoreV1().Events(""),
	})
	recorder := eventBroadcaster.NewRecorder(
		scheme.Scheme,
		corev1.EventSource{Component: "modelserving-controller"},
	)

	c := &ModelServingController{
		audit:                 newAuditRuntime(),
		volcanoClient:         volcanoClient,
		auditPeriod:           5 * time.Minute,
		auditTimeout:          30 * time.Second,
		kubeClientSet:         kubeClientSet,
		modelServingClient:    modelServingClient,
		podGroupManager:       nil,
		podsLister:            podsInformer.Lister(),
		podsInformer:          podsInformer.Informer(),
		servicesLister:        servicesInformer.Lister(),
		servicesInformer:      servicesInformer.Informer(),
		configMapsLister:      configMapsInformer.Lister(),
		configMapsInformer:    configMapsInformer.Informer(),
		modelServingLister:    modelServingInformer.Lister(),
		modelServingsInformer: modelServingInformer.Informer(),
		// nolint
		workqueue:       workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "ModelServings"),
		store:           store,
		fullSyncPeriod:  modelServingFullSyncPeriod,
		pluginsRegistry: plugins.DefaultRegistry,
		recorder:        recorder,
	}

	registerPodGroupHandler := func(pgInformer cache.SharedIndexInformer) {
		if c == nil || pgInformer == nil {
			return
		}
		_, _ = pgInformer.AddEventHandler(cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				metaObj := getMetaObject(obj)
				if metaObj == nil {
					return false
				}
				return isOwnedByModelServing(metaObj)
			},
			Handler: cache.ResourceEventHandlerFuncs{
				DeleteFunc: func(obj interface{}) {
					c.queueChildObservation(obj, true)
				},
			},
		})
	}

	c.podGroupManager = podgroupmanager.NewManager(kubeClientSet, volcanoClient, apiextClient, registerPodGroupHandler)

	klog.Info("Set the ModelServing event handler")
	_, _ = c.modelServingsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.queueModelServingObservation(nil, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.queueModelServingObservation(oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			c.queueModelServingObservation(obj, nil)
		},
	})

	_, _ = c.podsInformer.AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			metaObj := getMetaObject(obj)
			if metaObj == nil {
				return false
			}
			return isOwnedByModelServing(metaObj) || isLabeledForModelServing(metaObj)
		},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				c.queueChildObservation(obj, false)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				c.queueChildObservation(newObj, false)
			},
			DeleteFunc: func(obj interface{}) {
				c.queueChildObservation(obj, true)
			},
		},
	})

	_, _ = c.servicesInformer.AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			metaObj := getMetaObject(obj)
			if metaObj == nil {
				return false
			}
			return isOwnedByModelServing(metaObj)
		},
		Handler: cache.ResourceEventHandlerFuncs{
			DeleteFunc: func(obj interface{}) {
				c.queueChildObservation(obj, true)
			},
		},
	})

	c.syncHandler = c.reconcileModelServing

	return c, nil
}

func (c *ModelServingController) addModelServing(obj interface{}) {
	ms, ok := obj.(*workloadv1alpha1.ModelServing)
	if !ok {
		klog.Errorf("failed to parse ModelServing %#v", obj)
		return
	}
	klog.V(4).InfoS("Adding", "modelServing", klog.KObj(ms))
	c.enqueueModelServing(ms)
}

func (c *ModelServingController) updateModelServing(old, cur interface{}) {
	curms, ok := cur.(*workloadv1alpha1.ModelServing)
	if !ok {
		klog.Errorf("failed to parse new ModelServing type when update %#v", cur)
		return
	}
	oldms, ok := old.(*workloadv1alpha1.ModelServing)
	if !ok {
		klog.Errorf("failed to parse old ModelServing type when update %#v", old)
		return
	}

	if reflect.DeepEqual(oldms.Spec, curms.Spec) {
		// If the spec has not changed, we do not need to reconcile.
		klog.V(4).InfoS("Spec has not changed, skipping update", "modelServing", klog.KObj(curms))
		return
	}

	// If network topology is removed, we need to clean up the PodGroups.
	// Because minRoleReplicas is not allowed to be updated, so we do not need to check it here.
	if oldms.Spec.Template.NetworkTopology != nil && curms.Spec.Template.NetworkTopology == nil {
		if curms.Spec.Template.GangPolicy == nil || len(curms.Spec.Template.GangPolicy.MinRoleReplicas) == 0 {
			if err := c.podGroupManager.CleanupPodGroups(c.operationContext(), curms); err != nil {
				klog.Errorf("failed to clean up PodGroups for ModelServing %s/%s: %v", curms.Namespace, curms.Name, err)
			}
		}
	}

	c.enqueueModelServing(curms)
}

func (c *ModelServingController) deleteModelServing(obj interface{}) {
	ms, ok := obj.(*workloadv1alpha1.ModelServing)
	if !ok {
		// If the object is not a ModelServing, it might be a tombstone object.
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("failed to parse ModelServing type when delete %#v", obj)
			return
		}
		ms, ok = tombstone.Obj.(*workloadv1alpha1.ModelServing)
		if !ok {
			klog.Errorf("failed to parse ModelServing from tombstone %#v", tombstone.Obj)
			return
		}
	}

	c.store.DeleteModelServing(types.NamespacedName{
		Namespace: ms.Namespace,
		Name:      ms.Name,
	})
	// ControllerRevisions will be automatically deleted via OwnerReference when ModelServing is deleted
}

func (c *ModelServingController) addPod(obj interface{}) {
	c.updatePod(nil, obj)
}

func (c *ModelServingController) updatePod(_, newObj interface{}) {
	newPod, ok := newObj.(*corev1.Pod)
	if !ok {
		klog.Error("failed to parse newPod type when updatePod")
		return
	}
	klog.V(4).Infof("updatePod: %s/%s, %v", newPod.Namespace, newPod.Name, newPod.Status.Phase)

	if newPod.DeletionTimestamp != nil {
		// If the pod is being deleted, we do not need to handle it.
		// After deleted，following work will be done in deletePod.
		return
	}

	ms, servingGroupName, err := c.getModelServingByChildResource(newPod)
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.V(4).Infof("modelServing of pod %s has been deleted", newPod.Name)
		} else {
			klog.Errorf("get model Serving failed when update pod %s/%s: %v", newPod.Namespace, newPod.Name, err)
		}
		return
	}

	if c.shouldSkipHandling(ms, servingGroupName, newPod) {
		// Labeled orphan Pods are included in the event handler so reconciliation
		// can actively remove a stale object occupying a deterministic Pod name.
		c.enqueueModelServing(ms)
		return
	}

	if newPod.Status.Phase == corev1.PodRunning {
		if err = c.handleRunningPod(ms, servingGroupName, newPod); err != nil {
			klog.Errorf("handle running pod %s/%s failed for ModelServing %s/%s: %v", newPod.Namespace, newPod.Name, ms.Namespace, ms.Name, err)
		}
	}

	switch {
	case utils.IsPodRunningAndReady(newPod):
		klog.V(4).Infof("handleReadyPod: %s/%s", newPod.Namespace, newPod.Name)
		// The pod is available, that is, the state is running, and the container is ready
		err = c.handleReadyPod(ms, servingGroupName, newPod)
		if err != nil {
			klog.Errorf("handle running pod %s/%s failed for ModelServing %s/%s: %v", newPod.Namespace, newPod.Name, ms.Namespace, ms.Name, err)
		}
	case utils.IsPodFailed(newPod) || utils.ContainerRestarted(newPod):
		klog.V(4).Infof("handleErrorPod: %s/%s", newPod.Namespace, newPod.Name)
		err = c.handleErrorPod(ms, servingGroupName, newPod)
		if err != nil {
			klog.Errorf("handle error pod %s/%s failed for ModelServing %s/%s: %v", newPod.Namespace, newPod.Name, ms.Namespace, ms.Name, err)
		}
	default:
		klog.V(4).Infof("handleDefault: %s/%s", newPod.Namespace, newPod.Name)
		if !c.initialSync.Load() {
			roleName := utils.GetRoleName(newPod)
			roleTemplateHash := c.resolveRoleTemplateHash(ms, roleName, newPod)
			c.store.AddServingGroupAndRole(types.NamespacedName{
				Namespace: ms.Namespace,
				Name:      ms.Name,
			}, servingGroupName, utils.ObjectRevision(newPod), roleTemplateHash, roleName, utils.GetRoleID(newPod))
		}
	}
}

func (c *ModelServingController) deletePod(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		// If the object is not a Pod, it might be a tombstone object.
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Error("failed to parse pod type when deletePod")
			return
		}
		pod, ok = tombstone.Obj.(*corev1.Pod)
		if !ok {
			klog.Errorf("failed to parse Pod from tombstone %#v", tombstone.Obj)
			return
		}
	}

	ms, servingGroupName, roleName, roleID := c.getModelServingAndResourceDetails(pod)
	// ms is nil means the modelserving is deleted
	// delete the pod
	if ms == nil {
		klog.Warningf("ModelServing of deleted pod: %s not found, might be already deleted", pod.Name)
		c.enqueueModelServingByChildResourceAfter(pod, enqueueAfter)
		return
	}

	// Remove the pod from running pods in the store
	c.store.DeleteRunningPodFromServingGroup(utils.GetNamespaceName(ms), servingGroupName, pod.Name)

	// skip handling if pod revision mismatches serving group revision or owner mismatch
	if c.shouldSkipHandling(ms, servingGroupName, pod) {
		return
	}

	chain, err := c.buildPluginChain(ms)
	if err != nil {
		klog.Errorf("failed to build plugin chain for deleted pod %s/%s: %v", pod.Namespace, pod.Name, err)
	} else if chain != nil {
		if err := chain.OnPodDelete(c.operationContext(), &plugins.HookRequest{
			ModelServing:    ms,
			ServingGroup:    servingGroupName,
			RoleName:        roleName,
			RoleID:          roleID,
			IsEntry:         pod.Labels[workloadv1alpha1.EntryLabelKey] == utils.Entry,
			Pod:             pod,
			PodLister:       c.podsLister,
			ConfigMapLister: c.configMapsLister,
			KubeClient:      c.kubeClientSet,
			ServiceLister:   c.servicesLister,
		}); err != nil {
			klog.Errorf("failed to execute OnPodDelete for pod %s/%s: %v", pod.Namespace, pod.Name, err)
		}
	}

	if c.handleDeletionInProgress(ms, servingGroupName, roleName, roleID) {
		return
	}

	err = c.handleDeletedPod(ms, servingGroupName, pod)
	if err != nil {
		klog.Errorf("handle deleted pod %s/%s failed in ServingGroup %s of ModelServing %s/%s: %v", pod.Namespace, pod.Name, servingGroupName, ms.Namespace, ms.Name, err)
	}
}

func (c *ModelServingController) deleteService(obj interface{}) {
	svc, ok := obj.(*corev1.Service)
	if !ok {
		// If the object is not a Service, it might be a tombstone object.
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Error("failed to parse service type when deleteService")
			return
		}
		svc, ok = tombstone.Obj.(*corev1.Service)
		if !ok {
			klog.Errorf("failed to parse Service from tombstone %#v", tombstone.Obj)
			return
		}
	}

	ms, servingGroupName, roleName, roleID := c.getModelServingAndResourceDetails(svc)
	// ms is nil means the modelserving is deleted
	if ms == nil {
		klog.Warningf("ModelServing of deleted service: %s not found, might be already deleted", svc.Name)
		c.enqueueModelServingByChildResourceAfter(svc, enqueueAfter)
		return
	}

	// skip handling if service revision mismatches serving group revision or owner mismatch
	if c.shouldSkipHandling(ms, servingGroupName, svc) {
		return
	}

	if c.handleDeletionInProgress(ms, servingGroupName, roleName, roleID) {
		return
	}

	klog.V(4).Infof("Service %s/%s deleted, enqueuing ModelServing %s for reconcile", svc.GetNamespace(), svc.GetName(), ms.Name)
	c.enqueueModelServing(ms)
}

func (c *ModelServingController) deletePodGroup(obj interface{}) {
	pg, ok := obj.(*schedulingv1beta1.PodGroup)
	if !ok {
		// If the object is not a PodGroup, it might be a tombstone object.
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Error("failed to parse podgroup type when deletePodGroup")
			return
		}
		pg, ok = tombstone.Obj.(*schedulingv1beta1.PodGroup)
		if !ok {
			klog.Errorf("failed to parse PodGroup from tombstone %#v", tombstone.Obj)
			return
		}
	}

	ms, servingGroupName, _, _ := c.getModelServingAndResourceDetails(pg)
	// ms is nil means the modelserving is deleted
	if ms == nil {
		klog.Warningf("ModelServing of deleted podGroup: %s not found, might be already deleted", pg.Name)
		return
	}

	// skip handling if podGroup revision mismatches serving group revision or owner mismatch
	if c.shouldSkipHandling(ms, servingGroupName, pg) {
		return
	}

	// If servingGroup is deleting, skip handling.
	if c.handleDeletionInProgress(ms, servingGroupName, "", "") {
		return
	}

	klog.V(4).Infof("podGroup %s/%s deleted, enqueuing ModelServing %s for reconcile", pg.GetNamespace(), pg.GetName(), ms.Name)
	c.enqueueModelServing(ms)
}

func (c *ModelServingController) enqueueModelServing(ms *workloadv1alpha1.ModelServing) {
	if c.workqueue == nil {
		return
	}
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(ms); err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
}

func (c *ModelServingController) enqueueModelServingAfter(ms *workloadv1alpha1.ModelServing, duration time.Duration) {
	if c.workqueue == nil {
		return
	}
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(ms); err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.workqueue.AddAfter(key, duration)
}

func (c *ModelServingController) enqueueModelServingKeyAfter(key string, duration time.Duration) {
	if key == "" || c.workqueue == nil {
		return
	}
	c.workqueue.AddAfter(key, duration)
}

func (c *ModelServingController) enqueueModelServingByChildResourceAfter(obj metav1.Object, duration time.Duration) {
	key, ok := modelServingKeyFromChildResource(obj)
	if !ok {
		return
	}
	c.enqueueModelServingKeyAfter(key, duration)
}

func (c *ModelServingController) worker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *ModelServingController) processNextWorkItem(ctx context.Context) bool {
	key, quit := c.workqueue.Get()
	if quit {
		return false
	}
	defer c.workqueue.Done(key)

	err := c.syncHandler(ctx, key.(string))
	if err == nil {
		c.workqueue.Forget(key)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("sync %q failed with %v", key, err))
	c.workqueue.AddRateLimited(key)

	return true
}

func (c *ModelServingController) syncModelServing(ctx context.Context, key string) error {
	klog.V(4).InfoS("Started syncing ModelServing", "key", key)
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return fmt.Errorf("invalid resource key: %s", err)
	}

	ms, err := c.modelServingLister.ModelServings(namespace).Get(name)
	if apierrors.IsNotFound(err) {
		klog.V(4).Infof("%v has been deleted", key)
		return nil
	}
	if err != nil {
		return err
	}

	ctx = c.withRevisionHistory(ctx, ms)
	revision, err := c.revisionHistory(ctx, ms).desiredRevision(ctx)
	if err != nil {
		return errors.Join(fmt.Errorf("resolve desired revision: %w", err), c.scaleDownOnRevisionError(ctx, ms))
	}
	if err := c.persistCoordinatedRoleRevision(ctx, ms, revision); err != nil {
		return fmt.Errorf("failed to persist coordinated Role revision: %v", err)
	}
	if c.observation != nil {
		if err := c.reconcileObservation(ctx, ms); err != nil {
			// Publish unavailable membership without advancing rollout after a
			// required lifecycle hook has failed.
			if statusErr := c.updateModelServingStatus(ctx, ms, revision, nil); statusErr != nil {
				klog.ErrorS(statusErr, "Failed to publish audit failure readiness")
			}
			return err
		}
	}
	// 1. Sync the number of ServingGroups to match the expected replicas defined in spec.
	if err := c.syncServingGroupReplicas(ctx, ms, revision); err != nil {
		return fmt.Errorf("failed to sync ServingGroup replicas: %v", err)
	}

	// Derive optional dependency and proportional limits from the current Role state.
	rolloutPolicy, err := c.resolveRoleRolloutPolicy(ctx, ms, revision)
	if err != nil {
		return fmt.Errorf("failed to resolve Role rollout policy: %v", err)
	}

	// 2. Sync the roles and their replicas within each ServingGroup, handling partitioned scaling and revisions.
	if err := c.syncRoleReplicas(ctx, ms, revision, rolloutPolicy); err != nil {
		return fmt.Errorf("failed to sync role replicas: %v", err)
	}

	// 3. Handle the rolling update process, deleting outdated ServingGroups/Roles to trigger updates.
	if err := c.manageRollingUpdate(ctx, ms, revision, rolloutPolicy); err != nil {
		return fmt.Errorf("failed to handle rollingUpdate: %v", err)
	}

	// 4. Calculate and update the overall condition and replica status fields of the ModelServing.
	if err := c.updateModelServingStatus(ctx, ms, revision, rolloutPolicy); err != nil {
		return fmt.Errorf("failed to update status of ms %s/%s: %v", namespace, name, err)
	}

	// Report unresolved templates after scoped safe work has completed. This
	// preserves workqueue error handling without blocking unrelated replicas.
	var revisionErrors []error
	for _, snapshot := range c.revisionHistory(ctx, ms).snapshots {
		if snapshot.err != nil {
			revisionErrors = append(revisionErrors, snapshot.err)
		}
	}
	return errors.Join(revisionErrors...)
}

func (c *ModelServingController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()

	// start informers
	go c.podsInformer.RunWithContext(ctx)
	go c.servicesInformer.RunWithContext(ctx)
	go c.configMapsInformer.RunWithContext(ctx)
	go c.modelServingsInformer.RunWithContext(ctx)

	if err := c.podGroupManager.Run(ctx); err != nil {
		klog.Errorf("failed to start PodGroup informer: %v", err)
	}

	if !cache.WaitForCacheSync(ctx.Done(),
		c.podsInformer.HasSynced,
		c.servicesInformer.HasSynced,
		c.configMapsInformer.HasSynced,
		c.modelServingsInformer.HasSynced,
	) {
		return
	}

	// Startup, watch notifications and audits all enter the same worker path.
	if err := c.enqueuePeriodicAudit(); err != nil {
		klog.ErrorS(err, "Failed to enqueue initial ModelServing audit")
	}
	c.initialSync.Store(true)
	klog.Info("initial sync has been done")

	klog.Info("start modelServing controller")
	for i := 0; i < workers; i++ {
		go c.worker(ctx)
	}
	go c.runPeriodicAudit(ctx)
	go c.runPeriodicFullSync(ctx)
	<-ctx.Done()
	klog.Info("shut down modelServing controller")
}

func (c *ModelServingController) runPeriodicFullSync(ctx context.Context) {
	if c.fullSyncPeriod <= 0 {
		return
	}
	ticker := time.NewTicker(c.fullSyncPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			klog.V(4).Info("running periodic ModelServing full sync")
			c.syncAll()
		}
	}
}

func (c *ModelServingController) syncAll() {
	pods, err := c.podsLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("failed to list pods: %v", err)
	}

	for _, pod := range pods {
		c.addPod(pod)
	}

	modelServings, err := c.modelServingLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("failed to list model servings: %v", err)
	}
	for _, ms := range modelServings {
		c.addModelServing(ms)
	}

	c.initialSync.Store(true)
}

// syncServingGroupReplicas scales up or down whole ServingGroups to meet the top-level
// `Spec.Replicas` count of the ModelServing resource.
// Main processing steps:
// 1. Retrieve the list of active ServingGroups for the current ModelServing from the data store.
// 2. Compare the current count of ServingGroups with the expected replicas.
// 3. If scaling up: sequentially initialize needed group states, create PodGroups, and scale up groups.
// 4. If scaling down: sort the groups by priority/deletion-cost and delete the excess groups.
func (c *ModelServingController) syncServingGroupReplicas(ctx context.Context, ms *workloadv1alpha1.ModelServing, newRevision string) error {
	servingGroupList, err := c.store.GetServingGroupByModelServing(utils.GetNamespaceName(ms))
	if err != nil && !errors.Is(err, datastore.ErrServingGroupNotFound) {
		return fmt.Errorf("cannot get servingGroup of modelServing: %s from map: %v", ms.GetName(), err)
	}
	servingGroupList, err = c.pruneDeletedServingGroups(ctx, ms, servingGroupList)
	if err != nil {
		return err
	}
	replicas := modelServingReplicas(ms)
	expectedCount := replicas
	isServingGroupRollingUpdate := ms.Spec.RolloutStrategy == nil || ms.Spec.RolloutStrategy.Type == workloadv1alpha1.ServingGroupRollingUpdate
	if isServingGroupRollingUpdate {
		maxSurge, err := utils.GetMaxSurge(ms)
		if err != nil {
			return fmt.Errorf("failed to calculate maxSurge: %v", err)
		}
		partition, _, partitionErr := c.getPartition(modelServingPartition(ms), replicas)
		if partitionErr != nil {
			return fmt.Errorf("failed to calculate partition: %v", partitionErr)
		}
		if maxSurge > 0 && c.hasUpdateableOutdatedServingGroup(ctx, ms, servingGroupList, newRevision, partition) {
			expectedCount += maxSurge
		}
	}

	curReplicas := len(servingGroupList)
	if curReplicas < expectedCount {
		klog.V(2).Infof("manageServingGroupReplicas: scaling up modelServing=%s (%d -> %d)", utils.GetNamespaceName(ms), curReplicas, expectedCount)
		if err := c.scaleUpServingGroups(ctx, ms, servingGroupList, expectedCount, newRevision); err != nil {
			return fmt.Errorf("failed to scale up ServingGroups: %v", err)
		}
	} else if curReplicas > expectedCount {
		klog.V(2).Infof("manageServingGroupReplicas: scaling down modelServing=%s (%d -> %d)", utils.GetNamespaceName(ms), curReplicas, expectedCount)
		if err := c.scaleDownServingGroups(ctx, ms, servingGroupList, expectedCount); err != nil {
			return fmt.Errorf("failed to scale down ServingGroups: %v", err)
		}
	}

	// Note: in case the role is updated, we need to update pod groups as well.
	// update pod group after scaling down, so that we do not need to update pod group for deleting serving groups
	// Moreover, it is also possible to reconstruct accidentally deleted podGroups here.
	servingGroupList, err = c.store.GetServingGroupByModelServing(utils.GetNamespaceName(ms))
	if err != nil && !errors.Is(err, datastore.ErrServingGroupNotFound) {
		return fmt.Errorf("cannot refresh ServingGroups of modelServing %s: %v", ms.GetName(), err)
	}
	for _, servingGroup := range servingGroupList {
		if servingGroup.Status != datastore.ServingGroupDeleting {
			if err := c.createOrUpdatePodGroupByServingGroup(ctx, ms, servingGroup.Name); err != nil {
				if isRevisionResolutionError(err) {
					continue
				}
				return fmt.Errorf("failed to update PodGroup for ServingGroup %s: %v", servingGroup.Name, err)
			}
		}
	}
	return nil
}

func (c *ModelServingController) pruneDeletedServingGroups(ctx context.Context, ms *workloadv1alpha1.ModelServing, servingGroupList []datastore.ServingGroup) ([]datastore.ServingGroup, error) {
	if len(servingGroupList) == 0 {
		return servingGroupList, nil
	}
	kept := make([]datastore.ServingGroup, 0, len(servingGroupList))
	for _, servingGroup := range servingGroupList {
		if servingGroup.Status != datastore.ServingGroupDeleting || !c.isServingGroupDeleted(ms, servingGroup.Name) {
			kept = append(kept, servingGroup)
			continue
		}
		if err := c.runServingGroupDeletePlugins(ctx, ms, servingGroup.Name); err != nil {
			return nil, fmt.Errorf("complete plugin cleanup for deleted ServingGroup %s: %w", servingGroup.Name, err)
		}
		klog.V(2).Infof("ServingGroup %s has been deleted, removing it from store before replica accounting", servingGroup.Name)
		c.store.DeleteServingGroup(utils.GetNamespaceName(ms), servingGroup.Name)
	}
	return kept, nil
}

// hasUpdateableOutdatedServingGroup derives the temporary capacity requirement
// from observed ServingGroups. The partition protects ordinal values, not
// positions in the datastore's sorted slice.
func (c *ModelServingController) hasUpdateableOutdatedServingGroup(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	groups []datastore.ServingGroup,
	revision string,
	partition int,
) bool {
	for _, group := range groups {
		_, ordinal := utils.GetParentNameAndOrdinal(group.Name)
		if ordinal >= partition && c.compareServingGroupTemplate(ctx, ms, group, revision) == templateDifferent {
			return true
		}
	}
	return false
}

// scaleUpServingGroups fills missing ordinals in [0, expectedCount).
// Missing ordinals below partition use CurrentRevision; the rest use newRevision.
func (c *ModelServingController) scaleUpServingGroups(ctx context.Context, ms *workloadv1alpha1.ModelServing, servingGroupList []datastore.ServingGroup, expectedCount int, newRevision string) error {
	partition, _, _ := c.getPartition(modelServingPartition(ms), modelServingReplicas(ms))
	klog.V(4).Infof("scaleUpServingGroups: start for modelServing=%s, existingGroups=%d, expectedCount=%d, partition=%d, newRevision=%s",
		utils.GetNamespaceName(ms), len(servingGroupList), expectedCount, partition, newRevision)

	existingOrdinals := make([]int, 0, len(servingGroupList))
	for _, group := range servingGroupList {
		_, ordinal := utils.GetParentNameAndOrdinal(group.Name)
		if ordinal < 0 {
			klog.Warningf("scaleUpServingGroups: cannot parse ordinal from ServingGroup %s", group.Name)
			continue
		}
		existingOrdinals = append(existingOrdinals, ordinal)
	}

	toCreate := max(0, expectedCount-len(servingGroupList))
	klog.V(4).Infof("scaleUpServingGroups: modelServing=%s, existingOrdinals=%v, groupsToCreate=%d",
		utils.GetNamespaceName(ms), existingOrdinals, toCreate)

	// Helper function to create a ServingGroup
	createServingGroup := func(ordinal int, revision string, roles []workloadv1alpha1.Role) error {
		groupName := utils.GenerateServingGroupName(ms.Name, ordinal)
		klog.V(4).Infof("scaleUpServingGroups: creating/updating PodGroup for ServingGroup=%s", groupName)
		// Ensure a PodGroup exists for the new ServingGroup when gang scheduling is enabled.
		if err := c.createOrUpdatePodGroupByServingGroupWithRoles(ctx, ms, groupName, roles); err != nil {
			return err
		}
		klog.V(4).Infof("Creating ServingGroup %s at ordinal %d with revision %s", groupName, ordinal, revision)
		// Create pods for ServingGroup using the provided roles template
		if err := c.CreatePodsForServingGroup(ctx, ms, ordinal, revision, roles); err != nil {
			return fmt.Errorf("create Serving group failed: %v", err)
		}
		// Insert new ServingGroup to global storage
		c.store.AddServingGroup(utils.GetNamespaceName(ms), ordinal, revision)
		klog.V(4).Infof("scaleUpServingGroups: ServingGroup=%s added to store (ordinal=%d, revision=%s)", groupName, ordinal, revision)
		return nil
	}

	// Persist the template snapshot before creating a ServingGroup that references
	// newRevision. ModelServing.Spec.Template is mutable, while ControllerRevision
	// lets a future partition-protected recovery resolve this revision's template.
	// Create it lazily because restoring only ordinals below partition uses the
	// existing CurrentRevision and does not need a new snapshot.
	newRevisionCreated := false
	var scaleUpErr error
	forEachMissingOrdinal(expectedCount, existingOrdinals, toCreate, func(ordinal int) bool {
		if partition > 0 && ordinal < partition {
			// Use CurrentRevision for partition-protected ordinals
			revisionToUse := newRevision
			if ms.Status.CurrentRevision != "" {
				revisionToUse = ms.Status.CurrentRevision
			}
			klog.V(4).Infof("scaleUpServingGroups: ordinal %d missing (partition-protected), revisionToUse=%s, currentRevision=%s",
				ordinal, revisionToUse, ms.Status.CurrentRevision)

			// For ordinal < partition, we should use the old template from the revision
			// Two cases:
			// 1. First startup: use ms.Spec.Template.Roles (which corresponds to CurrentRevision)
			// 2. During recovery: use template from ControllerRevision retrieved by revision
			var rolesToUse []workloadv1alpha1.Role
			cr, err := utils.GetControllerRevision(ctx, c.kubeClientSet, ms, revisionToUse)
			if err != nil {
				scaleUpErr = fmt.Errorf("failed to get ControllerRevision %s for protected ordinal %d: %w", revisionToUse, ordinal, err)
				return false
			}
			if cr != nil {
				// Case 2: Recovery scenario - use template from ControllerRevision
				if roles, err := utils.GetRolesFromControllerRevision(cr); err != nil {
					scaleUpErr = fmt.Errorf("failed to get roles from ControllerRevision %s for protected ordinal %d: %w", revisionToUse, ordinal, err)
					return false
				} else {
					rolesToUse = mergeLatestRoleReplicas(roles, ms.Spec.Template.Roles)
					klog.V(4).Infof("Recovering ServingGroup at ordinal %d with revision %s using template from ControllerRevision (partition=%d)", ordinal, revisionToUse, partition)
				}
			} else if revisionToUse == newRevision {
				// First startup: persist the current template before creating even a
				// protected ordinal so later recovery never depends on mutable spec.
				if _, err := utils.CreateControllerRevision(ctx, c.kubeClientSet, ms, revisionToUse, ms.Spec.Template.Roles); err != nil {
					scaleUpErr = fmt.Errorf("failed to create ControllerRevision %s for protected ordinal %d: %w", revisionToUse, ordinal, err)
					return false
				}
				rolesToUse = ms.Spec.Template.Roles
			} else {
				scaleUpErr = fmt.Errorf("ControllerRevision %s for protected ordinal %d was not found", revisionToUse, ordinal)
				return false
			}

			if err := createServingGroup(ordinal, revisionToUse, rolesToUse); err != nil {
				scaleUpErr = err
				return false
			}
			return true
		}

		if !newRevisionCreated {
			// All ServingGroups created with newRevision share this snapshot, so create
			// it only once during this reconciliation.
			klog.V(4).Infof("scaleUpServingGroups: creating ControllerRevision for newRevision=%s, modelServing=%s", newRevision, utils.GetNamespaceName(ms))
			if _, err := utils.CreateControllerRevision(ctx, c.kubeClientSet, ms, newRevision, ms.Spec.Template.Roles); err != nil {
				scaleUpErr = fmt.Errorf("failed to create ControllerRevision for new revision %s: %w", newRevision, err)
				return false
			}
			newRevisionCreated = true
		}
		klog.V(4).Infof("scaleUpServingGroups: creating new ServingGroup at ordinal=%d with newRevision=%s for modelServing=%s", ordinal, newRevision, utils.GetNamespaceName(ms))
		if err := createServingGroup(ordinal, newRevision, ms.Spec.Template.Roles); err != nil {
			scaleUpErr = err
			return false
		}
		return true
	})
	if scaleUpErr != nil {
		return scaleUpErr
	}

	klog.V(4).Infof("scaleUpServingGroups: done for modelServing=%s", utils.GetNamespaceName(ms))
	return nil
}

// syncRoleReplicas coordinates role replicas within each active ServingGroup.
// A partition-protected group keeps its historical template while independently
// scalable Role replica counts follow the latest spec. An outdated group waiting
// for ServingGroupRollingUpdate keeps its complete historical Role configuration
// until the group itself is replaced.
//
// Main processing steps:
// 1. Iterate over all existing ServingGroups and skip those already marked as "Deleting".
// 2. Identify if the current ServingGroup must continue using its recorded revision.
// 3. Load the recorded Role configuration when required by partition or group rollout.
// 4. Update memory caches and use `manageRoleReplicas` to add/remove out-of-sync Pods and Services for each role.
func (c *ModelServingController) syncRoleReplicas(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	newRevision string,
	rolloutPolicy *roleRolloutPolicy,
) error {
	chain, err := c.buildPluginChain(ms)
	if err != nil {
		return fmt.Errorf("build plugin chain: %w", err)
	}
	servingGroupList, err := c.store.GetServingGroupByModelServing(utils.GetNamespaceName(ms))
	if err != nil && !errors.Is(err, datastore.ErrServingGroupNotFound) {
		return fmt.Errorf("cannot get ServingGroup of modelServing: %s from map: %v", ms.GetName(), err)
	}
	partition, _, _ := c.getPartition(modelServingPartition(ms), modelServingReplicas(ms))
	isServingGroupRollingUpdate := ms.Spec.RolloutStrategy == nil ||
		ms.Spec.RolloutStrategy.Type == workloadv1alpha1.ServingGroupRollingUpdate
	for _, servingGroup := range servingGroupList {
		if c.store.GetServingGroupStatus(utils.GetNamespaceName(ms), servingGroup.Name) == datastore.ServingGroupDeleting {
			// Deleting ServingGroup will be recreated after the deletion is complete, so there is no need to scale the roles
			continue
		}
		_, servingGroupOrdinal := utils.GetParentNameAndOrdinal(servingGroup.Name)
		if servingGroupOrdinal < 0 {
			return fmt.Errorf("cannot parse ordinal from ServingGroup %s", servingGroup.Name)
		}
		isPartitionProtected := partition > 0 && servingGroupOrdinal < partition
		useRecordedRoles := isPartitionProtected
		if isServingGroupRollingUpdate && !isPartitionProtected {
			// Unknown history is also kept conservative: it must not authorize
			// applying the target Role configuration inside an old group.
			useRecordedRoles = c.compareServingGroupTemplate(ctx, ms, servingGroup, newRevision) != templateEquivalent
		}

		rolesToManage := ms.Spec.Template.Roles
		revisionToUse := newRevision
		if useRecordedRoles {
			if servingGroup.Revision != "" {
				revisionToUse = c.revisionForServingGroup(ctx, ms, servingGroup)
			} else if ms.Status.CurrentRevision != "" {
				revisionToUse = ms.Status.CurrentRevision
			}

			if revisionToUse != "" {
				oldRoles, err := c.revisionHistory(ctx, ms).roles(ctx, revisionToUse)
				if err != nil {
					continue
				}
				rolesToManage = oldRoles
				if isPartitionProtected {
					rolesToManage = mergeLatestRoleReplicas(oldRoles, ms.Spec.Template.Roles)
				}
			}
		}

		for _, targetRole := range rolesToManage {
			if err := c.manageRoleReplicasPerGroup(
				ctx, ms, servingGroup.Name, targetRole, servingGroupOrdinal, revisionToUse, chain,
				rolloutPolicy.allowTargetStart(servingGroup.Name, targetRole.Name),
			); err != nil {
				if isRevisionResolutionError(err) {
					continue
				}
				return err
			}
		}
	}
	return nil
}

// mergeLatestRoleReplicas keeps rollout-controlled fields from the historical
// template while applying the latest independently scalable replica count.
// Inputs are never mutated.
func mergeLatestRoleReplicas(historicalRoles, latestRoles []workloadv1alpha1.Role) []workloadv1alpha1.Role {
	latestReplicas := make(map[string]*int32, len(latestRoles))
	for i := range latestRoles {
		if latestRoles[i].Replicas == nil {
			latestReplicas[latestRoles[i].Name] = nil
			continue
		}
		replicas := *latestRoles[i].Replicas
		latestReplicas[latestRoles[i].Name] = &replicas
	}

	merged := make([]workloadv1alpha1.Role, 0, len(historicalRoles))
	for i := range historicalRoles {
		role := historicalRoles[i].DeepCopy()
		if replicas, ok := latestReplicas[role.Name]; ok {
			if replicas == nil {
				role.Replicas = nil
			} else {
				value := *replicas
				role.Replicas = &value
			}
		}
		merged = append(merged, *role)
	}
	return merged
}

// scaleDownRoles handles Role scaling down with two-level priority-based selection:
// 1. Primary: Not-ready roles (Creating, NotFound) are deleted first
// 2. Secondary: Among roles with same status, lower deletion cost = delete first
// When partition is set, the first N replicas (where N = partition) are protected.
// Non-protected replicas (after the first N) are deleted first, then protected replicas if needed.
func (c *ModelServingController) scaleDownRoles(ctx context.Context, ms *workloadv1alpha1.ModelServing, groupName string, targetRole workloadv1alpha1.Role, roleList []datastore.Role, expectedCount int) error {
	// Calculate priority information for all Roles
	allScores := make([]RoleWithScore, 0, len(roleList))
	for _, role := range roleList {
		if role.Status == datastore.RoleDeleting {
			// Skip roles that are already being deleted
			continue
		}
		scoreInfo := c.calculateRoleScore(ms, groupName, targetRole.Name, role.Name)
		allScores = append(allScores, scoreInfo)
	}

	if len(allScores) <= expectedCount {
		klog.V(4).Infof("No need to scale down role %s in ServingGroup %s: current count=%d, expected count=%d", targetRole.Name, groupName, len(allScores), expectedCount)
		return nil
	}

	partition, _, partitionErr := c.getPartition(rolePartition(ms, targetRole), roleReplicas(targetRole))
	if partitionErr != nil {
		klog.Errorf("scaleDownRoles: failed to parse partition for role %s: %v", targetRole.Name, partitionErr)
		partition = 0
	}

	protectedRoleNames := sets.New[string]()
	for _, role := range roleList {
		_, ordinal := utils.GetParentNameAndOrdinal(role.Name)
		if ordinal >= 0 && ordinal < partition {
			protectedRoleNames.Insert(role.Name)
		}
	}

	var protectedScores []RoleWithScore
	var nonProtectedScores []RoleWithScore
	if partition > 0 {
		protectedScores = make([]RoleWithScore, 0, len(allScores))
		nonProtectedScores = make([]RoleWithScore, 0, len(allScores))
		for _, score := range allScores {
			if protectedRoleNames.Has(score.Name) {
				protectedScores = append(protectedScores, score)
			} else {
				nonProtectedScores = append(nonProtectedScores, score)
			}
		}
	} else {
		nonProtectedScores = allScores
	}

	// Sort both lists by priority tuple: (priority, deletionCost, index)
	// Lower priority value = higher deletion priority (delete first)
	// Lower deletion cost = higher deletion priority
	// Higher index = higher deletion priority (backward compatibility)
	sortRoles := func(a, b RoleWithScore) int {
		// Primary: Sort by priority (not-ready first)
		if a.Priority != b.Priority {
			return cmp.Compare(a.Priority, b.Priority) // Ascending: lower priority (not-ready) first
		}

		// Secondary: Among roles with same priority, lower deletion cost comes first
		if a.DeletionCost != b.DeletionCost {
			return cmp.Compare(a.DeletionCost, b.DeletionCost) // Ascending: lower cost first
		}

		// Tertiary: Higher index comes first (backward compatibility)
		return cmp.Compare(b.Index, a.Index) // Descending: higher indices first
	}

	slices.SortFunc(nonProtectedScores, sortRoles)

	totalToDelete := max(0, len(allScores)-expectedCount)

	// Role needs to scale down, and the ServingGroup status needs to be set to Scaling
	err := c.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), groupName, datastore.ServingGroupScaling)
	klog.V(4).Infof("Setting ServingGroup %s/%s status to Scaling for role %s scaling down", ms.Namespace+"/"+ms.Name, groupName, targetRole.Name)
	if err != nil {
		klog.Errorf("failed to set ServingGroup %s/%s status: %v", ms.Namespace+"/"+ms.Name, groupName, err)
		return err
	}

	// Delete non-protected roles first (replicas after the first partition replicas)
	numNonProtectedToDelete := min(totalToDelete, len(nonProtectedScores))
	for i := 0; i < numNonProtectedToDelete; i++ {
		target := nonProtectedScores[i]
		klog.V(2).Infof("Scaling down non-protected role %s (priority: %d, deletion cost: %d, index: %d)",
			target.Name, target.Priority, target.DeletionCost, target.Index)
		if err := c.DeleteRole(ctx, ms, groupName, targetRole.Name, target.Name); err != nil {
			return err
		}
	}

	// After all non-protected roles are deleted, proceed to delete protected roles if needed
	remainingToDelete := totalToDelete - numNonProtectedToDelete
	if remainingToDelete > 0 && partition > 0 {
		// Sort protected scores only when we need to delete them
		slices.SortFunc(protectedScores, sortRoles)
		numProtectedToDelete := min(remainingToDelete, len(protectedScores))
		for i := 0; i < numProtectedToDelete; i++ {
			target := protectedScores[i]
			klog.V(2).Infof("Scaling down protected role %s (priority: %d, deletion cost: %d, index: %d, partition=%d)",
				target.Name, target.Priority, target.DeletionCost, target.Index, partition)
			if err := c.DeleteRole(ctx, ms, groupName, targetRole.Name, target.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

// scaleUpRoles fills missing Role ordinals in [0, expectedCount).
// Missing ordinals below partition use CurrentRevision; the rest use newRevision.
// Dependency coordination may temporarily pause newRevision creation.
func (c *ModelServingController) scaleUpRoles(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	groupName string,
	targetRole workloadv1alpha1.Role,
	roleList []datastore.Role,
	expectedCount int,
	servingGroupOrdinal int,
	newRevision string,
	allowTargetStart bool,
) error {
	partition, partitionConfigured, partitionErr := c.getPartition(rolePartition(ms, targetRole), roleReplicas(targetRole))
	if partitionErr != nil {
		klog.Errorf("scaleUpRoles: failed to parse partition for role %s: %v", targetRole.Name, partitionErr)
	}

	existingOrdinals := make([]int, 0, len(roleList))
	for _, role := range roleList {
		_, ordinal := utils.GetParentNameAndOrdinal(role.Name)
		if ordinal < 0 {
			klog.Warningf("scaleUpRoles: cannot parse ordinal from Role %s", role.Name)
			continue
		}
		existingOrdinals = append(existingOrdinals, ordinal)
	}
	toCreate := max(0, expectedCount-len(roleList))

	// Role needs to scale up, and the ServingGroup status needs to be set to Scaling
	err := c.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), groupName, datastore.ServingGroupScaling)
	klog.V(4).Infof("Setting ServingGroup %s/%s status to Scaling for role %s scaling up", ms.Namespace+"/"+ms.Name, groupName, targetRole.Name)
	if err != nil {
		klog.Errorf("failed to set ServingGroup %s/%s status: %v", ms.Namespace+"/"+ms.Name, groupName, err)
		return err
	}

	// Helper function to create a Role
	createRole := func(ordinal int, revision string, roleToApply workloadv1alpha1.Role, roleTemplateHash string) error {
		// Create pods for role
		err := c.CreatePodsByRole(ctx, *roleToApply.DeepCopy(), ms, ordinal, servingGroupOrdinal, revision, roleTemplateHash)
		if err != nil {
			return fmt.Errorf("create role %s for ServingGroup %s: %w", utils.GenerateRoleID(targetRole.Name, ordinal), groupName, err)
		}
		// Insert new Role to global storage
		roleID := utils.GenerateRoleID(targetRole.Name, ordinal)
		c.store.AddRole(utils.GetNamespaceName(ms), groupName, targetRole.Name, roleID, revision, roleTemplateHash)
		// Emit event for new role entering Creating state
		message := fmt.Sprintf("Role %s/%s in ServingGroup %s is now Creating", targetRole.Name, roleID, groupName)
		c.emitRoleStatusEvent(ms, corev1.EventTypeNormal, "RoleCreating", message)
		return nil
	}

	roleTemplateHash := utils.CalRoleTemplateHash(targetRole)
	var scaleUpErr error
	forEachMissingOrdinal(expectedCount, existingOrdinals, toCreate, func(ordinal int) bool {
		if partitionConfigured && partition > 0 && ordinal < partition {
			// Use CurrentRevision for partition-protected ordinals
			revisionToUse := newRevision
			if ms.Status.CurrentRevision != "" {
				revisionToUse = ms.Status.CurrentRevision
			}
			klog.V(4).Infof("scaleUpRoles: ordinal %d missing (partition-protected), revisionToUse=%s, currentRevision=%s",
				ordinal, revisionToUse, ms.Status.CurrentRevision)

			// The reconcile entry has already persisted the initial snapshot.
			// Never select an arbitrary earlier revision from history: independent
			// Roles can legitimately keep several different revisions alive.
			roleToApply, err := c.revisionHistory(ctx, ms).role(ctx, revisionToUse, targetRole.Name)
			if err != nil {
				scaleUpErr = fmt.Errorf("resolve protected Role %s/%d at revision %s: %w", targetRole.Name, ordinal, revisionToUse, err)
				return false
			}
			hashToUse := utils.CalRoleTemplateHash(roleToApply)
			if err := createRole(ordinal, revisionToUse, roleToApply, hashToUse); err != nil {
				klog.Errorf("scaleUpRoles: failed to create role %s at ordinal %d in ServingGroup %s of ModelServing %s/%s: %v", targetRole.Name, ordinal, groupName, ms.Namespace, ms.Name, err)
				scaleUpErr = err
				return false
			}
			return true
		}
		if !allowTargetStart {
			// Dependency startup gating applies to every target-version creation,
			// including stable capacity added by an ordinary scale-up.
			return false
		}
		if err := createRole(ordinal, newRevision, targetRole, roleTemplateHash); err != nil {
			klog.Errorf("scaleUpRoles: failed to create role %s at ordinal %d in ServingGroup %s of ModelServing %s/%s: %v", targetRole.Name, ordinal, groupName, ms.Namespace, ms.Name, err)
			scaleUpErr = err
			return false
		}
		return true
	})
	return scaleUpErr
}

// manageRoleReplicasPerGroup manages the replicas of a specific role within an Serving group
// It handles both scale up and scale down operations for the role
func (c *ModelServingController) manageRoleReplicasPerGroup(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	groupName string,
	targetRole workloadv1alpha1.Role,
	servingGroupOrdinal int,
	newRevision string,
	chain *plugins.Chain,
	allowTargetStart bool,
) error {
	// TODO: add podGroup update after gang scheduler finished
	// Get all replicas of a role from storage, for example, prefill-0, prefill-1...
	roleList, err := c.store.GetRoleList(utils.GetNamespaceName(ms), groupName, targetRole.Name)
	if err != nil {
		return fmt.Errorf("manageRoleReplicasPerGroup: cannot get role %s in ServingGroup %s: %w", targetRole.Name, groupName, err)
	}

	expectedCount := roleReplicas(targetRole)
	if ms.Spec.RolloutStrategy != nil && ms.Spec.RolloutStrategy.Type == workloadv1alpha1.RoleRollingUpdate &&
		c.hasUpdateableOutdatedRole(ctx, ms, groupName, targetRole, roleList) {
		maxSurge, err := utils.GetMaxSurgeForRole(targetRole)
		if err != nil {
			klog.Errorf("manageRoleReplicasPerGroup: failed to calculate maxSurge for role %s in ServingGroup %s: %v", targetRole.Name, groupName, err)
		} else {
			expectedCount += maxSurge
		}
	}
	history := c.revisionHistory(ctx, ms)
	for _, roleObj := range roleList {
		if roleObj.Status == datastore.RoleDeleting {
			c.reconcileDeletingRole(ctx, ms, groupName, targetRole.Name, roleObj.Name)
			continue
		}
		roleIDValue := fmt.Sprintf("%s/%s/%s/%s", ms.Namespace, groupName, targetRole.Name, roleObj.Name)
		pods, err := c.getPodsByIndex(RoleIDKey, roleIDValue)
		if err != nil {
			klog.Warningf("manageRoleReplicasPerGroup: failed to list pods for role %s/%s in ServingGroup %s: %v", targetRole.Name, roleObj.Name, groupName, err)
			continue
		}
		template, instanceRevision, instanceHash, err := history.instanceTemplate(ctx, groupName, targetRole.Name, roleObj, pods)
		if err != nil {
			if len(roleList) > expectedCount {
				return errors.Join(err, c.scaleDownRoles(ctx, ms, groupName, targetRole, roleList, expectedCount))
			}
			return err
		}
		expectedPods := 1 + int(template.WorkerReplicas)
		ownedPods := make([]*corev1.Pod, 0, len(pods))
		for _, pod := range pods {
			if !utils.IsOwnedByModelServingWithUID(pod, ms.UID) {
				expectedName := false
				for podIndex := 0; podIndex < expectedPods; podIndex++ {
					if pod.Name == utils.GeneratePodName(groupName, roleObj.Name, podIndex) {
						expectedName = true
						break
					}
				}
				if expectedName && pod.Labels[workloadv1alpha1.ModelServingNameLabelKey] == ms.Name {
					if err := c.deleteConflictingPod(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
						klog.Errorf("manageRoleReplicasPerGroup: failed to delete orphan pod %s/%s: %v", pod.Namespace, pod.Name, err)
					}
				}
				klog.Warningf("manageRoleReplicasPerGroup: pod %s/%s is not owned by current ModelServing %s/%s (expected UID=%s), re-enqueuing",
					pod.Namespace, pod.Name, ms.Namespace, ms.Name, ms.UID)
				c.enqueueModelServingAfter(ms, 1*time.Second)
				continue
			}
			ownedPods = append(ownedPods, pod)
		}
		podNames := make(map[string]bool, len(ownedPods))
		for _, pod := range ownedPods {
			podNames[pod.Name] = true
		}
		missing := false
		for i := 0; i < expectedPods; i++ {
			if !podNames[utils.GeneratePodName(groupName, roleObj.Name, i)] {
				missing = true
				break
			}
		}
		if missing {
			if ms.Spec.RecoveryPolicy == workloadv1alpha1.RoleRecreate && roleObj.Status == datastore.RoleRunning {
				klog.V(2).Infof("manageRoleReplicasPerGroup: running role %s/%s in ServingGroup %s is missing pods (%d/%d), deleting role for RoleRecreate recovery", targetRole.Name, roleObj.Name, groupName, len(ownedPods), expectedPods)
				if groupStatus := c.store.GetServingGroupStatus(utils.GetNamespaceName(ms), groupName); groupStatus == datastore.ServingGroupRunning {
					if err := c.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), groupName, datastore.ServingGroupCreating); err != nil {
						klog.Warningf("manageRoleReplicasPerGroup: failed to set ServingGroup %s/%s to Creating before RoleRecreate recovery: %v", ms.Namespace, groupName, err)
					}
				}
				if err := c.DeleteRole(ctx, ms, groupName, targetRole.Name, roleObj.Name); err != nil {
					return fmt.Errorf("delete incomplete Role %s/%s: %w", groupName, roleObj.Name, err)
				}
				continue
			}
			klog.V(2).Infof("manageRoleReplicasPerGroup: role %s/%s in ServingGroup %s is missing pods (%d/%d), recreating", targetRole.Name, roleObj.Name, groupName, len(ownedPods), expectedPods)
			_, roleIndex := utils.GetParentNameAndOrdinal(roleObj.Name)
			if err := c.CreatePodsByRole(ctx, *template.DeepCopy(), ms, roleIndex, servingGroupOrdinal, instanceRevision, instanceHash); err != nil {
				return fmt.Errorf("restore Role %s/%s at revision %s: %w", groupName, roleObj.Name, instanceRevision, err)
			}
		}
		if !missing && c.observation == nil && roleObj.Status == datastore.RoleCreating {
			for _, pod := range ownedPods {
				if pod.DeletionTimestamp == nil && utils.IsPodRunningAndReady(pod) && !c.shouldSkipHandling(ms, groupName, pod) {
					if err := c.handleReadyPod(ms, groupName, pod); err != nil {
						return err
					}
					break
				}
			}
		}

		if chain != nil {
			_, roleIndex := utils.GetParentNameAndOrdinal(roleObj.Name)
			roleForSync := template.DeepCopy()
			if err := chain.OnRoleSync(ctx, &plugins.HookRequest{
				ModelServing:    ms,
				ServingGroup:    groupName,
				RoleName:        targetRole.Name,
				RoleID:          roleObj.Name,
				RoleIndex:       roleIndex,
				Role:            roleForSync,
				KubeClient:      c.kubeClientSet,
				ServiceLister:   c.servicesLister,
				PodLister:       c.podsLister,
				ConfigMapLister: c.configMapsLister,
			}); err != nil {
				return fmt.Errorf("sync plugins for Role %s/%s in ServingGroup %s: %w", targetRole.Name, roleObj.Name, groupName, err)
			}
		}
	}

	// Determine whether it is a scale-up or scale-down scenario
	if len(roleList) < expectedCount {
		klog.V(2).Infof("manageRoleReplicasPerGroup: scaling UP role %s in ServingGroup %s: current=%d, expected=%d", targetRole.Name, groupName, len(roleList), expectedCount)
		if ms.Spec.RolloutStrategy == nil || ms.Spec.RolloutStrategy.Type == workloadv1alpha1.ServingGroupRollingUpdate {
			groupRevision, _ := c.store.GetServingGroupRevision(utils.GetNamespaceName(ms), groupName)
			historical, rev, _, err := history.instanceTemplate(ctx, groupName, targetRole.Name, datastore.Role{Revision: groupRevision}, nil)
			if err != nil {
				return err
			}
			historical.Replicas = targetRole.Replicas
			targetRole, newRevision = historical, rev
		}
		if err := c.scaleUpRoles(ctx, ms, groupName, targetRole, roleList, expectedCount, servingGroupOrdinal, newRevision, allowTargetStart); err != nil {
			return err
		}
	} else if len(roleList) > expectedCount {
		klog.V(2).Infof("manageRoleReplicasPerGroup: scaling DOWN role %s in ServingGroup %s: current=%d, expected=%d", targetRole.Name, groupName, len(roleList), expectedCount)
		if err := c.scaleDownRoles(ctx, ms, groupName, targetRole, roleList, expectedCount); err != nil {
			return err
		}
	}
	return nil
}

// hasUpdateableOutdatedRole reports whether a Role has an outdated replica
// outside its partition-protected prefix. maxSurge changes only the temporary
// expected replica count; individual replicas are not classified as surge by
// ordinal because binpack scale-down may leave sparse or high ordinals.
func (c *ModelServingController) hasUpdateableOutdatedRole(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	groupName string,
	targetRole workloadv1alpha1.Role,
	roleList []datastore.Role,
) bool {
	partition, _, err := c.getPartition(rolePartition(ms, targetRole), roleReplicas(targetRole))
	if err != nil {
		klog.Errorf("hasUpdateableOutdatedRole: failed to calculate partition for role %s in ServingGroup %s: %v", targetRole.Name, groupName, err)
		return false
	}
	for _, role := range roleList {
		_, ordinal := utils.GetParentNameAndOrdinal(role.Name)
		if ordinal < 0 || ordinal < partition || role.Status == datastore.RoleDeleting {
			continue
		}
		if c.compareRoleTemplate(ctx, ms, datastore.ServingGroup{
			Name:     groupName,
			Revision: role.Revision,
		}, targetRole.Name, role) == templateDifferent {
			return true
		}
	}
	return false
}

// roleTemplateForReplica resolves the role template, revision, and hash to use when recreating pods for a replica.
// When keepCurrentRevision is true, the replica keeps its recorded revision (or
// CurrentRevision) and loads that Role template from ControllerRevision.
func (c *ModelServingController) roleTemplateForReplica(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	targetRole workloadv1alpha1.Role,
	roleObj datastore.Role,
	newRevision string,
	keepCurrentRevision bool,
) (workloadv1alpha1.Role, string, string, error) {
	roleToApply := targetRole
	revisionToUse := newRevision
	hashToUse := ""
	if !keepCurrentRevision {
		return roleToApply, revisionToUse, utils.CalRoleTemplateHash(roleToApply), nil
	}

	revisionToUse = roleObj.Revision
	if revisionToUse == "" {
		revisionToUse = ms.Status.CurrentRevision
	}
	hashToUse = roleObj.RoleTemplateHash
	var err error
	roleToApply, err = c.revisionHistory(ctx, ms).role(ctx, revisionToUse, targetRole.Name)
	if err != nil {
		return workloadv1alpha1.Role{}, "", "", err
	}
	if hashToUse == "" {
		hashToUse = utils.CalRoleTemplateHash(roleToApply)
	}
	return roleToApply, revisionToUse, hashToUse, nil
}

// emitRoleStatusEvent emits a Kubernetes Event for a role-related status change.
// It is intentionally lightweight and no-op when recorder is not initialized.
func (c *ModelServingController) emitRoleStatusEvent(
	ms *workloadv1alpha1.ModelServing,
	eventType, reason, message string,
) {
	if c == nil || c.recorder == nil || ms == nil {
		return
	}
	c.recorder.Event(ms, eventType, reason, message)
}

func (c *ModelServingController) getModelServingAndResourceDetails(resource metav1.Object) (*workloadv1alpha1.ModelServing, string, string, string) {
	ms, servingGroupName, err := c.getModelServingByChildResource(resource)
	if apierrors.IsNotFound(err) {
		modelServingName, groupName, _ := utils.GetModelServingAndGroupByLabel(resource.GetLabels())
		ms, err = c.modelServingClient.WorkloadV1alpha1().ModelServings(resource.GetNamespace()).Get(c.operationContext(), modelServingName, metav1.GetOptions{})
		servingGroupName = groupName
	}
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.V(4).Infof("modelServing of svc %s/%s has been deleted", resource.GetNamespace(), resource.GetName())
		} else {
			klog.Errorf("failed to get modelServing of pod %s/%s: %v", resource.GetNamespace(), resource.GetName(), err)
		}
		return nil, "", "", ""
	}

	roleName, roleID := utils.GetRoleName(resource), utils.GetRoleID(resource)

	return ms, servingGroupName, roleName, roleID
}

func (c *ModelServingController) DeleteRole(ctx context.Context, ms *workloadv1alpha1.ModelServing, groupName, roleName, roleID string) (deleteErr error) {
	if c.servingState != nil {
		c.servingState.roleDeletes[roleCleanupKey{groupName, roleName, roleID}] = struct{}{}
	}
	selector := labels.SelectorFromSet(map[string]string{
		workloadv1alpha1.GroupNameLabelKey: groupName,
		workloadv1alpha1.RoleLabelKey:      roleName,
		workloadv1alpha1.RoleIDKey:         roleID,
	})

	// If the role is already in the deletion process, no further processing will be done.
	roleStatus := c.store.GetRoleStatus(utils.GetNamespaceName(ms), groupName, roleName, roleID)
	if roleStatus == datastore.RoleDeleting {
		c.enqueueModelServingAfter(ms, roleDeletionRecheckDelay)
		return nil
	}
	err := c.store.UpdateRoleStatus(utils.GetNamespaceName(ms), groupName, roleName, roleID, datastore.RoleDeleting)
	klog.V(4).Infof("Setting role %s/%s status to Deleting", ms.GetName(), roleID)
	if err != nil {
		klog.Errorf("failed to set role %s/%s status: %v", groupName, roleID, err)
		return err
	}

	// Emit event for role entering Deleting state.
	message := fmt.Sprintf("Role %s/%s in ServingGroup %s is now Deleting", roleName, roleID, groupName)
	c.emitRoleStatusEvent(ms, corev1.EventTypeNormal, "RoleDeleting", message)
	defer func() {
		if deleteErr == nil {
			return
		}
		rollbackErr := c.store.UpdateRoleStatus(utils.GetNamespaceName(ms), groupName, roleName, roleID, roleStatus)
		if rollbackErr != nil {
			klog.ErrorS(rollbackErr, "Failed to rollback role status", "role", roleID, "group", groupName)
		}
		c.enqueueModelServing(ms)
	}()

	deleteErr = c.kubeClientSet.CoreV1().Pods(ms.Namespace).DeleteCollection(
		ctx,
		metav1.DeleteOptions{},
		metav1.ListOptions{
			LabelSelector: selector.String(),
		},
	)
	if deleteErr != nil {
		klog.Errorf("failed to delete pods of role %s/%s: %v", groupName, roleID, deleteErr)
		return deleteErr
	}
	if deleteErr = c.runRoleDeletePlugins(ctx, ms, groupName, roleName, roleID); deleteErr != nil {
		return deleteErr
	}

	// Once the role's pods are fully deleted, remove the role from the store.
	// Note: This measure is taken to prevent the Role’s resources from being deleted before the current function execution has completed,
	// which would prevent them from being queued for re-coordination.
	if c.isRoleDeleted(ms, groupName, roleName, roleID) {
		klog.V(2).Infof("Role %s of ServingGroup %s has been deleted", roleID, groupName)
		c.store.DeleteRole(utils.GetNamespaceName(ms), groupName, roleName, roleID)
		c.clearRoleDeletionProgress(ms, groupName, roleName, roleID)
		// Re-enqueue the ModelServing for reconciliation after the role has been deleted
		// so the controller can recreate any missing resources if needed.
		c.enqueueModelServing(ms)
		return nil
	}
	c.enqueueModelServingAfter(ms, roleDeletionRecheckDelay)
	return nil
}

// manageRollingUpdate updates outdated resources at the granularity selected by
// rolloutStrategy.type. ServingGroupRollingUpdate uses only the ModelServing-level
// maxUnavailable and partition. RoleRollingUpdate uses only each Role's
// maxUnavailable, partition and ServingGroup's partition.
//
// Main processing steps:
//  1. Identify the boundary for the currently active rollout partition.
//  2. Filter outdated groups (mismatched revision) that are allowed to be updated.
//  3. For ServingGroupRollingUpdate, enforce the ServingGroup-level maxUnavailable budget.
//  4. For RoleRollingUpdate, update outdated roles using each Role's maxUnavailable budget.
func (c *ModelServingController) manageRollingUpdate(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	revision string,
	rolloutPolicy *roleRolloutPolicy,
) error {
	servingGroupList, err := c.store.GetServingGroupByModelServing(utils.GetNamespaceName(ms))
	if err != nil {
		if errors.Is(err, datastore.ErrServingGroupNotFound) {
			return nil
		}
		return fmt.Errorf("cannot get ServingGroupList from store, err:%v", err)
	}

	partition, _, _ := c.getPartition(modelServingPartition(ms), modelServingReplicas(ms))
	// Separate outdated groups into two categories: not-running and running
	// We prioritize updating not-running outdated groups first
	var notRunningOutdatedGroups []datastore.ServingGroup
	var runningOutdatedGroups []datastore.ServingGroup
	groupsAfterPartition := make([]datastore.ServingGroup, 0, len(servingGroupList))
	for _, servingGroup := range servingGroupList {
		_, ordinal := utils.GetParentNameAndOrdinal(servingGroup.Name)
		if ordinal < 0 {
			return fmt.Errorf("cannot parse ordinal from ServingGroup %s", servingGroup.Name)
		}
		if ordinal >= partition {
			groupsAfterPartition = append(groupsAfterPartition, servingGroup)
		}
	}
	if len(groupsAfterPartition) == 0 {
		return nil
	}

	newServingGroupUnavailableCount := 0
	for _, sg := range groupsAfterPartition {
		comparison := c.compareServingGroupTemplate(ctx, ms, sg, revision)
		if sg.Status != datastore.ServingGroupRunning {
			if comparison != templateDifferent {
				// Unknown history cannot authorize deletion or provide spare
				// availability for deleting another Running group.
				newServingGroupUnavailableCount++
			} else {
				notRunningOutdatedGroups = append(notRunningOutdatedGroups, sg)
			}
		} else if comparison == templateDifferent {
			runningOutdatedGroups = append(runningOutdatedGroups, sg)
		}
	}

	if ms.Spec.RolloutStrategy != nil && ms.Spec.RolloutStrategy.Type == workloadv1alpha1.RoleRollingUpdate {
		roleRollingGroups := make([]datastore.ServingGroup, 0, len(groupsAfterPartition))
		for _, servingGroup := range groupsAfterPartition {
			if servingGroup.Status != datastore.ServingGroupDeleting {
				roleRollingGroups = append(roleRollingGroups, servingGroup)
			}
		}
		updateCount, err := c.deleteOutdatedRoles(ctx, ms, roleRollingGroups, revision, rolloutPolicy)
		if err != nil {
			return err
		}
		if updateCount > 0 {
			klog.V(4).Infof("Started Role updates in %d ServingGroups for ModelServing %s", updateCount, ms.Name)
		}
		return nil
	}

	maxScaleDown := 0
	if ms.Spec.RolloutStrategy == nil || ms.Spec.RolloutStrategy.Type == workloadv1alpha1.ServingGroupRollingUpdate {
		maxUnavailable, err := utils.GetMaxUnavailable(ms)
		if err != nil {
			return fmt.Errorf("failed to calculate maxUnavailable: %v", err)
		}

		// Calculate the minimum number of available ServingGroups required
		// Refer to https://github.com/kubernetes/kubernetes/blob/master/pkg/controller/deployment/rolling.go
		// Check if we can scale down. We can scale down in the following 2 cases:
		// * Some old servingGroups are unhealthy, we could safely scale down those unhealthy servingGroups
		//   since that won't further increase unavailability.
		// * New servingGroup has scaled up and its replicas become ready, then we can scale down old servingGroups
		//   in a further step.
		minAvailable := modelServingReplicas(ms) - maxUnavailable
		// All Running ServingGroups, including temporary maxSurge capacity,
		// contribute to the availability budget. Unhealthy outdated groups can be
		// removed without reducing availability; an unavailable new-revision group
		// contributes nothing and may naturally reduce maxScaleDown to zero.
		maxScaleDown = len(servingGroupList) - minAvailable - newServingGroupUnavailableCount

		// TODO(hzxuzhonghu): reuse calMaxScaleDown
		if maxScaleDown <= 0 {
			klog.V(4).Infof("No ServingGroups can be updated for ModelServing %s/%s: maxScaleDown=%d",
				ms.Namespace, ms.Name, maxScaleDown)
			return nil
		}
	}

	allOutdatedGroups := append(runningOutdatedGroups, notRunningOutdatedGroups...)
	updateCount, err := c.deleteOutdatedServingGroups(ctx, ms, maxScaleDown, allOutdatedGroups)
	if err != nil {
		return err
	}

	if updateCount > 0 {
		strategy := workloadv1alpha1.ServingGroupRollingUpdate
		if ms.Spec.RolloutStrategy != nil {
			strategy = ms.Spec.RolloutStrategy.Type
		}
		klog.V(4).Infof("Started updates in %d ServingGroups for ModelServing %s (strategy=%s)", updateCount, ms.Name, strategy)
	}
	return nil
}

// deleteOutdatedServingGroups deletes outdated ServingGroups
// for `ServingGroupRollingUpdate`.
func (c *ModelServingController) deleteOutdatedServingGroups(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	maxScaleDown int,
	groups []datastore.ServingGroup,
) (int, error) {
	updateCount := 0

	// Iterate from end to start to delete largest ordinals first.
	for i := len(groups) - 1; i >= 0 && updateCount < maxScaleDown; i-- {
		sg := groups[i]
		klog.V(2).Infof("ServingGroup %s will be terminated for update (status=%s)", sg.Name, sg.Status)
		if err := c.deleteServingGroup(ctx, ms, sg.Name); err != nil {
			return updateCount, err
		}
		updateCount++
	}

	return updateCount, nil
}

// deleteOutdatedRoles deletes outdated Roles for `RoleRollingUpdate`.
func (c *ModelServingController) deleteOutdatedRoles(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	groups []datastore.ServingGroup,
	revision string,
	rolloutPolicy *roleRolloutPolicy,
) (int, error) {
	updateCount := 0

	// Iterate from end to start to delete largest ordinals first.
	for i := len(groups) - 1; i >= 0; i-- {
		sg := groups[i]
		rolesToDelete, hasOutdatedRoles, err := c.rolesToDeleteForRoleRollingUpdate(ctx, ms, sg, rolloutPolicy.group(sg.Name))
		if err != nil {
			return updateCount, err
		}
		if !hasOutdatedRoles {
			continue
		}
		if len(rolesToDelete) == 0 {
			continue
		}
		for _, role := range rolesToDelete {
			klog.V(2).Infof("Role %s/%s in ServingGroup %s will be terminated for update", role.roleName, role.roleID, sg.Name)
			if err := c.DeleteRole(ctx, ms, sg.Name, role.roleName, role.roleID); err != nil {
				return updateCount, err
			}
		}
		updateCount++
	}

	return updateCount, nil
}

type roleToDelete struct {
	roleName string
	roleID   string
}

func (c *ModelServingController) rolesToDeleteForRoleRollingUpdate(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	sg datastore.ServingGroup,
	groupPolicy *roleRolloutGroupPolicy,
) ([]roleToDelete, bool, error) {
	roleSpecByName := make(map[string]workloadv1alpha1.Role, len(ms.Spec.Template.Roles))
	for _, role := range ms.Spec.Template.Roles {
		roleSpecByName[role.Name] = role
	}

	allRoles, err := c.store.GetRolesByGroup(utils.GetNamespaceName(ms), sg.Name)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get roles for ServingGroup %s: %v", sg.Name, err)
	}

	var rolesToDelete []roleToDelete
	hasOutdatedRoles := groupPolicy != nil && groupPolicy.inProgress
	for _, roleSpec := range ms.Spec.Template.Roles {
		roleList, err := c.store.GetRoleList(utils.GetNamespaceName(ms), sg.Name, roleSpec.Name)
		if err != nil {
			return nil, false, fmt.Errorf("failed to get roles for ServingGroup %s, role %s: %v", sg.Name, roleSpec.Name, err)
		}

		outdatedRoles, newUnavailable := c.outdatedRoles(ctx, ms, sg, roleSpec, roleList)
		partition, partitionConfigured, partitionErr := c.getPartition(rolePartition(ms, roleSpec), roleReplicas(roleSpec))
		if partitionErr != nil {
			return nil, false, fmt.Errorf("failed to parse partition for role %s: %v", roleSpec.Name, partitionErr)
		}
		protected := sets.New[string]()
		if partitionConfigured && partition > 0 {
			for _, role := range roleList {
				_, ordinal := utils.GetParentNameAndOrdinal(role.Name)
				if ordinal >= 0 && ordinal < partition {
					protected.Insert(role.Name)
				}
			}
		}
		if len(protected) > 0 && len(outdatedRoles) > 0 {
			filtered := outdatedRoles[:0]
			for _, r := range outdatedRoles {
				if protected.Has(r.Name) {
					continue
				}
				filtered = append(filtered, r)
			}
			outdatedRoles = filtered
		}

		// Keep the existing ServingGroup revision semantics: protected outdated
		// replicas still mean this ServingGroup has not fully reached the revision,
		// even though they are not rolling candidates.
		if len(outdatedRoles) == 0 {
			if len(protected) > 0 {
				for _, role := range roleList {
					if !protected.Has(role.Name) {
						continue
					}
					if role.Status == datastore.RoleDeleting {
						continue
					}
					comparison := c.compareRoleTemplate(ctx, ms, sg, roleSpec.Name, role)
					if comparison == templateDifferent {
						hasOutdatedRoles = true
						break
					}
				}
			}
		}

		if len(outdatedRoles) > 0 {
			hasOutdatedRoles = true
		}

		if len(outdatedRoles) == 0 {
			continue
		}
		maxScaleDown, err := calMaxScaleDown(roleSpec, outdatedRoles, len(roleList), newUnavailable)
		if err != nil {
			klog.Errorf("failed to calculate maxScaleDown for role %s in ServingGroup %s: %v", roleSpec.Name, sg.Name, err)
		}
		outdatedRoles, maxScaleDown = groupPolicy.constrainRoleDeletion(roleSpec.Name, outdatedRoles, maxScaleDown)
		if len(outdatedRoles) == 0 {
			continue
		}
		localCandidates, err := selectOutdatedRolesToDelete(roleSpec.Name, outdatedRoles, maxScaleDown)
		if err != nil {
			return nil, false, err
		}
		rolesToDelete = append(rolesToDelete, localCandidates...)
	}

	// handle the case when there are roles whose roleSpec has been deleted in the new revision. Those roles should be deleted directly since they are all outdated.
	for roleName, roles := range allRoles {
		if _, ok := roleSpecByName[roleName]; ok {
			continue
		}
		for roleID, role := range roles {
			if role.Status == datastore.RoleDeleting {
				continue
			}
			hasOutdatedRoles = true
			rolesToDelete = append(rolesToDelete, roleToDelete{roleName: roleName, roleID: roleID})
		}
	}

	return rolesToDelete, hasOutdatedRoles, nil
}

func (c *ModelServingController) outdatedRoles(ctx context.Context, ms *workloadv1alpha1.ModelServing, sg datastore.ServingGroup, roleSpec workloadv1alpha1.Role, roleList []datastore.Role) ([]datastore.Role, int) {
	outdatedRoles := make([]datastore.Role, 0, len(roleList))
	// record the number of roles that is in rollingupdate but not ready yet.
	newUnavailable := 0
	for _, role := range roleList {
		if role.Status == datastore.RoleDeleting {
			newUnavailable++
			continue
		}
		comparison := c.compareRoleTemplate(ctx, ms, sg, roleSpec.Name, role)
		if comparison == templateUnknown {
			if role.Status != datastore.RoleRunning {
				newUnavailable++
			}
			continue
		}
		if comparison == templateDifferent {
			outdatedRoles = append(outdatedRoles, role)
		} else if role.Status != datastore.RoleRunning {
			newUnavailable++
		}
	}

	slices.SortFunc(outdatedRoles, func(a, b datastore.Role) int {
		if a.Status != b.Status {
			if a.Status != datastore.RoleRunning {
				return -1
			}
			return 1
		}
		_, aOrdinal := utils.GetParentNameAndOrdinal(a.Name)
		_, bOrdinal := utils.GetParentNameAndOrdinal(b.Name)
		return cmp.Compare(bOrdinal, aOrdinal)
	})
	return outdatedRoles, newUnavailable
}

func selectOutdatedRolesToDelete(roleName string, outdatedRoles []datastore.Role, maxScaleDown int) ([]roleToDelete, error) {
	rolesToDelete := make([]roleToDelete, 0, len(outdatedRoles))
	for _, role := range outdatedRoles {
		if maxScaleDown == 0 {
			break
		}
		maxScaleDown--
		rolesToDelete = append(rolesToDelete, roleToDelete{roleName: roleName, roleID: role.Name})
	}
	return rolesToDelete, nil
}

func (c *ModelServingController) handleRunningPod(ms *workloadv1alpha1.ModelServing, servingGroupName string, pod *corev1.Pod) error {
	chain, err := c.buildPluginChain(ms)
	if err != nil {
		return fmt.Errorf("build plugin chain: %w", err)
	}
	if chain == nil {
		return nil
	}
	if c.observation != nil {
		req, err := c.observedPodHookRequest(c.operationContext(), ms, pod)
		if err != nil {
			return err
		}
		return chain.OnPodRunning(c.operationContext(), req)
	}
	return chain.OnPodRunning(c.operationContext(), &plugins.HookRequest{
		ModelServing:    ms,
		ServingGroup:    servingGroupName,
		RoleName:        utils.GetRoleName(pod),
		RoleID:          utils.GetRoleID(pod),
		IsEntry:         pod.Labels[workloadv1alpha1.EntryLabelKey] == utils.Entry,
		Pod:             pod,
		PodLister:       c.podsLister,
		ConfigMapLister: c.configMapsLister,
		KubeClient:      c.kubeClientSet,
		ServiceLister:   c.servicesLister,
	})
}

func (c *ModelServingController) handleReadyPod(ms *workloadv1alpha1.ModelServing, servingGroupName string, newPod *corev1.Pod) error {
	chain, err := c.buildPluginChain(ms)
	if err != nil {
		return fmt.Errorf("build plugin chain: %w", err)
	}
	if chain != nil {
		if err := chain.OnPodReady(c.operationContext(), &plugins.HookRequest{
			ModelServing:    ms,
			ServingGroup:    servingGroupName,
			RoleName:        utils.GetRoleName(newPod),
			RoleID:          utils.GetRoleID(newPod),
			IsEntry:         newPod.Labels[workloadv1alpha1.EntryLabelKey] == utils.Entry,
			Pod:             newPod,
			PodLister:       c.podsLister,
			ConfigMapLister: c.configMapsLister,
			KubeClient:      c.kubeClientSet,
			ServiceLister:   c.servicesLister,
		}); err != nil {
			return err
		}
	}

	// Add the running pod to the global storage and try to update the ServingGroup status
	roleName := utils.GetRoleName(newPod)
	roleID := utils.GetRoleID(newPod)
	roleTemplateHash := c.resolveRoleTemplateHash(ms, roleName, newPod)
	c.store.AddRunningPodToServingGroup(types.NamespacedName{
		Namespace: ms.Namespace,
		Name:      ms.Name,
	}, servingGroupName, newPod.Name, utils.ObjectRevision(newPod), roleTemplateHash, roleName, roleID)

	// Check and update role status to Running when all pods in the role are ready
	roleBecameRunning := false
	roleReady, err := c.checkRoleReady(ms, servingGroupName, roleName, roleID)
	if err != nil {
		klog.Warningf("failed to check role %s/%s readiness, skipping role status update: %v", roleName, roleID, err)
	} else if roleReady {
		currentRoleStatus := c.store.GetRoleStatus(utils.GetNamespaceName(ms), servingGroupName, roleName, roleID)
		if currentRoleStatus != datastore.RoleRunning && currentRoleStatus != datastore.RoleDeleting {
			if err := c.store.UpdateRoleStatus(utils.GetNamespaceName(ms), servingGroupName, roleName, roleID, datastore.RoleRunning); err != nil {
				klog.Warningf("failed to update role %s/%s status to Running: %v", roleName, roleID, err)
			} else {
				roleBecameRunning = true
				klog.V(2).Infof("Update role %s/%s status to Running", roleName, roleID)
				// Emit event for role transitioning to Running
				message := fmt.Sprintf("Role %s/%s in ServingGroup %s is now Running", roleName, roleID, servingGroupName)
				c.emitRoleStatusEvent(ms, corev1.EventTypeNormal, "RoleRunning", message)
			}
		}
	}

	ready, err := c.checkServingGroupReady(ms, servingGroupName)
	if err != nil {
		return fmt.Errorf("failed to check ServingGroup status, err: %v", err)
	}
	if ready {
		// All pods in the ServingGroup are running, so the ServingGroup status also needs to be set to running
		err = c.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), servingGroupName, datastore.ServingGroupRunning)
		klog.V(4).Infof("ServingGroup: %s/%s status updated to Running", ms.GetName(), servingGroupName)
		if err != nil {
			return fmt.Errorf("failed to set ServingGroup %s status: %v", servingGroupName, err)
		}
		klog.V(2).Infof("Update ServingGroup %s status to Running", servingGroupName)
		c.enqueueModelServing(ms)
	} else {
		klog.V(4).Infof("ServingGroup %s still creating", servingGroupName)
		// A Role becoming Ready may release maxSurge, proportional-progress, or
		// dependency budget before the whole ServingGroup is Ready. Reconcile
		// immediately so RoleRollingUpdate can spend the newly available budget.
		if roleBecameRunning {
			c.enqueueModelServing(ms)
		}
	}
	return nil
}

func (c *ModelServingController) handleErrorPod(ms *workloadv1alpha1.ModelServing, servingGroupName string, errPod *corev1.Pod) error {
	// None: leave a restarted, still-alive pod to the kubelet (do not delete it),
	// but mark it unavailable. A terminal PodFailed pod falls through to deletion.
	if ms.Spec.RecoveryPolicy == workloadv1alpha1.NoneRestartPolicy && utils.ContainerRestarted(errPod) && !utils.IsPodFailed(errPod) {
		if err := c.markPodUnavailable(ms, servingGroupName, errPod); err != nil {
			klog.Warningf("mark pod %s unavailable: %v", errPod.Name, err)
		}
		c.enqueueModelServing(ms)
		return nil
	}
	// pod is already in the grace period and does not need to be processed for the time being.
	key := getPodGracePeriodKey(errPod)
	now := time.Now()
	_, loaded := c.graceMap.LoadOrStore(key, now)
	if loaded {
		klog.V(4).Infof("Pod %v already in grace period", key)
		return nil
	}
	if err := c.markPodUnavailable(ms, servingGroupName, errPod); err != nil {
		return err
	}
	// Wait for the grace period before processing
	go c.handlePodAfterGraceTime(ms, errPod)
	// ServingGroup status may change, needs reconcile
	c.enqueueModelServing(ms)
	return nil
}

// markPodUnavailable removes the pod from the running set and transitions its
// role/serving group out of Running so AvailableReplicas stops counting it. It
// does not delete the pod.
func (c *ModelServingController) markPodUnavailable(ms *workloadv1alpha1.ModelServing, servingGroupName string, errPod *corev1.Pod) error {
	c.store.DeleteRunningPodFromServingGroup(types.NamespacedName{
		Namespace: ms.Namespace,
		Name:      ms.Name,
	}, servingGroupName, errPod.Name)

	roleName := utils.GetRoleName(errPod)
	roleID := utils.GetRoleID(errPod)
	// Update role status back to Creating when pod fails
	if roleStatus := c.store.GetRoleStatus(utils.GetNamespaceName(ms), servingGroupName, roleName, roleID); roleStatus == datastore.RoleRunning {
		if err := c.store.UpdateRoleStatus(utils.GetNamespaceName(ms), servingGroupName, roleName, roleID, datastore.RoleCreating); err != nil {
			klog.Warningf("failed to update role %s/%s status to Creating: %v", roleName, roleID, err)
		} else {
			klog.V(2).Infof("update role %s/%s to Creating when pod fails", roleName, roleID)
			// Emit event for role re-entering Creating state due to failure
			message := fmt.Sprintf("Role %s/%s in ServingGroup %s is now Creating", roleName, roleID, servingGroupName)
			c.emitRoleStatusEvent(ms, corev1.EventTypeNormal, "RoleCreating", message)
		}
	}

	// If the ServingGroup status is already running, the status needs to be updated
	if groupStatus := c.store.GetServingGroupStatus(utils.GetNamespaceName(ms), servingGroupName); groupStatus == datastore.ServingGroupRunning {
		if err := c.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), servingGroupName, datastore.ServingGroupCreating); err != nil {
			return fmt.Errorf("update ServingGroup status failed, err:%v", err)
		}
		klog.V(2).Infof("update ServingGroup %s to processing when pod fails", servingGroupName)
	}
	return nil
}

func (c *ModelServingController) handlePodAfterGraceTime(ms *workloadv1alpha1.ModelServing, errPod *corev1.Pod) {
	if ms.Spec.Template.RestartGracePeriodSeconds != nil && *ms.Spec.Template.RestartGracePeriodSeconds > 0 {
		// Wait for the grace period before making a decision
		time.Sleep(time.Duration(*ms.Spec.Template.RestartGracePeriodSeconds) * time.Second)
		klog.V(4).Infof("%s after grace time", errPod.Name)
		defer c.graceMap.Delete(getPodGracePeriodKey(errPod))

		newPod, err := c.podsLister.Pods(ms.Namespace).Get(errPod.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				klog.V(4).Infof("pod %s has been deleted after grace time", errPod.Name)
			} else {
				klog.Errorf("cannot get pod %s after grace time, err: %v", errPod.Name, err)
			}
			return
		}
		if newPod.UID != errPod.UID {
			klog.V(4).Infof("pod %s has been replaced after grace time", errPod.Name)
			return
		}

		if !utils.IsPodRunningAndReady(newPod) {
			// pod has not recovered after the grace period, needs to be rebuilt
			// After this pod has been deleted, we will rebuild the ServingGroup in deletePod function
			err = c.kubeClientSet.CoreV1().Pods(ms.Namespace).Delete(c.operationContext(), newPod.Name, *metav1.NewPreconditionDeleteOptions(string(errPod.UID)))
			if err != nil {
				klog.Errorf("cannot delete pod %s after grace time, err: %v", newPod.Name, err)
				return
			}
			klog.V(2).Infof("%s been deleted after grace time", errPod.Name)
		}
	} else {
		// grace period is not set or the grace period is 0, the deletion will be executed immediately.
		defer c.graceMap.Delete(getPodGracePeriodKey(errPod))

		err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Delete(c.operationContext(), errPod.Name, *metav1.NewPreconditionDeleteOptions(string(errPod.UID)))
		if err != nil {
			klog.Errorf("cannot delete pod %s when it error, err: %v", errPod.Name, err)
			return
		}
		klog.V(2).Infof("%s been deleted without grace time", errPod.Name)
	}
}

func (c *ModelServingController) handleDeletedPod(ms *workloadv1alpha1.ModelServing, servingGroupName string, pod *corev1.Pod) error {
	// pod is deleted due to failure or other reasons and needs to be rebuilt according to the RecoveryPolicy
	switch ms.Spec.RecoveryPolicy {
	case workloadv1alpha1.ServingGroupRecreate:
		// Rebuild the entire ServingGroup directly
		if err := c.deleteServingGroup(c.operationContext(), ms, servingGroupName); err != nil {
			klog.Errorf("failed to delete ServingGroup %s: %v", servingGroupName, err)
			return err
		}
	case workloadv1alpha1.RoleRecreate:
		// If Rolling update in RoleRecreate mode, requires re-entering the queue during the pod delete event.
		if c.store.GetServingGroupStatus(utils.GetNamespaceName(ms), servingGroupName) == datastore.ServingGroupDeleting {
			if err := c.deleteServingGroup(c.operationContext(), ms, servingGroupName); err != nil {
				klog.Errorf("failed to delete ServingGroup %s: %v", servingGroupName, err)
				return err
			}
			return nil
		} else if c.store.GetServingGroupStatus(utils.GetNamespaceName(ms), servingGroupName) == datastore.ServingGroupRunning {
			// If the ServingGroup status is running when the pod fails, we need to set it to creating
			err := c.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), servingGroupName, datastore.ServingGroupCreating)
			klog.V(4).Infof("Setting ServingGroup %s/%s status to Creating when pod deleted for recreating", ms.GetName(), servingGroupName)
			if err != nil {
				return fmt.Errorf("failed to set ServingGroup %s status: %v", servingGroupName, err)
			}
		}
		if err := c.DeleteRole(c.operationContext(), ms, servingGroupName, utils.GetRoleName(pod), utils.GetRoleID(pod)); err != nil {
			return err
		}
	case workloadv1alpha1.NoneRestartPolicy:
		// None: re-enqueue to refill the single missing pod (not the whole role/group).
		if err := c.markPodUnavailable(ms, servingGroupName, pod); err != nil {
			klog.Warningf("mark pod %s unavailable: %v", pod.Name, err)
		}
		c.enqueueModelServing(ms)
	}
	return nil
}

func (c *ModelServingController) checkServingGroupReady(ms *workloadv1alpha1.ModelServing, servingGroupName string) (bool, error) {
	klog.V(4).Infof("checkServingGroupReady: modelServing=%s/%s, servingGroup=%s", ms.Namespace, ms.Name, servingGroupName)
	roles, err := c.rolesForServingGroupReadiness(ms, servingGroupName)
	if err != nil {
		return false, err
	}
	for _, role := range roles {
		roleList, err := c.store.GetRoleList(utils.GetNamespaceName(ms), servingGroupName, role.Name)
		if err != nil {
			return false, err
		}
		expectedReplicas := roleReplicas(role)
		if len(roleList) != expectedReplicas {
			klog.V(4).Infof("checkServingGroupReady: role %s in group %s not ready: replica count mismatch (%d/%d)",
				role.Name, servingGroupName, len(roleList), expectedReplicas)
			return false, nil
		}
		for _, r := range roleList {
			if r.Status != datastore.RoleRunning {
				klog.V(4).Infof("checkServingGroupReady: role %s/%s in group %s not ready: status=%s",
					role.Name, r.Name, servingGroupName, r.Status)
				return false, nil
			}
		}
	}
	klog.V(4).Infof("checkServingGroupReady: servingGroup %s is ready", servingGroupName)
	return true, nil
}

func (c *ModelServingController) checkRoleReady(ms *workloadv1alpha1.ModelServing, servingGroupName, roleName, roleID string) (bool, error) {
	pods, err := c.getPodsByIndex(RoleIDKey, fmt.Sprintf("%s/%s/%s/%s", ms.Namespace, servingGroupName, roleName, roleID))
	if err != nil {
		return false, err
	}
	instances, err := c.store.GetRoleList(utils.GetNamespaceName(ms), servingGroupName, roleName)
	if err != nil {
		return false, err
	}
	observed := datastore.Role{Name: roleID}
	for _, instance := range instances {
		if instance.Name == roleID {
			observed = instance
			break
		}
	}
	ctx, cancel := context.WithTimeout(c.operationContext(), 5*time.Second)
	defer cancel()
	history := c.revisionHistory(ctx, ms)
	template, revision, _, err := history.instanceTemplate(ctx, servingGroupName, roleName, observed, pods)
	if err != nil {
		klog.V(4).Infof("Role %s/%s: unresolved layout: %v", roleName, roleID, err)
		return false, err
	}
	expected := 1 + int(template.WorkerReplicas)
	byName := make(map[string]*corev1.Pod, len(pods))
	running := 0
	for _, pod := range pods {
		byName[pod.Name] = pod
		if pod.DeletionTimestamp == nil && utils.IsOwnedByModelServingWithUID(pod, ms.UID) && utils.IsPodRunningAndReady(pod) {
			running++
		}
	}
	if len(pods) != expected || running != expected {
		klog.V(4).Infof("Role %s/%s: %d/%d pods running and ready (observed %d)", roleName, roleID, running, expected, len(pods))
		return false, nil
	}
	for i := 0; i < expected; i++ {
		pod := byName[utils.GeneratePodName(servingGroupName, roleID, i)]
		if pod == nil {
			klog.V(4).Infof("Role %s/%s: missing Pod ordinal %d", roleName, roleID, i)
			return false, nil
		}
		matches, err := history.podMatchesTemplate(ctx, pod, template, revision)
		if err != nil || !matches {
			klog.V(4).Infof("Role %s/%s: Pod %s does not match revision %s: %v", roleName, roleID, pod.Name, revision, err)
			return false, err
		}
	}
	klog.V(4).Infof("Role %s/%s: all %d pods are running and ready", roleName, roleID, expected)
	return true, nil
}

// rolesForServingGroupReadiness returns the Role templates that should be used
// to evaluate the readiness of a ServingGroup. RoleRollingUpdate always uses
// the current templates because Roles are updated independently. During a
// ServingGroupRollingUpdate, a partition-protected ServingGroup may still run
// an older revision whose pod layout differs from the current spec, so its
// templates are loaded from the corresponding ControllerRevision and overlaid
// with the latest independently scalable replica counts.
func (c *ModelServingController) rolesForServingGroupReadiness(ms *workloadv1alpha1.ModelServing, servingGroupName string) ([]workloadv1alpha1.Role, error) {
	if ms.Spec.RolloutStrategy != nil && ms.Spec.RolloutStrategy.Type == workloadv1alpha1.RoleRollingUpdate {
		return ms.Spec.Template.Roles, nil
	}
	revision, ok := c.store.GetServingGroupRevision(utils.GetNamespaceName(ms), servingGroupName)
	if ok {
		revision = c.revisionForServingGroup(c.operationContext(), ms, datastore.ServingGroup{Name: servingGroupName, Revision: revision})
	}
	if !ok || revision == "" || revision == utils.ModelServingRevision(ms) {
		return ms.Spec.Template.Roles, nil
	}
	lookupCtx, cancel := context.WithTimeout(c.operationContext(), 5*time.Second)
	defer cancel()
	roles, err := c.revisionHistory(lookupCtx, ms).roles(lookupCtx, revision)
	if err != nil {
		return nil, err
	}
	return mergeLatestRoleReplicas(roles, ms.Spec.Template.Roles), nil
}

func (c *ModelServingController) isServingGroupOutdated(group datastore.ServingGroup, namespace, newRevision string) bool {
	// Find the pods corresponding to ServingGroup
	groupNameValue := fmt.Sprintf("%s/%s", namespace, group.Name)
	pods, err := c.getPodsByIndex(GroupNameKey, groupNameValue)
	if err != nil {
		klog.Errorf("cannot list pod when check ServingGroup %s/%s updated: %v", namespace, group.Name, err)
		return false
	}
	// Check all pods match the newHash
	for _, pod := range pods {
		if utils.ObjectRevision(pod) != newRevision {
			return true
		}
	}
	return false
}

// getModelServingByChildResource gets the ModelServing and group name for any resource that has the appropriate labels
func (c *ModelServingController) getModelServingByChildResource(resource metav1.Object) (*workloadv1alpha1.ModelServing, string, error) {
	modelServingName, servingGroupName, ok := utils.GetModelServingAndGroupByLabel(resource.GetLabels())
	if !ok {
		return nil, "", fmt.Errorf("cannot get modelServing name and ServingGroup name from resource %s/%s", resource.GetNamespace(), resource.GetName())
	}
	ms, err := c.modelServingLister.ModelServings(resource.GetNamespace()).Get(modelServingName)
	if err != nil {
		return nil, "", err
	}
	return ms, servingGroupName, nil
}

// shouldSkipHandling checks if a pod should be skipped based on owner mismatch or revision mismatch
func (c *ModelServingController) shouldSkipHandling(ms *workloadv1alpha1.ModelServing, servingGroupName string, obj metav1.Object) bool {
	if !utils.IsOwnedByModelServingWithUID(obj, ms.UID) {
		// If the pod is not owned by the ModelServing, we do not need to handle it.
		klog.V(4).Infof("object %s/%s maybe left from previous same named ModelServing %s/%s, skip handling",
			obj.GetNamespace(), obj.GetName(), ms.Namespace, ms.Name)
		return true
	}
	return false
}

func getMetaObject(obj interface{}) metav1.Object {
	if metaObj, ok := obj.(metav1.Object); ok {
		return metaObj
	}

	// Handle tombstone object
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		if metaObj, ok := tombstone.Obj.(metav1.Object); ok {
			return metaObj
		}
	}

	return nil
}

func isOwnedByModelServing(metaObj metav1.Object) bool {
	for _, ownerRef := range metaObj.GetOwnerReferences() {
		if ownerRef.APIVersion == workloadv1alpha1.SchemeGroupVersion.String() && ownerRef.Kind == "ModelServing" {
			return true
		}
	}
	return false
}

func isLabeledForModelServing(metaObj metav1.Object) bool {
	modelServingName, servingGroupName, ok := utils.GetModelServingAndGroupByLabel(metaObj.GetLabels())
	return ok && modelServingName != "" && servingGroupName != ""
}

// handleDeletionInProgress checks and handles deletion states for ServingGroup or Role.
// Returns true if the resource deletion is already in progress and the caller should stop further handling.
func (c *ModelServingController) handleDeletionInProgress(ms *workloadv1alpha1.ModelServing, servingGroupName, roleName, roleID string) bool {
	// check ServingGroup status
	if c.store.GetServingGroupStatus(utils.GetNamespaceName(ms), servingGroupName) == datastore.ServingGroupDeleting {
		// ServingGroup is already in the deletion process, only checking whether the deletion is completed
		if c.isServingGroupDeleted(ms, servingGroupName) {
			// ServingGroup has been deleted, so the storage needs to be updated and need to reconcile.
			klog.V(2).Infof("servingGroup %s has been deleted", servingGroupName)

			if err := c.runServingGroupDeletePlugins(c.operationContext(), ms, servingGroupName); err != nil {
				klog.Errorf("failed to execute OnServingGroupDelete hook: %v", err)
			}

			c.store.DeleteServingGroup(utils.GetNamespaceName(ms), servingGroupName)
			c.enqueueModelServing(ms)
		}
		return true
	}

	if roleName != "" && roleID != "" {
		// check role status
		if c.store.GetRoleStatus(utils.GetNamespaceName(ms), servingGroupName, roleName, roleID) == datastore.RoleDeleting {
			c.reconcileDeletingRole(c.operationContext(), ms, servingGroupName, roleName, roleID)
			return true
		}
	}

	return false
}

func (c *ModelServingController) reconcileDeletingRole(ctx context.Context, ms *workloadv1alpha1.ModelServing, servingGroupName, roleName, roleID string) bool {
	if c.isRoleDeleted(ms, servingGroupName, roleName, roleID) {
		return c.completeRoleDeletion(ctx, ms, servingGroupName, roleName, roleID, "cache")
	}
	if c.shouldLiveCheckRoleDeletion(ms, servingGroupName, roleName, roleID) {
		deleted, err := c.isRoleDeletedLive(ctx, ms, servingGroupName, roleName, roleID)
		if err != nil {
			klog.Warningf("failed live check for deleting role %s/%s/%s in ModelServing %s/%s: %v", servingGroupName, roleName, roleID, ms.Namespace, ms.Name, err)
		} else if deleted {
			return c.completeRoleDeletion(ctx, ms, servingGroupName, roleName, roleID, "live check")
		}
	}
	c.enqueueModelServingAfter(ms, roleDeletionRecheckDelay)
	return false
}

func (c *ModelServingController) completeRoleDeletion(ctx context.Context, ms *workloadv1alpha1.ModelServing, servingGroupName, roleName, roleID, source string) bool {
	if err := c.runRoleDeletePlugins(ctx, ms, servingGroupName, roleName, roleID); err != nil {
		klog.Errorf("failed to execute OnRoleDelete hook while completing role %s/%s/%s after %s: %v", servingGroupName, roleName, roleID, source, err)
		c.enqueueModelServingAfter(ms, roleDeletionRecheckDelay)
		return false
	}
	klog.V(2).Infof("role %s of servingGroup %s has been deleted after %s", roleID, servingGroupName, source)
	c.store.DeleteRole(utils.GetNamespaceName(ms), servingGroupName, roleName, roleID)
	c.clearRoleDeletionProgress(ms, servingGroupName, roleName, roleID)
	c.enqueueModelServing(ms)
	return true
}

func (c *ModelServingController) isServingGroupDeleted(ms *workloadv1alpha1.ModelServing, servingGroupName string) bool {
	status := c.store.GetServingGroupStatus(utils.GetNamespaceName(ms), servingGroupName)
	if status != datastore.ServingGroupDeleting {
		// It will be Determined whether all resource have been deleted only when the group status is deleting.
		return false
	}
	// check whether the ServingGroup deletion has been completed
	groupNameValue := fmt.Sprintf("%s/%s", ms.Namespace, servingGroupName)
	pods, err := c.getPodsByIndex(GroupNameKey, groupNameValue)
	if err != nil {
		klog.Errorf("failed to get pods for ServingGroup %s of ModelServing %s/%s: %v", servingGroupName, ms.Namespace, ms.Name, err)
		return false
	}
	pgs := []*schedulingv1beta1.PodGroup{}
	if c.podGroupManager.HasPodGroupCRD() {
		pgs, err = c.getPodGroupsByIndex(GroupNameKey, groupNameValue)
		if err != nil {
			klog.Errorf("failed to get podGroups for ServingGroup %s of ModelServing %s/%s: %v", servingGroupName, ms.Namespace, ms.Name, err)
			return false
		}
	}
	if c.observation != nil {
		services, err := c.servicesLister.Services(ms.Namespace).List(labels.SelectorFromSet(map[string]string{workloadv1alpha1.GroupNameLabelKey: servingGroupName}))
		if err != nil {
			return false
		}
		for _, service := range services {
			if utils.IsOwnedByModelServingWithUID(service, ms.UID) {
				return false
			}
		}
	}
	return len(filterPodGroupsOwnedByModelServing(pgs, ms.UID)) == 0 && len(filterPodsOwnedByModelServing(pods, ms.UID)) == 0
}

func filterPodsOwnedByModelServing(pods []*corev1.Pod, uid types.UID) []*corev1.Pod {
	owned := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if isOwnedByCurrentModelServing(pod, uid) {
			owned = append(owned, pod)
		}
	}
	return owned
}

func filterPodGroupsOwnedByModelServing(podGroups []*schedulingv1beta1.PodGroup, uid types.UID) []*schedulingv1beta1.PodGroup {
	owned := make([]*schedulingv1beta1.PodGroup, 0, len(podGroups))
	for _, podGroup := range podGroups {
		if isOwnedByCurrentModelServing(podGroup, uid) {
			owned = append(owned, podGroup)
		}
	}
	return owned
}

func isOwnedByCurrentModelServing(obj metav1.Object, uid types.UID) bool {
	if uid == "" {
		return true
	}
	return utils.IsOwnedByModelServingWithUID(obj, uid)
}

func (c *ModelServingController) isRoleDeleted(ms *workloadv1alpha1.ModelServing, servingGroupName, roleName, roleID string) bool {
	if c.store.GetRoleStatus(utils.GetNamespaceName(ms), servingGroupName, roleName, roleID) != datastore.RoleDeleting {
		// It will be Determined whether all resource have been deleted only when the role status is deleting.
		return false
	}
	roleIDValue := fmt.Sprintf("%s/%s/%s/%s", ms.Namespace, servingGroupName, roleName, roleID)
	// check whether the role deletion has been completed
	pods, err := c.getPodsByIndex(RoleIDKey, roleIDValue)
	if err != nil {
		klog.Errorf("failed to get pods for role %s/%s in ServingGroup %s of ModelServing %s/%s: %v", roleName, roleID, servingGroupName, ms.Namespace, ms.Name, err)
		return false
	}
	if c.observation != nil {
		services, err := c.servicesLister.Services(ms.Namespace).List(labels.SelectorFromSet(map[string]string{workloadv1alpha1.GroupNameLabelKey: servingGroupName, workloadv1alpha1.RoleLabelKey: roleName, workloadv1alpha1.RoleIDKey: roleID}))
		if err != nil {
			return false
		}
		for _, service := range services {
			if utils.IsOwnedByModelServingWithUID(service, ms.UID) {
				return false
			}
		}
		return len(filterPodsOwnedByModelServing(pods, ms.UID)) == 0
	}
	return len(pods) == 0
}

func (c *ModelServingController) isRoleDeletedLive(ctx context.Context, ms *workloadv1alpha1.ModelServing, servingGroupName, roleName, roleID string) (bool, error) {
	selector := labels.SelectorFromSet(map[string]string{
		workloadv1alpha1.GroupNameLabelKey: servingGroupName,
		workloadv1alpha1.RoleLabelKey:      roleName,
		workloadv1alpha1.RoleIDKey:         roleID,
	}).String()
	pods, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return false, err
	}
	for i := range pods.Items {
		if utils.IsOwnedByModelServingWithUID(&pods.Items[i], ms.UID) {
			return false, nil
		}
	}
	services, err := c.kubeClientSet.CoreV1().Services(ms.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return false, err
	}
	for i := range services.Items {
		if utils.IsOwnedByModelServingWithUID(&services.Items[i], ms.UID) {
			return false, nil
		}
	}
	return true, nil
}

func (c *ModelServingController) shouldLiveCheckRoleDeletion(ms *workloadv1alpha1.ModelServing, servingGroupName, roleName, roleID string) bool {
	key := roleDeletionKey(ms, servingGroupName, roleName, roleID)
	attempts := 1
	if value, ok := c.rootController().roleDeleteMap.Load(key); ok {
		if previous, ok := value.(int); ok {
			attempts = previous + 1
		}
	}
	c.rootController().roleDeleteMap.Store(key, attempts)
	return attempts >= roleDeletionLiveCheckThreshold
}

func (c *ModelServingController) clearRoleDeletionProgress(ms *workloadv1alpha1.ModelServing, servingGroupName, roleName, roleID string) {
	c.rootController().roleDeleteMap.Delete(roleDeletionKey(ms, servingGroupName, roleName, roleID))
}

func roleDeletionKey(ms *workloadv1alpha1.ModelServing, servingGroupName, roleName, roleID string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s", ms.Namespace, ms.Name, ms.UID, servingGroupName, roleName, roleID)
}

func modelServingKeyFromChildResource(obj metav1.Object) (string, bool) {
	if obj == nil {
		return "", false
	}
	modelServingName := obj.GetLabels()[workloadv1alpha1.ModelServingNameLabelKey]
	if modelServingName == "" {
		return "", false
	}
	return obj.GetNamespace() + "/" + modelServingName, true
}

// getPodsByIndex filter pods using the informer indexer.
func (c *ModelServingController) getPodsByIndex(indexName, indexValue string) ([]*corev1.Pod, error) {
	indexer := c.podsInformer.GetIndexer()
	if c.observation != nil {
		indexer = c.observation.pods
	}
	if _, exists := indexer.GetIndexers()[indexName]; !exists {
		return nil, fmt.Errorf("pod indexer %s not found", indexName)
	}
	objs, err := indexer.ByIndex(indexName, indexValue)
	if err != nil {
		return nil, err
	}

	var pods []*corev1.Pod
	for _, obj := range objs {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			klog.Errorf("unexpected object type in pod indexer: %T", obj)
			continue
		}
		pods = append(pods, pod)
	}
	return pods, nil
}

// TODO: move to podgroup manager
func (c *ModelServingController) getPodGroupsByIndex(indexName, indexValue string) ([]*schedulingv1beta1.PodGroup, error) {
	if c.observation != nil && c.observation.podGroups != nil {
		objects, err := c.observation.podGroups.ByIndex(indexName, indexValue)
		if err != nil {
			return nil, err
		}
		result := make([]*schedulingv1beta1.PodGroup, 0, len(objects))
		for _, object := range objects {
			result = append(result, object.(*schedulingv1beta1.PodGroup))
		}
		return result, nil
	}
	if c.podGroupManager == nil || !c.podGroupManager.HasPodGroupCRD() {
		return nil, nil
	}

	podGroupInformer := c.podGroupManager.GetPodGroupInformer()
	if podGroupInformer == nil {
		return nil, fmt.Errorf("podGroup informer is not initialized")
	}
	indexer := podGroupInformer.GetIndexer()
	if indexer == nil {
		return nil, fmt.Errorf("podGroup informer indexer is not initialized")
	}
	if _, exists := indexer.GetIndexers()[indexName]; !exists {
		return nil, fmt.Errorf("podGroup indexer %s not found", indexName)
	}
	objs, err := indexer.ByIndex(indexName, indexValue)
	if err != nil {
		return nil, err
	}

	var podGroups []*schedulingv1beta1.PodGroup
	for _, obj := range objs {
		podGroup, ok := obj.(*schedulingv1beta1.PodGroup)
		if !ok {
			klog.Errorf("unexpected object type in podGroup indexer: %T", obj)
			continue
		}
		podGroups = append(podGroups, podGroup)
	}
	return podGroups, nil
}

// UpdateModelServingStatus update replicas in modelServing status.
func (c *ModelServingController) UpdateModelServingStatus(ms *workloadv1alpha1.ModelServing, revision string) error {
	ctx := c.withRevisionHistory(c.operationContext(), ms)
	rolloutPolicy, err := c.resolveRoleRolloutPolicy(ctx, ms, revision)
	if err != nil {
		return err
	}
	return c.updateModelServingStatus(ctx, ms, revision, rolloutPolicy)
}

func (c *ModelServingController) updateModelServingStatus(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	revision string,
	rolloutPolicy *roleRolloutPolicy,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Get latest modelserving from informer store
		latestMS, getErr := c.modelServingLister.ModelServings(ms.Namespace).Get(ms.Name)
		if c.observation != nil {
			latestMS, getErr = c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Get(ctx, ms.Name, metav1.GetOptions{})
			if getErr == nil && (latestMS.UID != ms.UID || !reflect.DeepEqual(latestMS.Spec, ms.Spec)) {
				return fmt.Errorf("ModelServing changed during status update")
			}
		}
		if getErr != nil {
			return getErr
		}

		if latestMS.UID != ms.UID || latestMS.Generation != ms.Generation {
			return fmt.Errorf("ModelServing changed during status update")
		}

		// Calculate status based on latestMS
		groups, err := c.store.GetServingGroupByModelServing(utils.GetNamespaceName(latestMS))
		if err != nil && !errors.Is(err, datastore.ErrServingGroupNotFound) {
			return err
		}

		replicas := modelServingReplicas(latestMS)
		partition, _, partitionErr := c.getPartition(modelServingPartition(latestMS), replicas)
		if partitionErr != nil {
			return fmt.Errorf("failed to resolve rollout partition: %v", partitionErr)
		}
		available, updated := 0, 0
		progressingGroups, updatedGroups, currentGroups := []int{}, []int{}, []int{}
		// Track revision counts to determine the most common non-updated revision (CurrentRevision)
		revisionCount := make(map[string]int)
		referencedRevisions := make([]string, 0, len(groups))
		rolloutActive := c.hasUpdateableOutdatedServingGroup(ctx, latestMS, groups, revision, partition)
		for index := range groups {
			group := groups[index]
			_, ordinal := utils.GetParentNameAndOrdinal(group.Name)
			if group.Revision != "" {
				referencedRevisions = append(referencedRevisions, group.Revision)
			}
			// Independent Role updates can leave multiple observed revisions in
			// one group. Preserve every live snapshot for comparison and recovery.
			rolesByName, rolesErr := c.store.GetRolesByGroup(utils.GetNamespaceName(latestMS), group.Name)
			if rolesErr != nil {
				return rolesErr
			}
			for _, roles := range rolesByName {
				for _, role := range roles {
					if role.Revision != "" {
						referencedRevisions = append(referencedRevisions, role.Revision)
					}
				}
			}
			if group.Status == datastore.ServingGroupDeleting {
				// Scaling -> Running or
				// Creating -> Running
				// No Deleting -> Running.
				// So directly add deleting groups to progressingGroups
				progressingGroups = append(progressingGroups, ordinal)
				continue
			}

			if group.Status == datastore.ServingGroupRunning {
				available = available + 1
			} else if ok, err := c.checkServingGroupReady(latestMS, group.Name); ok && err == nil {
				// some scenarios, pod events may not trigger group status updates, such as role scaling down.
				err = c.store.UpdateServingGroupStatus(utils.GetNamespaceName(latestMS), group.Name, datastore.ServingGroupRunning)
				if err != nil {
					return fmt.Errorf("failed to set servingGroup %s status: %v", group.Name, err)
				}
				available = available + 1
				klog.V(2).Infof("Update servingGroup %s status to Running", group.Name)
			} else {
				progressingGroups = append(progressingGroups, ordinal)
			}

			if c.compareServingGroupTemplate(ctx, latestMS, group, revision) == templateEquivalent {
				updated = updated + 1
				updatedGroups = append(updatedGroups, ordinal)
			} else {
				currentGroups = append(currentGroups, ordinal)
				// Count revisions for non-updated groups to find the most common one
				revisionCount[group.Revision]++
			}
		}
		progressActive := len(progressingGroups) > 0 || len(groups) != replicas || available != replicas

		copy := latestMS.DeepCopy()
		shouldUpdate := utils.SetConditionWithRolloutAndProgressState(
			copy, progressingGroups, updatedGroups, currentGroups, rolloutActive, progressActive,
		)
		coordinationConditionChanged, coordinationCondition := rolloutPolicy.setCondition(copy)
		shouldUpdate = shouldUpdate || coordinationConditionChanged

		// Update revision fields following StatefulSet's logic:
		// 1. UpdateRevision is always the new revision being applied
		// 2. CurrentRevision is read from Status.CurrentRevision if it exists and is still valid
		// 3. If Status.CurrentRevision doesn't exist or is invalid, compute from current groups
		// 4. When all groups are updated, CurrentRevision = UpdateRevision
		updateRevision := revision
		var currentRevision string
		rolloutComplete := updated == replicas && available == replicas && len(groups) == replicas

		// First, try to use existing CurrentRevision from status if it's still valid
		if copy.Status.CurrentRevision != "" {
			// Check if CurrentRevision is still valid (exists in non-updated groups)
			if len(revisionCount) > 0 {
				// Check if the existing CurrentRevision is still used by some groups
				if count, exists := revisionCount[copy.Status.CurrentRevision]; exists && count > 0 {
					currentRevision = copy.Status.CurrentRevision
				}
			}
			// Promote only after every desired ServingGroup is updated and ready
			// and temporary capacity has been fully removed.
			if rolloutComplete {
				currentRevision = updateRevision
			} else if currentRevision == "" && (rolloutActive || available != len(groups) || (rolloutPolicy != nil && rolloutPolicy.inProgress)) {
				// Desired replicas may all be updated while temporary capacity is
				// still draining. Keep the previous CurrentRevision until the total
				// ServingGroup count has converged.
				currentRevision = copy.Status.CurrentRevision
			}
		}

		// If CurrentRevision is not set (either not in status or invalid), compute it from current groups
		if currentRevision == "" {
			if rolloutComplete || len(revisionCount) == 0 {
				// All groups are updated or no groups exist
				currentRevision = updateRevision
			} else {
				// Find the revision with the highest count among non-updated groups
				maxCount := 0
				for rev, count := range revisionCount {
					if count > maxCount {
						maxCount = count
						currentRevision = rev
					}
				}
				// If no current revision found (shouldn't happen), fallback to updateRevision
				if currentRevision == "" {
					currentRevision = updateRevision
				}
			}
		}

		current := 0
		for _, group := range groups {
			if (currentRevision == revision && c.compareServingGroupTemplate(ctx, latestMS, group, revision) == templateEquivalent) ||
				(currentRevision != revision && group.Revision == currentRevision) {
				current++
			}
		}
		if copy.Status.Replicas != int32(len(groups)) || copy.Status.AvailableReplicas != int32(available) || copy.Status.UpdatedReplicas != int32(updated) || copy.Status.CurrentReplicas != int32(current) {
			shouldUpdate = true
			copy.Status.Replicas = int32(len(groups))
			copy.Status.AvailableReplicas = int32(available)
			copy.Status.UpdatedReplicas = int32(updated)
			copy.Status.CurrentReplicas = int32(current)
		}

		if copy.Status.CurrentRevision != currentRevision || copy.Status.UpdateRevision != updateRevision {
			shouldUpdate = true
			copy.Status.CurrentRevision = currentRevision
			copy.Status.UpdateRevision = updateRevision
		}

		if copy.Spec.RolloutStrategy == nil || copy.Spec.RolloutStrategy.RollingUpdateConfiguration == nil || copy.Spec.RolloutStrategy.RollingUpdateConfiguration.Partition == nil {
			// if not set spec.RolloutStrategy.RollingUpdateConfiguration.Partition,
			// should set currentReplicas = updatedReplicas when rolling update is over.
			if copy.Status.UpdatedReplicas == int32(replicas) &&
				copy.Status.AvailableReplicas == int32(replicas) &&
				copy.Status.Replicas == int32(replicas) {
				shouldUpdate = true
				copy.Status.CurrentReplicas = copy.Status.UpdatedReplicas
			}
		}

		if copy.Status.ObservedGeneration != latestMS.Generation {
			shouldUpdate = true
			copy.Status.ObservedGeneration = latestMS.Generation
		}

		// Set labelSelector so the scale subresource can report it to HPA.
		// spec.replicas counts ServingGroups, not pods, so the selector must
		// match exactly one pod per group — otherwise HPA/KEDA sees a pod count
		// that is a multiple of the group count and scales incorrectly.
		// Pin to the entry pod of the 0th instance of the first role: there is
		// exactly one such pod per group, regardless of role.Replicas.
		selectorSet := labels.Set{
			workloadv1alpha1.ModelServingNameLabelKey: latestMS.Name,
			workloadv1alpha1.EntryLabelKey:            utils.Entry,
		}
		if len(latestMS.Spec.Template.Roles) > 0 {
			roleName := latestMS.Spec.Template.Roles[0].Name
			selectorSet[workloadv1alpha1.RoleLabelKey] = roleName
			selectorSet[workloadv1alpha1.RoleIDKey] = utils.GenerateRoleID(roleName, 0)
		}
		selector := selectorSet.String()
		if copy.Status.LabelSelector != selector {
			shouldUpdate = true
			copy.Status.LabelSelector = selector
		}

		if shouldUpdate {
			_, err := c.modelServingClient.WorkloadV1alpha1().ModelServings(copy.GetNamespace()).UpdateStatus(c.operationContext(), copy, metav1.UpdateOptions{})
			if err != nil {
				return err
			}
			if coordinationConditionChanged && coordinationCondition != nil {
				eventType := corev1.EventTypeNormal
				if coordinationCondition.Status == metav1.ConditionTrue {
					eventType = corev1.EventTypeWarning
				}
				c.emitRoleStatusEvent(latestMS, eventType, coordinationCondition.Reason, coordinationCondition.Message)
			}
		}

		// Retry cleanup even when status has already converged. The last Pod
		// reference may disappear later, or a previous cleanup may have failed.
		return utils.CleanupOldControllerRevisions(ctx, c.kubeClientSet, copy, referencedRevisions...)
	})
}

// getPartition resolves an absolute partition from the configuration selected
// for the active rollout granularity.
// Returns (0, false, nil) when partition is not configured.
// If partition is a percentage, it is calculated from replicas (rounded up).
func (c *ModelServingController) getPartition(partitionConfig *intstr.IntOrString, replicas int) (int, bool, error) {
	if partitionConfig == nil {
		return 0, false, nil
	}
	// Percentage partition requires replicas to compute the absolute value.
	partition, err := intstr.GetScaledValueFromIntOrPercent(partitionConfig, replicas, true)
	if err != nil {
		return 0, true, err
	}
	return partition, true, nil
}

func modelServingPartition(ms *workloadv1alpha1.ModelServing) *intstr.IntOrString {
	if ms.Spec.RolloutStrategy == nil || ms.Spec.RolloutStrategy.RollingUpdateConfiguration == nil {
		return nil
	}
	return ms.Spec.RolloutStrategy.RollingUpdateConfiguration.Partition
}

func rolePartition(ms *workloadv1alpha1.ModelServing, role workloadv1alpha1.Role) *intstr.IntOrString {
	if ms.Spec.RolloutStrategy == nil || ms.Spec.RolloutStrategy.Type != workloadv1alpha1.RoleRollingUpdate {
		return nil
	}
	return role.Partition
}

func modelServingReplicas(ms *workloadv1alpha1.ModelServing) int {
	if ms.Spec.Replicas == nil {
		return 0
	}
	return int(*ms.Spec.Replicas)
}

func roleReplicas(role workloadv1alpha1.Role) int {
	if role.Replicas == nil {
		return 1
	}
	return int(*role.Replicas)
}

// forEachMissingOrdinal visits at most limit missing ordinals in [0, expectedCount).
// Memory usage depends on the observed ordinals rather than the user-provided
// expectedCount, which may be as large as math.MaxInt32.
// Invalid, out-of-range, duplicate, and unsorted existing ordinals are tolerated.
func forEachMissingOrdinal(expectedCount int, existingOrdinals []int, limit int, handler func(int) bool) {
	if expectedCount <= 0 || limit <= 0 {
		return
	}

	existing := make(map[int]struct{}, min(len(existingOrdinals), expectedCount))
	for _, ordinal := range existingOrdinals {
		if ordinal >= 0 && ordinal < expectedCount {
			existing[ordinal] = struct{}{}
		}
	}

	visited := 0
	for ordinal := 0; ordinal < expectedCount && visited < limit; ordinal++ {
		if _, found := existing[ordinal]; found {
			continue
		}
		visited++
		if !handler(ordinal) {
			return
		}
	}
}

// scaleDownServingGroups scales down the ServingGroups to the expected count with two-level priority-based selection:
// 1. Primary: Not-ready groups (Creating, NotFound) are deleted first
// 2. Secondary: Among groups with same status, lower deletion cost = delete first
// When partition is set, the first N replicas (where N = partition) are protected.
// Non-protected replicas (after the first N) are deleted first, then protected replicas if needed.
func (c *ModelServingController) scaleDownServingGroups(ctx context.Context, ms *workloadv1alpha1.ModelServing, servingGroupList []datastore.ServingGroup, expectedCount int) error {
	partition, _, _ := c.getPartition(modelServingPartition(ms), modelServingReplicas(ms))

	protectedGroupNames := sets.New[string]()
	for _, group := range servingGroupList {
		_, ordinal := utils.GetParentNameAndOrdinal(group.Name)
		if ordinal >= 0 && ordinal < partition {
			protectedGroupNames.Insert(group.Name)
		}
	}

	// Calculate scores for all servingGroups first
	allScores := make([]ServingGroupWithScore, 0, len(servingGroupList))
	for _, group := range servingGroupList {
		scoreInfo := c.calculateServingGroupScore(ms, group.Name)
		allScores = append(allScores, scoreInfo)
	}

	var protectedScores []ServingGroupWithScore
	var nonProtectedScores []ServingGroupWithScore
	for _, score := range allScores {
		if protectedGroupNames.Has(score.Name) {
			protectedScores = append(protectedScores, score)
		} else {
			nonProtectedScores = append(nonProtectedScores, score)
		}
	}

	// Sort both lists by priority tuple: (priority, deletionCost, index)
	// Lower priority value = higher deletion priority (delete first)
	// Lower deletion cost = higher deletion priority
	// Higher index = higher deletion priority (backward compatibility)
	sortGroups := func(a, b ServingGroupWithScore) int {
		// Primary: Sort by priority (not-ready first)
		if a.Priority != b.Priority {
			return cmp.Compare(a.Priority, b.Priority) // Ascending: lower priority (not-ready) first
		}

		// Secondary: Among groups with same priority, lower deletion cost comes first
		if a.DeletionCost != b.DeletionCost {
			return cmp.Compare(a.DeletionCost, b.DeletionCost) // Ascending: lower cost first
		}

		// Tertiary: Higher index comes first (backward compatibility)
		return cmp.Compare(b.Index, a.Index) // Descending: higher indices first
	}

	slices.SortFunc(nonProtectedScores, sortGroups)

	totalToDelete := max(0, len(servingGroupList)-expectedCount)

	var err []error
	// Delete non-protected groups first (replicas after the first partition replicas)
	numNonProtectedToDelete := min(totalToDelete, len(nonProtectedScores))

	for i := 0; i < numNonProtectedToDelete; i++ {
		targetGroup := nonProtectedScores[i]
		klog.V(2).Infof("Scaling down non-protected serving group %s (priority: %d, deletion cost: %d, index: %d)",
			targetGroup.Name, targetGroup.Priority, targetGroup.DeletionCost, targetGroup.Index)
		if e := c.deleteServingGroup(ctx, ms, targetGroup.Name); e != nil {
			err = append(err, e)
		}
	}

	// After all non-protected groups are deleted, proceed to delete protected groups if needed
	remainingToDelete := totalToDelete - numNonProtectedToDelete
	if remainingToDelete > 0 && partition > 0 {
		// Sort protected scores only when we need to delete them
		slices.SortFunc(protectedScores, sortGroups)
		numProtectedToDelete := min(remainingToDelete, len(protectedScores))

		for i := 0; i < numProtectedToDelete; i++ {
			targetGroup := protectedScores[i]
			klog.V(2).Infof("Scaling down protected serving group %s (priority: %d, deletion cost: %d, index: %d, partition=%d)",
				targetGroup.Name, targetGroup.Priority, targetGroup.DeletionCost, targetGroup.Index, partition)
			if e := c.deleteServingGroup(ctx, ms, targetGroup.Name); e != nil {
				err = append(err, e)
			}
		}
	}

	if len(err) > 0 {
		return errors.Join(err...)
	}

	return nil
}

func (c *ModelServingController) buildPluginChain(ms *workloadv1alpha1.ModelServing) (*plugins.Chain, error) {
	if ms == nil || len(ms.Spec.Plugins) == 0 {
		return nil, nil
	}
	if c.pluginsRegistry == nil {
		return nil, fmt.Errorf("plugin registry is not initialized")
	}
	return plugins.NewChain(c.pluginsRegistry, ms.Spec.Plugins)
}

func (c *ModelServingController) runRoleDeletePlugins(ctx context.Context, ms *workloadv1alpha1.ModelServing, groupName, roleName, roleID string) error {
	if c.pluginsRegistry == nil {
		return fmt.Errorf("plugin registry is not initialized")
	}
	chain, err := plugins.NewRoleDeleteChain(c.pluginsRegistry, ms.Spec.Plugins)
	if err != nil {
		return fmt.Errorf("build Role delete plugin chain: %w", err)
	}
	_, roleIndex := utils.GetParentNameAndOrdinal(roleID)
	var role *workloadv1alpha1.Role
	for i := range ms.Spec.Template.Roles {
		if ms.Spec.Template.Roles[i].Name == roleName {
			role = ms.Spec.Template.Roles[i].DeepCopy()
			break
		}
	}
	return chain.OnRoleDelete(ctx, &plugins.HookRequest{
		ModelServing:    ms,
		ServingGroup:    groupName,
		RoleName:        roleName,
		RoleID:          roleID,
		RoleIndex:       roleIndex,
		Role:            role,
		KubeClient:      c.kubeClientSet,
		PodLister:       c.podsLister,
		ConfigMapLister: c.configMapsLister,
		ServiceLister:   c.servicesLister,
	})
}

func (c *ModelServingController) runServingGroupDeletePlugins(ctx context.Context, ms *workloadv1alpha1.ModelServing, groupName string) error {
	chain, err := c.buildPluginChain(ms)
	if err != nil {
		return fmt.Errorf("build ServingGroup delete plugin chain: %w", err)
	}
	if chain == nil {
		return nil
	}
	return chain.OnServingGroupDelete(ctx, &plugins.HookRequest{
		ModelServing:    ms,
		ServingGroup:    groupName,
		KubeClient:      c.kubeClientSet,
		ConfigMapLister: c.configMapsLister,
		ServiceLister:   c.servicesLister,
	})
}

func (c *ModelServingController) CreatePodsForServingGroup(ctx context.Context, ms *workloadv1alpha1.ModelServing, servingGroupIndex int, revision string, roles []workloadv1alpha1.Role) error {
	servingGroupName := utils.GenerateServingGroupName(ms.Name, servingGroupIndex)
	for _, role := range roles {
		roleTemplateHash := utils.CalRoleTemplateHash(role)
		replicas := int(*role.Replicas)
		for i := 0; i < replicas; i++ {
			err := c.CreatePodsByRole(ctx, *role.DeepCopy(), ms, i, servingGroupIndex, revision, roleTemplateHash)
			if err != nil {
				return err
			}
			roleID := utils.GenerateRoleID(role.Name, i)
			c.store.AddRole(utils.GetNamespaceName(ms), servingGroupName, role.Name, roleID, revision, roleTemplateHash)
			// Emit event for new role entering Creating state
			message := fmt.Sprintf("Role %s/%s in ServingGroup %s is now Creating", role.Name, roleID, servingGroupName)
			c.emitRoleStatusEvent(ms, corev1.EventTypeNormal, "RoleCreating", message)
		}
	}
	return nil
}

func (c *ModelServingController) CreatePodsByRole(ctx context.Context, role workloadv1alpha1.Role, ms *workloadv1alpha1.ModelServing, roleIndex int, servingGroupOrdinal int, revision string, roleTemplateHash string) error {
	servingGroupName := utils.GenerateServingGroupName(ms.Name, servingGroupOrdinal)
	// TODO(hzxuzhonghu): build the plugin chain only once per ModelServing
	// This is not critical now, so we leave it for future optimization.
	chain, err := c.buildPluginChain(ms)
	if err != nil {
		return fmt.Errorf("build plugin chain: %w", err)
	}
	roleID := utils.GenerateRoleID(role.Name, roleIndex)
	entryPod := utils.GenerateEntryPod(role, ms, servingGroupName, roleID, revision, roleTemplateHash)
	taskName := c.podGroupManager.GenerateTaskName(role.Name, roleIndex)
	c.podGroupManager.AnnotatePodWithPodGroup(entryPod, ms, servingGroupName, taskName)
	if err := c.createPod(ctx, ms, servingGroupName, role.Name, roleID, role.DeepCopy(), entryPod, true, chain, "entry"); err != nil {
		return err
	}
	if role.WorkerReplicas > 0 && role.WorkerTemplate == nil {
		klog.Errorf("WorkerTemplate is required when workerReplicas > 0 for role %s. This should have been caught by webhook validation.", role.Name)
		return nil
	}

	for i := 1; i <= int(role.WorkerReplicas); i++ {
		workerPod := utils.GenerateWorkerPod(role, ms, servingGroupName, roleID, i, revision, roleTemplateHash)
		c.podGroupManager.AnnotatePodWithPodGroup(workerPod, ms, servingGroupName, taskName)
		if err := c.createPod(ctx, ms, servingGroupName, role.Name, roleID, role.DeepCopy(), workerPod, false, chain, "worker"); err != nil {
			return err
		}
	}
	return nil
}

func (c *ModelServingController) createPod(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	servingGroupName string,
	roleName string,
	roleID string,
	role *workloadv1alpha1.Role,
	pod *corev1.Pod,
	isEntry bool,
	chain *plugins.Chain,
	roleKind string,
) error {
	if chain != nil {
		req := &plugins.HookRequest{
			ModelServing:    ms,
			ServingGroup:    servingGroupName,
			RoleName:        roleName,
			RoleID:          roleID,
			Role:            role,
			IsEntry:         isEntry,
			Pod:             pod,
			PodLister:       c.podsLister,
			ConfigMapLister: c.configMapsLister,
			KubeClient:      c.kubeClientSet,
			ServiceLister:   c.servicesLister,
		}
		if err := chain.OnPodCreate(ctx, req); err != nil {
			return fmt.Errorf("execute OnPodCreate failed for %s pod %s: %v", roleKind, pod.Name, err)
		}
	}

	_, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			existing, getErr := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			if getErr != nil {
				return fmt.Errorf("failed to verify existing %s pod %s: %v", roleKind, pod.Name, getErr)
			}
			ownedByCurrentModelServing := utils.IsOwnedByModelServingWithUID(existing, ms.UID)
			if ownedByCurrentModelServing && existing.DeletionTimestamp != nil {
				klog.V(4).Infof("%s pod %s already exists but is deleting, enqueueing to reconcile", roleKind, pod.Name)
				c.enqueueModelServingAfter(ms, enqueueAfter)
				return nil
			}
			if ownedByCurrentModelServing && utils.IsPodFailed(existing) {
				klog.V(4).Infof("%s pod %s already exists but has failed, deleting and enqueueing to reconcile", roleKind, pod.Name)
				if deleteErr := c.deleteConflictingPod(ctx, existing); deleteErr != nil && !apierrors.IsNotFound(deleteErr) {
					return fmt.Errorf("failed to delete failed %s pod %s: %v", roleKind, pod.Name, deleteErr)
				}
				c.enqueueModelServingAfter(ms, enqueueAfter)
				return nil
			}
			existingRoleTemplateHash := utils.ObjectRoleTemplateHash(existing)
			expectedRoleTemplateHash := utils.ObjectRoleTemplateHash(pod)
			roleTemplateHashMatches := false
			if ownedByCurrentModelServing && role != nil {
				matches, matchErr := c.revisionHistory(ctx, ms).podMatchesTemplate(ctx, existing, *role, utils.ObjectRevision(pod))
				if matchErr != nil {
					return fmt.Errorf("verify existing %s Pod %s template: %w", roleKind, pod.Name, matchErr)
				}
				roleTemplateHashMatches = matches
			}
			identityMatches := ownedByCurrentModelServing &&
				existing.Labels[workloadv1alpha1.ModelServingNameLabelKey] == ms.Name &&
				existing.Labels[workloadv1alpha1.GroupNameLabelKey] == servingGroupName &&
				existing.Labels[workloadv1alpha1.RoleLabelKey] == roleName &&
				existing.Labels[workloadv1alpha1.RoleIDKey] == roleID &&
				roleTemplateHashMatches
			if !identityMatches {
				labelsMatch := existing.Labels[workloadv1alpha1.ModelServingNameLabelKey] == ms.Name &&
					existing.Labels[workloadv1alpha1.GroupNameLabelKey] == servingGroupName &&
					existing.Labels[workloadv1alpha1.RoleLabelKey] == roleName &&
					existing.Labels[workloadv1alpha1.RoleIDKey] == roleID
				if labelsMatch && !ownedByCurrentModelServing {
					if deleteErr := c.deleteConflictingPod(ctx, existing); deleteErr != nil && !apierrors.IsNotFound(deleteErr) {
						return fmt.Errorf("failed to delete conflicting %s pod %s: %v", roleKind, pod.Name, deleteErr)
					}
				}
				c.enqueueModelServingAfter(ms, enqueueAfter)
				return fmt.Errorf("existing %s pod %s does not match expected identity: owner=%t group=%q/%q role=%q/%q roleID=%q/%q revision=%q/%q roleTemplateHash=%q/%q",
					roleKind, pod.Name,
					ownedByCurrentModelServing,
					existing.Labels[workloadv1alpha1.GroupNameLabelKey], servingGroupName,
					existing.Labels[workloadv1alpha1.RoleLabelKey], roleName,
					existing.Labels[workloadv1alpha1.RoleIDKey], roleID,
					utils.ObjectRevision(existing), utils.ObjectRevision(pod),
					existingRoleTemplateHash, expectedRoleTemplateHash)
			}
		} else {
			return fmt.Errorf("failed to create %s pod %s: %v", roleKind, pod.Name, err)
		}
	}

	return nil
}

func (c *ModelServingController) deleteConflictingPod(ctx context.Context, pod *corev1.Pod) error {
	deleteOptions := metav1.DeleteOptions{}
	if pod.UID != "" {
		uid := pod.UID
		deleteOptions.Preconditions = &metav1.Preconditions{UID: &uid}
		if pod.ResourceVersion != "" {
			version := pod.ResourceVersion
			deleteOptions.Preconditions.ResourceVersion = &version
		}
	}
	return c.kubeClientSet.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, deleteOptions)
}

func (c *ModelServingController) deleteServingGroup(ctx context.Context, ms *workloadv1alpha1.ModelServing, servingGroupName string) error {
	if c.servingState != nil {
		c.servingState.groupDeletes[servingGroupName] = struct{}{}
	}
	status := c.store.GetServingGroupStatus(utils.GetNamespaceName(ms), servingGroupName)
	if status == datastore.ServingGroupNotFound {
		return nil
	}
	// Mark the ServingGroup deleting before deleting its Role resources so
	// active-role reconciliation cannot recreate them during teardown.
	err := c.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), servingGroupName, datastore.ServingGroupDeleting)
	if err != nil {
		klog.ErrorS(err, "Failed to update ServingGroup status", "namespace", ms.Namespace, "servingGroup", servingGroupName)
		return err
	}
	defer func() {
		if err != nil {
			// Due to the failure to delete the role.
			// It is necessary to roll back the roleStatus to enable subsequent deletion of the role.
			rollbackErr := c.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), servingGroupName, status)
			if rollbackErr != nil {
				klog.ErrorS(rollbackErr, "Failed to update ServingGroup status", "namespace", ms.Namespace, "servingGroup", servingGroupName)
			}
			c.enqueueModelServing(ms)
		}
	}()

	err = c.podGroupManager.DeletePodGroup(ctx, ms, servingGroupName)
	if err != nil {
		return fmt.Errorf("failed to delete PodGroup for ServingGroup %s: %v", servingGroupName, err)
	}

	selector := labels.SelectorFromSet(map[string]string{
		workloadv1alpha1.GroupNameLabelKey: servingGroupName,
	})
	err = c.kubeClientSet.CoreV1().Pods(ms.Namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: selector.String(),
	})
	if err != nil {
		return fmt.Errorf("failed to delete pods of ServingGroup %s: %v", servingGroupName, err)
	}

	rolesByName, err := c.store.GetRolesByGroup(utils.GetNamespaceName(ms), servingGroupName)
	if err != nil {
		return fmt.Errorf("get Roles for ServingGroup %s during deletion: %w", servingGroupName, err)
	}
	for roleName, roles := range rolesByName {
		for roleID := range roles {
			if err = c.runRoleDeletePlugins(ctx, ms, servingGroupName, roleName, roleID); err != nil {
				return err
			}
		}
	}
	if err = c.runServingGroupDeletePlugins(ctx, ms, servingGroupName); err != nil {
		return err
	}

	if c.isServingGroupDeleted(ms, servingGroupName) {
		klog.V(2).Infof("ServingGroup %s has been deleted", servingGroupName)
		c.store.DeleteServingGroup(utils.GetNamespaceName(ms), servingGroupName)
		// this is needed when a pod is deleted accidentally, and the ServingGroup is deleted completely
		// and the controller has no chance to supplement it.
		c.enqueueModelServing(ms)
	}
	return nil
}

func (c *ModelServingController) createOrUpdatePodGroupByServingGroup(ctx context.Context, ms *workloadv1alpha1.ModelServing, servingGroupName string) error {
	effective, err := c.revisionHistory(ctx, ms).podGroupModelServing(ctx, servingGroupName)
	if err != nil {
		return fmt.Errorf("resolve PodGroup layout for ServingGroup %s: %w", servingGroupName, err)
	}
	return c.createOrUpdatePodGroupByServingGroupWithRoles(ctx, ms, servingGroupName, effective.Spec.Template.Roles)
}

// createOrUpdatePodGroupByServingGroupWithRoles keeps the PodGroup gang size
// aligned with the exact Role layout used to create pods for this ServingGroup.
// In particular, partition-protected groups keep historical worker topology
// while independently scalable Role replicas come from the latest spec.
func (c *ModelServingController) createOrUpdatePodGroupByServingGroupWithRoles(
	ctx context.Context,
	ms *workloadv1alpha1.ModelServing,
	servingGroupName string,
	roles []workloadv1alpha1.Role,
) error {
	podGroupMS := ms.DeepCopy()
	podGroupMS.Spec.Template.Roles = roles
	if err, retryAfter := c.podGroupManager.CreateOrUpdatePodGroup(ctx, podGroupMS, servingGroupName); err != nil {
		if c.recorder != nil {
			c.recorder.Eventf(
				ms,
				corev1.EventTypeWarning,
				"PodGroupSyncFailed",
				"Failed to reconcile PodGroup %s: %v",
				servingGroupName,
				err,
			)
		}
		if retryAfter > 0 {
			klog.V(2).Infof("Retry syncing modelserving %s after %v: %v", servingGroupName, retryAfter, err)
			c.enqueueModelServingAfter(ms, retryAfter)
			return nil
		}
		return fmt.Errorf("failed to update PodGroup for ServingGroup %s: %v", servingGroupName, err)
	}
	return nil
}

// resolveRoleTemplateHash resolves role template hash from labels first.
// For legacy pods without roleTemplateHash label, fallback to:
// 1. get pod revision from labels
// 2. find corresponding ControllerRevision
// 3. hash the matched role from ControllerRevision
// If any step fails, return empty string and let rolling update handle reconciliation.
func (c *ModelServingController) resolveRoleTemplateHash(ms *workloadv1alpha1.ModelServing, roleName string, obj metav1.Object) string {
	roleTemplateHash := utils.ObjectRoleTemplateHash(obj)
	if roleTemplateHash != "" {
		return roleTemplateHash
	}

	revision := utils.ObjectRevision(obj)
	if revision == "" {
		klog.V(4).Infof("roleTemplateHash and revision labels are missing on object %s/%s, leave roleTemplateHash empty", obj.GetNamespace(), obj.GetName())
		return ""
	}

	if c == nil || c.kubeClientSet == nil {
		klog.V(4).Infof("kube client is nil when resolving roleTemplateHash for object %s/%s, leave empty", obj.GetNamespace(), obj.GetName())
		return ""
	}

	resolvedHash, ok := c.resolveRoleTemplateHashFromRevision(ms, revision, roleName)
	if ok {
		return resolvedHash
	}

	klog.V(4).Infof("role %s not found in ControllerRevision %s for ModelServing %s/%s, leave roleTemplateHash empty", roleName, revision, ms.Namespace, ms.Name)
	return ""
}

// resolveRoleTemplateHashFromRevision resolves roleTemplateHash from a revision's ControllerRevision.
// Returns (hash, true) when resolved, otherwise ("", false).
func (c *ModelServingController) resolveRoleTemplateHashFromRevision(ms *workloadv1alpha1.ModelServing, revision, roleName string) (string, bool) {
	if c == nil || c.kubeClientSet == nil || revision == "" {
		return "", false
	}

	ctx, cancel := context.WithTimeout(c.operationContext(), 5*time.Second)
	defer cancel()

	cr, err := utils.GetControllerRevision(ctx, c.kubeClientSet, ms, revision)
	if err != nil {
		klog.Warningf("failed to get ControllerRevision %s for ModelServing %s/%s: %v", revision, ms.Namespace, ms.Name, err)
		return "", false
	}
	if cr == nil || !metav1.IsControlledBy(cr, ms) {
		return "", false
	}

	roles, err := utils.GetRolesFromControllerRevision(cr)
	if err != nil {
		klog.Warningf("failed to parse roles from ControllerRevision %s for ModelServing %s/%s: %v", revision, ms.Namespace, ms.Name, err)
		return "", false
	}

	for _, role := range roles {
		if role.Name == roleName {
			return utils.CalRoleTemplateHash(role), true
		}
	}

	return "", false
}

// findOutdatedRolesInServingGroups finds outdated roles in serving groups and returns a map of serving group names to outdated role names
// This is a read-only classification; observed revisions remain unchanged.
func (c *ModelServingController) findOutdatedRolesInServingGroups(ctx context.Context, ms *workloadv1alpha1.ModelServing, servingGroups []datastore.ServingGroup, revision string) map[string][]string {
	outdatedRolesMap := make(map[string][]string)

	// Track desired Role names; template comparisons use the instance history.
	newRoleNames := make(map[string]bool)
	for _, role := range ms.Spec.Template.Roles {
		newRoleNames[role.Name] = true
	}

	for _, sg := range servingGroups {
		var outdatedRoleNames []string

		// Check each role in the current serving group
		for roleName := range newRoleNames {
			// Get a safe copy of the roles list from the store to avoid concurrent map iteration/write.
			roles, err := c.store.GetRoleList(utils.GetNamespaceName(ms), sg.Name, roleName)
			if err != nil {
				klog.Errorf("failed to get roles for ServingGroup %s, role %s: %v", sg.Name, roleName, err)
				continue
			}

			// Check if any instance of this role type is outdated
			hasOutdatedRole := false
			for _, role := range roles {
				comparison := c.compareRoleTemplate(ctx, ms, sg, roleName, role)
				if comparison == templateUnknown {
					// Unknown history cannot authorize template-driven deletion.
					klog.Warningf("skip outdated check for role %s/%s in ServingGroup %s because its historical template is unresolved", roleName, role.Name, sg.Name)
					continue
				}
				// Only a proven template difference makes an active Role outdated.
				if comparison == templateDifferent && role.Status != datastore.RoleDeleting {
					hasOutdatedRole = true
					break
				}
			}

			if hasOutdatedRole {
				outdatedRoleNames = append(outdatedRoleNames, roleName)
			}
		}

		// Additionally, check for roles that exist in the store but are not in the new spec
		// These roles should also be considered "outdated" and need to be deleted
		allRoles, err := c.store.GetRolesByGroup(utils.GetNamespaceName(ms), sg.Name)
		if err != nil {
			klog.Errorf("failed to get all roles for ServingGroup %s: %v", sg.Name, err)
			continue
		}
		for storedRoleName := range allRoles {
			if !newRoleNames[storedRoleName] {
				// This role exists in the store but not in the new spec, so it's outdated
				outdatedRoleNames = append(outdatedRoleNames, storedRoleName)
			}
		}

		if len(outdatedRoleNames) > 0 {
			// There are outdated roles in this serving group
			outdatedRolesMap[sg.Name] = outdatedRoleNames
		}
	}

	return outdatedRolesMap
}

// handleModelServingDatastoreCacheDump handles requests to dump the ServingGroup and role in dataStore cache.
func (c *ModelServingController) handleModelServingDatastoreCacheDump(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	data, err := c.store.DumpCache()
	if err != nil {
		klog.Errorf("failed to dump model serving datastore cache: %v", err)
		http.Error(w, "Failed to dump datastore cache", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(data); err != nil {
		klog.Errorf("failed to write cache dump response: %v", err)
	}
}

// RegisterModelServingDebugEndpoints registers debug endpoints for the ModelServingController
func (c *ModelServingController) RegisterModelServingDebugEndpoints(mux *http.ServeMux) {
	mux.HandleFunc("/debug/modelserving/cache", c.handleModelServingDatastoreCacheDump)
	if c.audit != nil {
		c.registerAuditMetrics(mux)
	}
}

func calMaxScaleDown(role workloadv1alpha1.Role, outdatedRoles []datastore.Role, allReplicas, newUnavailable int) (int, error) {
	// RoleRollingUpdate has an independent budget for each Role. The
	// ModelServing-level maxUnavailable is intentionally not consulted here.
	maxUnavailable, configured, err := utils.GetMaxUnavailableForRole(role)
	if err != nil {
		return 0, fmt.Errorf("failed to calculate maxUnavailable for role %s: %v", role.Name, err)
	}
	if !configured {
		return len(outdatedRoles), nil
	}
	expectedReplicas := 1
	if role.Replicas != nil {
		expectedReplicas = int(*role.Replicas)
	}
	minAvailable := expectedReplicas - maxUnavailable
	if minAvailable < 0 {
		minAvailable = 0
	}
	maxScaleDown := allReplicas - minAvailable - newUnavailable
	if maxScaleDown < 0 {
		maxScaleDown = 0
	}
	return maxScaleDown, nil
}
