/*
Copyright 2019 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/apache/rocketmq-client-go/v2/admin"

	"go.uber.org/zap"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	rbacv1listers "k8s.io/client-go/listers/rbac/v1"
	"k8s.io/client-go/tools/cache"

	"knative.dev/eventing/pkg/apis/eventing"
	eventingclientset "knative.dev/eventing/pkg/client/clientset/versioned"
	"knative.dev/eventing/pkg/logging"
	"knative.dev/eventing/pkg/reconciler/names"

	"knative.dev/pkg/apis"
	"knative.dev/pkg/controller"
	pkgreconciler "knative.dev/pkg/reconciler"

	"knative.dev/eventing-contrib/rocketmq/pkg/apis/messaging/v1alpha1"
	rocketmqclientset "knative.dev/eventing-contrib/rocketmq/pkg/client/clientset/versioned"
	rocketmqScheme "knative.dev/eventing-contrib/rocketmq/pkg/client/clientset/versioned/scheme"
	rocketmqChannelReconciler "knative.dev/eventing-contrib/rocketmq/pkg/client/injection/reconciler/messaging/v1alpha1/rocketmqchannel"
	listers "knative.dev/eventing-contrib/rocketmq/pkg/client/listers/messaging/v1alpha1"
	"knative.dev/eventing-contrib/rocketmq/pkg/reconciler/controller/resources"
	"knative.dev/eventing-contrib/rocketmq/pkg/utils"
)

const (
	// controllerAgentName is the string used by this controller to identify
	// itself when creating events.
	controllerAgentName = "rocketmq-ch-controller"

	// Name of the corev1.Events emitted from the reconciliation process.
	dispatcherDeploymentCreated     = "DispatcherDeploymentCreated"
	dispatcherDeploymentUpdated     = "DispatcherDeploymentUpdated"
	dispatcherDeploymentFailed      = "DispatcherDeploymentFailed"
	dispatcherServiceCreated        = "DispatcherServiceCreated"
	dispatcherServiceFailed         = "DispatcherServiceFailed"
	dispatcherServiceAccountCreated = "DispatcherServiceAccountCreated"
	dispatcherRoleBindingCreated    = "DispatcherRoleBindingCreated"

	dispatcherName = "rocketmq-ch-dispatcher"
)

func newReconciledNormal(namespace, name string) pkgreconciler.Event {
	return pkgreconciler.NewEvent(corev1.EventTypeNormal, "RocketmqChannelReconciled", "RocketmqChannel reconciled: \"%s/%s\"", namespace, name)
}

func newDeploymentWarn(err error) pkgreconciler.Event {
	return pkgreconciler.NewEvent(corev1.EventTypeWarning, "DispatcherDeploymentFailed", "Reconciling dispatcher Deployment failed with: %s", err)
}

func newDispatcherServiceWarn(err error) pkgreconciler.Event {
	return pkgreconciler.NewEvent(corev1.EventTypeWarning, "DispatcherServiceFailed", "Reconciling dispatcher Service failed with: %s", err)
}

func newServiceAccountWarn(err error) pkgreconciler.Event {
	return pkgreconciler.NewEvent(corev1.EventTypeWarning, "DispatcherServiceAccountFailed", "Reconciling dispatcher ServiceAccount failed: %s", err)
}

func newRoleBindingWarn(err error) pkgreconciler.Event {
	return pkgreconciler.NewEvent(corev1.EventTypeWarning, "DispatcherRoleBindingFailed", "Reconciling dispatcher RoleBinding failed: %s", err)
}

func init() {
	// Add run types to the default Kubernetes Scheme so Events can be
	// logged for run types.
	_ = rocketmqScheme.AddToScheme(scheme.Scheme)
}

// Reconciler reconciles Rocketmq Channels.
type Reconciler struct {
	KubeClientSet kubernetes.Interface

	EventingClientSet eventingclientset.Interface

	systemNamespace string
	dispatcherImage string

	rocketmqConfig      *utils.RocketmqConfig
	rocketmqConfigError error
	rocketmqClientSet   rocketmqclientset.Interface

	rocketmqClusterAdmin admin.Admin

	rocketmqchannelLister   listers.RocketmqChannelLister
	rocketmqchannelInformer cache.SharedIndexInformer
	deploymentLister        appsv1listers.DeploymentLister
	serviceLister           corev1listers.ServiceLister
	endpointsLister         corev1listers.EndpointsLister
	serviceAccountLister    corev1listers.ServiceAccountLister
	roleBindingLister       rbacv1listers.RoleBindingLister
}

var (
	scopeNamespace = "namespace"
	scopeCluster   = "cluster"
)

type envConfig struct {
	Image string `envconfig:"DISPATCHER_IMAGE" required:"true"`
}

// Check that our Reconciler implements rocketmq's injection Interface
var _ rocketmqChannelReconciler.Interface = (*Reconciler)(nil)
var _ rocketmqChannelReconciler.Finalizer = (*Reconciler)(nil)

func (r *Reconciler) ReconcileKind(ctx context.Context, rc *v1alpha1.RocketmqChannel) pkgreconciler.Event {
	rc.Status.InitializeConditions()

	logger := logging.FromContext(ctx)
	// Verify channel is valid.
	rc.SetDefaults(ctx)
	if err := rc.Validate(ctx); err != nil {
		logger.Error("Invalid rocketmq channel", zap.String("channel", rc.Name), zap.Error(err))
		return err
	}

	if r.rocketmqConfig == nil {
		if r.rocketmqConfigError == nil {
			r.rocketmqConfigError = errors.New("The config map 'config-rocketmq' does not exist")
		}
		rc.Status.MarkConfigFailed("MissingConfiguration", "%v", r.rocketmqConfigError)
		return r.rocketmqConfigError
	}

	rocketmqClusterAdmin, err := r.createClient(ctx, rc)
	if err != nil {
		rc.Status.MarkConfigFailed("InvalidConfiguration", "Unable to build Rocketmq admin client for channel %s: %v", rc.Name, err)
		return err
	}

	rc.Status.MarkConfigTrue()

	// We reconcile the status of the Channel by looking at:
	// 1. Rocketmq topic used by the channel.
	// 2. Dispatcher Deployment for it's readiness.
	// 3. Dispatcher k8s Service for it's existence.
	// 4. Dispatcher endpoints to ensure that there's something backing the Service.
	// 5. K8s service representing the channel that will use ExternalName to point to the Dispatcher k8s service.

	if err := r.createTopic(ctx, rc, rocketmqClusterAdmin); err != nil {
		rc.Status.MarkTopicFailed("TopicCreateFailed", "error while creating topic: %s", err)
		return err
	}
	rc.Status.MarkTopicTrue()

	scope, ok := rc.Annotations[eventing.ScopeAnnotationKey]
	if !ok {
		scope = scopeCluster
	}

	dispatcherNamespace := r.systemNamespace
	if scope == scopeNamespace {
		dispatcherNamespace = rc.Namespace
	}

	// Make sure the dispatcher deployment exists and propagate the status to the Channel
	_, err = r.reconcileDispatcher(ctx, scope, dispatcherNamespace, rc)
	if err != nil {
		return err
	}

	// Make sure the dispatcher service exists and propagate the status to the Channel in case it does not exist.
	// We don't do anything with the service because it's status contains nothing useful, so just do
	// an existence check. Then below we check the endpoints targeting it.
	_, err = r.reconcileDispatcherService(ctx, dispatcherNamespace, rc)
	if err != nil {
		return err
	}

	// Get the Dispatcher Service Endpoints and propagate the status to the Channel
	// endpoints has the same name as the service, so not a bug.
	e, err := r.endpointsLister.Endpoints(dispatcherNamespace).Get(dispatcherName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			rc.Status.MarkEndpointsFailed("DispatcherEndpointsDoesNotExist", "Dispatcher Endpoints does not exist")
		} else {
			logger.Error("Unable to get the dispatcher endpoints", zap.Error(err))
			rc.Status.MarkEndpointsFailed("DispatcherEndpointsGetFailed", "Failed to get dispatcher endpoints")
		}
		return err
	}

	if len(e.Subsets) == 0 {
		logger.Error("No endpoints found for Dispatcher service", zap.Error(err))
		rc.Status.MarkEndpointsFailed("DispatcherEndpointsNotReady", "There are no endpoints ready for Dispatcher service")
		return fmt.Errorf("there are no endpoints ready for Dispatcher service %s", dispatcherName)
	}
	rc.Status.MarkEndpointsTrue()

	// Reconcile the k8s service representing the actual Channel. It points to the Dispatcher service via ExternalName
	svc, err := r.reconcileChannelService(ctx, dispatcherNamespace, rc)
	if err != nil {
		return err
	}
	rc.Status.MarkChannelServiceTrue()
	rc.Status.SetAddress(&apis.URL{
		Scheme: "http",
		Host:   names.ServiceHostName(svc.Name, svc.Namespace),
	})

	// close the connection (TODO)
	//err = rocketmqClusterAdmin.Close()
	//if err != nil {
	//	logger.Error("Error closing the connection", zap.Error(err))
	//	return err
	//}

	// Ok, so now the Dispatcher Deployment & Service have been created, we're golden since the
	// dispatcher watches the Channel and where it needs to dispatch events to.
	return newReconciledNormal(rc.Namespace, rc.Name)
}

func (r *Reconciler) reconcileDispatcher(ctx context.Context, scope string, dispatcherNamespace string, rc *v1alpha1.RocketmqChannel) (*appsv1.Deployment, error) {
	if scope == scopeNamespace {
		// Configure RBAC in namespace to access the configmaps
		sa, err := r.reconcileServiceAccount(ctx, dispatcherNamespace, rc)
		if err != nil {
			return nil, err
		}

		_, err = r.reconcileRoleBinding(ctx, dispatcherName, dispatcherNamespace, rc, dispatcherName, sa)
		if err != nil {
			return nil, err
		}

		// Reconcile the RoleBinding allowing read access to the shared configmaps.
		// Note this RoleBinding is created in the system namespace and points to a
		// subject in the dispatcher's namespace.
		// TODO: might change when ConfigMapPropagation lands
		roleBindingName := fmt.Sprintf("%s-%s", dispatcherName, dispatcherNamespace)
		_, err = r.reconcileRoleBinding(ctx, roleBindingName, r.systemNamespace, rc, "eventing-config-reader", sa)
		if err != nil {
			return nil, err
		}
	}
	args := resources.DispatcherArgs{
		DispatcherScope:     scope,
		DispatcherNamespace: dispatcherNamespace,
		Image:               r.dispatcherImage,
	}

	expected := resources.MakeDispatcher(args)
	d, err := r.deploymentLister.Deployments(dispatcherNamespace).Get(dispatcherName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			d, err := r.KubeClientSet.AppsV1().Deployments(dispatcherNamespace).Create(expected)
			if err == nil {
				controller.GetEventRecorder(ctx).Event(rc, corev1.EventTypeNormal, dispatcherDeploymentCreated, "Dispatcher deployment created")
				rc.Status.PropagateDispatcherStatus(&d.Status)
				return d, err
			} else {
				rc.Status.MarkDispatcherFailed(dispatcherDeploymentFailed, "Failed to create the dispatcher deployment: %v", err)
				return d, newDeploymentWarn(err)
			}
		}

		logging.FromContext(ctx).Error("Unable to get the dispatcher deployment", zap.Error(err))
		rc.Status.MarkDispatcherUnknown("DispatcherDeploymentFailed", "Failed to get dispatcher deployment: %v", err)
		return nil, err
	} else if !reflect.DeepEqual(expected.Spec.Template.Spec.Containers[0].Image, d.Spec.Template.Spec.Containers[0].Image) {
		logging.FromContext(ctx).Sugar().Infof("Deployment image is not what we expect it to be, updating Deployment Got: %q Expect: %q", expected.Spec.Template.Spec.Containers[0].Image, d.Spec.Template.Spec.Containers[0].Image)
		d, err := r.KubeClientSet.AppsV1().Deployments(dispatcherNamespace).Update(expected)
		if err == nil {
			controller.GetEventRecorder(ctx).Event(rc, corev1.EventTypeNormal, dispatcherDeploymentUpdated, "Dispatcher deployment updated")
			rc.Status.PropagateDispatcherStatus(&d.Status)
			return d, nil
		} else {
			rc.Status.MarkServiceFailed("DispatcherDeploymentUpdateFailed", "Failed to update the dispatcher deployment: %v", err)
		}
		return d, newDeploymentWarn(err)
	}

	rc.Status.PropagateDispatcherStatus(&d.Status)
	return d, nil
}

func (r *Reconciler) reconcileServiceAccount(ctx context.Context, dispatcherNamespace string, rc *v1alpha1.RocketmqChannel) (*corev1.ServiceAccount, error) {
	sa, err := r.serviceAccountLister.ServiceAccounts(dispatcherNamespace).Get(dispatcherName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			expected := resources.MakeServiceAccount(dispatcherNamespace, dispatcherName)
			sa, err := r.KubeClientSet.CoreV1().ServiceAccounts(dispatcherNamespace).Create(expected)
			if err == nil {
				controller.GetEventRecorder(ctx).Event(rc, corev1.EventTypeNormal, dispatcherServiceAccountCreated, "Dispatcher service account created")
				return sa, nil
			} else {
				rc.Status.MarkDispatcherFailed("DispatcherDeploymentFailed", "Failed to create the dispatcher service account: %v", err)
				return sa, newServiceAccountWarn(err)
			}
		}

		rc.Status.MarkDispatcherUnknown("DispatcherServiceAccountFailed", "Failed to get dispatcher service account: %v", err)
		return nil, newServiceAccountWarn(err)
	}
	return sa, err
}

func (r *Reconciler) reconcileRoleBinding(ctx context.Context, name string, ns string, rc *v1alpha1.RocketmqChannel, clusterRoleName string, sa *corev1.ServiceAccount) (*rbacv1.RoleBinding, error) {
	rb, err := r.roleBindingLister.RoleBindings(ns).Get(name)
	if err != nil {
		if apierrs.IsNotFound(err) {
			expected := resources.MakeRoleBinding(ns, name, sa, clusterRoleName)
			rb, err := r.KubeClientSet.RbacV1().RoleBindings(ns).Create(expected)
			if err == nil {
				controller.GetEventRecorder(ctx).Event(rc, corev1.EventTypeNormal, dispatcherRoleBindingCreated, "Dispatcher role binding created")
				return rb, nil
			} else {
				rc.Status.MarkDispatcherFailed("DispatcherDeploymentFailed", "Failed to create the dispatcher role binding: %v", err)
				return rb, newRoleBindingWarn(err)
			}
		}
		rc.Status.MarkDispatcherUnknown("DispatcherRoleBindingFailed", "Failed to get dispatcher role binding: %v", err)
		return nil, newRoleBindingWarn(err)
	}
	return rb, err
}

func (r *Reconciler) reconcileDispatcherService(ctx context.Context, dispatcherNamespace string, rc *v1alpha1.RocketmqChannel) (*corev1.Service, error) {
	svc, err := r.serviceLister.Services(dispatcherNamespace).Get(dispatcherName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			expected := resources.MakeDispatcherService(dispatcherNamespace)
			svc, err := r.KubeClientSet.CoreV1().Services(dispatcherNamespace).Create(expected)

			if err == nil {
				controller.GetEventRecorder(ctx).Event(rc, corev1.EventTypeNormal, dispatcherServiceCreated, "Dispatcher service created")
				rc.Status.MarkServiceTrue()
			} else {
				logging.FromContext(ctx).Error("Unable to create the dispatcher service", zap.Error(err))
				controller.GetEventRecorder(ctx).Eventf(rc, corev1.EventTypeWarning, dispatcherServiceFailed, "Failed to create the dispatcher service: %v", err)
				rc.Status.MarkServiceFailed("DispatcherServiceFailed", "Failed to create the dispatcher service: %v", err)
				return svc, err
			}

			return svc, err
		}

		rc.Status.MarkServiceUnknown("DispatcherServiceFailed", "Failed to get dispatcher service: %v", err)
		return nil, newDispatcherServiceWarn(err)
	}

	rc.Status.MarkServiceTrue()
	return svc, nil
}

func (r *Reconciler) reconcileChannelService(ctx context.Context, dispatcherNamespace string, channel *v1alpha1.RocketmqChannel) (*corev1.Service, error) {
	logger := logging.FromContext(ctx)
	// Get the Service and propagate the status to the Channel in case it does not exist.
	// We don't do anything with the service because it's status contains nothing useful, so just do
	// an existence check. Then below we check the endpoints targeting it.
	// We may change this name later, so we have to ensure we use proper addressable when resolving these.
	expected, err := resources.MakeK8sService(channel, resources.ExternalService(dispatcherNamespace, dispatcherName))
	if err != nil {
		logging.FromContext(ctx).Error("failed to create the channel service object", zap.Error(err))
		channel.Status.MarkChannelServiceFailed("ChannelServiceFailed", fmt.Sprintf("Channel Service failed: %s", err))
		return nil, err
	}

	svc, err := r.serviceLister.Services(channel.Namespace).Get(resources.MakeChannelServiceName(channel.Name))
	if err != nil {
		if apierrs.IsNotFound(err) {
			svc, err = r.KubeClientSet.CoreV1().Services(channel.Namespace).Create(expected)
			if err != nil {
				logging.FromContext(ctx).Error("failed to create the channel service object", zap.Error(err))
				channel.Status.MarkChannelServiceFailed("ChannelServiceFailed", fmt.Sprintf("Channel Service failed: %s", err))
				return nil, err
			}
			return svc, nil
		}
		logger.Error("Unable to get the channel service", zap.Error(err))
		return nil, err
	} else if !equality.Semantic.DeepEqual(svc.Spec, expected.Spec) {
		svc = svc.DeepCopy()
		svc.Spec = expected.Spec

		svc, err = r.KubeClientSet.CoreV1().Services(channel.Namespace).Update(svc)
		if err != nil {
			logging.FromContext(ctx).Error("Failed to update the channel service", zap.Error(err))
			return nil, err
		}
	}
	// Check to make sure that the RocketmqChannel owns this service and if not, complain.
	if !metav1.IsControlledBy(svc, channel) {
		err := fmt.Errorf("rocketmqchannel: %s/%s does not own Service: %q", channel.Namespace, channel.Name, svc.Name)
		channel.Status.MarkChannelServiceFailed("ChannelServiceFailed", fmt.Sprintf("Channel Service failed: %s", err))
		return nil, err
	}
	return svc, nil
}

func (r *Reconciler) createClient(ctx context.Context, rc *v1alpha1.RocketmqChannel) (admin.Admin, error) {
	rocketmqClusterAdmin := r.rocketmqClusterAdmin
	if rocketmqClusterAdmin == nil {
		var err error
		rocketmqClusterAdmin, err = resources.MakeClient(controllerAgentName, r.rocketmqConfig.BrokerAddr)
		if err != nil {
			return nil, err
		}
	}
	return rocketmqClusterAdmin, nil
}

func (r *Reconciler) createTopic(ctx context.Context, channel *v1alpha1.RocketmqChannel, rocketmqClusterAdmin admin.Admin) error {
	logger := logging.FromContext(ctx)

	topicName := utils.TopicName(utils.RocketmqChannelSeparator, channel.Namespace, channel.Name)
	logger.Info("Creating topic on Rocketmq cluster", zap.String("topic", topicName))
	err := rocketmqClusterAdmin.CreateTopic(
		WithTopicCreate(topicName),
		WithBrokerAddrCreate("127.0.0.1:10911"),
	)
	if err != nil {
		logger.Error("Error creating topic", zap.String("topic", topicName), zap.Error(err))
		return err
	}
	return nil

}

func (r *Reconciler) deleteTopic(ctx context.Context, channel *v1alpha1.RocketmqChannel, rocketmqClusterAdmin admin.Admin) error {
	logger := logging.FromContext(ctx)

	topicName := utils.TopicName(utils.RocketmqChannelSeparator, channel.Namespace, channel.Name)
	logger.Info("Deleting topic on Rocketmq Cluster", zap.String("topic", topicName))
	err := rocketmqClusterAdmin.DeleteTopic(WithTopicDelete(topicName))
	if err != nil {
		logger.Error("Error creating topic", zap.String("topic", topicName), zap.Error(err))
		return err
	}
	return nil
	//ErrTopicAlreadyExists have not been implemented yet
}

func (r *Reconciler) updateRocketmqConfig(ctx context.Context, configMap *corev1.ConfigMap) {
	logging.FromContext(ctx).Info("Reloading Rocketmq configuration")
	rocketmqConfig, err := utils.GetRocketmqConfig(configMap.Data)
	if err != nil {
		logging.FromContext(ctx).Error("Error reading Rocketmq configuration", zap.Error(err))
	}
	// For now just override the previous config.
	// Eventually the previous config should be snapshotted to delete Rocketmq topics
	r.rocketmqConfig = rocketmqConfig
	r.rocketmqConfigError = err
}

func (r *Reconciler) FinalizeKind(ctx context.Context, rc *v1alpha1.RocketmqChannel) pkgreconciler.Event {
	// Do not attempt retrying creating the client because it might be a permanent error
	// in which case the finalizer will never get removed.
	if rocketmqClusterAdmin, err := r.createClient(ctx, rc); err == nil && r.rocketmqConfig != nil {
		if err := r.deleteTopic(ctx, rc, rocketmqClusterAdmin); err != nil {
			return err
		}
	}
	return newReconciledNormal(rc.Namespace, rc.Name) //ok to remove finalizer
}
