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
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	duckv1 "knative.dev/pkg/apis/duck/v1"

	"knative.dev/pkg/injection"

	"go.uber.org/zap"

	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"

	eventingduck "knative.dev/eventing/pkg/apis/duck/v1alpha1"
	eventingduckv1beta1 "knative.dev/eventing/pkg/apis/duck/v1beta1"
	messagingv1beta1 "knative.dev/eventing/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing/pkg/kncloudevents"
	"knative.dev/eventing/pkg/logging"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"

	"knative.dev/eventing-contrib/rocektmq/pkg/apis/messaging/v1alpha1"
	clientset "knative.dev/eventing-contrib/rocektmq/pkg/client/clientset/versioned"
	"knative.dev/eventing-contrib/rocektmq/pkg/client/injection/informers/messaging/v1alpha1/rocektmqchannel"
	listers "knative.dev/eventing-contrib/rocektmq/pkg/client/listers/messaging/v1alpha1"
	"knative.dev/eventing-contrib/rocektmq/pkg/dispatcher"
	"knative.dev/eventing-contrib/rocektmq/pkg/util"
)

const (
	// ReconcilerName is the name of the reconciler.
	ReconcilerName = "RocektmqChannels"

	// controllerAgentName is the string used by this controller to identify
	// itself when creating events.
	controllerAgentName = "rocektmq-ch-dispatcher"

	finalizerName = controllerAgentName
)

// Reconciler reconciles RocektMQ Channels.
type Reconciler struct {
	rocektmqDispatcher dispatcher.RocektmqDispatcher

	rocektmqClientSet clientset.Interface

	rocektmqchannelLister listers.RocektmqChannelLister
	impl                  *controller.Impl
}

// Check that our Reconciler implements controller.Reconciler.
var _ controller.Reconciler = (*Reconciler)(nil)

// NewController initializes the controller and is called by the generated code.
// Registers event handlers to enqueue events.
func NewController(ctx context.Context, _ configmap.Watcher) *controller.Impl {

	logger := logging.FromContext(ctx)

	rocektmqConfig := util.GetRocektmqConfig()
	dispatcherArgs := dispatcher.Args{
		RocektmqURL: util.GetDefaultRocektmqURL(),
		ClusterID:   util.GetDefaultClusterID(),
		ClientID:    rocektmqConfig.ClientID,
		Cargs: kncloudevents.ConnectionArgs{
			MaxIdleConns:        rocektmqConfig.MaxIdleConns,
			MaxIdleConnsPerHost: rocektmqConfig.MaxIdleConnsPerHost,
		},
		Logger: logger,
	}
	rocektmqDispatcher, err := dispatcher.NewDispatcher(dispatcherArgs)
	if err != nil {
		logger.Fatal("Unable to create rocektmq dispatcher", zap.Error(err))
	}

	logger = logger.With(zap.String("controller/impl", "pkg"))
	logger.Info("Starting the RocektMQ dispatcher")

	channelInformer := rocektmqchannel.Get(ctx)

	r := &Reconciler{
		rocektmqDispatcher:    rocektmqDispatcher,
		rocektmqchannelLister: channelInformer.Lister(),
		rocektmqClientSet:     clientset.NewForConfigOrDie(injection.GetConfig(ctx)),
	}
	r.impl = controller.NewImpl(r, logger.Sugar(), ReconcilerName)

	logger.Info("Setting up event handlers")

	channelInformer.Informer().AddEventHandler(controller.HandleAll(r.impl.Enqueue))

	logger.Info("Starting dispatcher.")
	go func() {
		if err := rocektmqDispatcher.Start(ctx); err != nil {
			logger.Error("Cannot start dispatcher", zap.Error(err))
		}
	}()
	return r.impl
}

// Reconcile a RocektmqChannel and perform the following actions:
// - update rocektmq subscriptions based on RocektmqChannel subscribers
// - set RocektmqChannel subscriber status based on rocektmq subscriptions
// - maintain mapping between host header and rocektmq channel which is used by the dispatcher
func (r *Reconciler) Reconcile(ctx context.Context, key string) error {
	// Convert the namespace/name string into a distinct namespace and name.
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		logging.FromContext(ctx).Error("invalid resource key")
		return nil
	}

	// Get the RocektmqChannel resource with this namespace/name.
	original, err := r.rocektmqchannelLister.RocektmqChannels(namespace).Get(name)
	if apierrs.IsNotFound(err) {
		// The resource may no longer exist, in which case we stop processing.
		logging.FromContext(ctx).Error("RocektmqChannel key in work queue no longer exists")
		return nil
	} else if err != nil {
		return err
	}

	// Don't modify the informers copy.
	rocektmqChannel := original.DeepCopy()

	// See if the channel has been deleted.
	if rocektmqChannel.DeletionTimestamp != nil {
		c := toChannel(rocektmqChannel)

		if _, err := r.rocektmqDispatcher.UpdateSubscriptions(ctx, c, true); err != nil {
			logging.FromContext(ctx).Error("Error updating subscriptions", zap.Any("channel", c), zap.Error(err))
			return err
		}
		removeFinalizer(rocektmqChannel)
		_, err := r.rocektmqClientSet.MessagingV1alpha1().RocektmqChannels(rocektmqChannel.Namespace).Update(rocektmqChannel)
		return err
	}

	if !rocektmqChannel.Status.IsReady() {
		return fmt.Errorf("Channel is not ready. Cannot configure and update subscriber status")
	}

	reconcileErr := r.reconcile(ctx, rocektmqChannel)
	if reconcileErr != nil {
		logging.FromContext(ctx).Error("Error reconciling RocektmqChannel", zap.Error(reconcileErr))
	} else {
		logging.FromContext(ctx).Debug("RocektmqChannel reconciled")
	}

	// TODO: Should this check for subscribable status rather than entire status?
	if _, updateStatusErr := r.updateStatus(ctx, rocektmqChannel); updateStatusErr != nil {
		logging.FromContext(ctx).Error("Failed to update RocektmqChannel status", zap.Error(updateStatusErr))
		return updateStatusErr
	}

	// Trigger reconciliation in case of errors
	return reconcileErr
}

// reconcile performs the following steps
// - add finalizer
// - update rocektmq subscriptions
// - set RocektmqChannel SubscribableStatus
// - update host2channel map
func (r *Reconciler) reconcile(ctx context.Context, rocektmqChannel *v1alpha1.RocektmqChannel) error {
	// TODO update dispatcher API and use Channelable or RocektmqChannel.
	c := toChannel(rocektmqChannel)

	// If we are adding the finalizer for the first time, then ensure that finalizer is persisted
	// before manipulating Rocektmq.
	if err := r.ensureFinalizer(rocektmqChannel); err != nil {
		return err
	}

	// Try to subscribe.
	failedSubscriptions, err := r.rocektmqDispatcher.UpdateSubscriptions(ctx, c, false)
	if err != nil {
		logging.FromContext(ctx).Error("Error updating subscriptions", zap.Any("channel", c), zap.Error(err))
		return err
	}

	rocektmqChannel.Status.SubscribableStatus = r.createSubscribableStatus(rocektmqChannel.Spec.Subscribable, failedSubscriptions)
	if len(failedSubscriptions) > 0 {
		var b strings.Builder
		for _, subError := range failedSubscriptions {
			b.WriteString("\n")
			b.WriteString(subError.Error())
		}
		errMsg := b.String()
		logging.FromContext(ctx).Error(errMsg)
		return fmt.Errorf(errMsg)
	}

	rocektmqChannels, err := r.rocektmqchannelLister.List(labels.Everything())
	if err != nil {
		logging.FromContext(ctx).Error("Error listing rocektmq channels")
		return err
	}

	channels := make([]messagingv1beta1.Channel, 0)
	for _, nc := range rocektmqChannels {
		if nc.Status.IsReady() {
			channels = append(channels, *toChannel(nc))
		}
	}

	if err := r.rocektmqDispatcher.ProcessChannels(ctx, channels); err != nil {
		logging.FromContext(ctx).Error("Error updating host to channel map", zap.Error(err))
		return err
	}

	return nil
}

// createSubscribableStatus creates the SubscribableStatus based on the failedSubscriptions
// checks for each subscriber on the rocektmq channel if there is a failed subscription on rocektmq side
// if there is no failed subscription => set ready status
func (r *Reconciler) createSubscribableStatus(subscribable *eventingduck.Subscribable, failedSubscriptions map[eventingduck.SubscriberSpec]error) *eventingduck.SubscribableStatus {
	if subscribable == nil {
		return nil
	}
	subscriberStatus := make([]eventingduckv1beta1.SubscriberStatus, 0)
	for _, sub := range subscribable.Subscribers {
		status := eventingduckv1beta1.SubscriberStatus{
			UID:                sub.UID,
			ObservedGeneration: sub.Generation,
			Ready:              corev1.ConditionTrue,
		}

		if err, ok := failedSubscriptions[sub]; ok {
			status.Ready = corev1.ConditionFalse
			status.Message = err.Error()
		}
		subscriberStatus = append(subscriberStatus, status)
	}
	return &eventingduck.SubscribableStatus{
		Subscribers: subscriberStatus,
	}
}

func (r *Reconciler) updateStatus(ctx context.Context, desired *v1alpha1.RocektmqChannel) (*v1alpha1.RocektmqChannel, error) {
	nc, err := r.rocektmqchannelLister.RocektmqChannels(desired.Namespace).Get(desired.Name)
	if err != nil {
		return nil, err
	}

	if reflect.DeepEqual(nc.Status, desired.Status) {
		return nc, nil
	}

	// Don't modify the informers copy.
	existing := nc.DeepCopy()
	existing.Status = desired.Status

	return r.rocektmqClientSet.MessagingV1alpha1().RocektmqChannels(desired.Namespace).UpdateStatus(existing)
}

func (r *Reconciler) ensureFinalizer(channel *v1alpha1.RocektmqChannel) error {
	finalizers := sets.NewString(channel.Finalizers...)
	if finalizers.Has(finalizerName) {
		return nil
	}

	mergePatch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"finalizers":      append(channel.Finalizers, finalizerName),
			"resourceVersion": channel.ResourceVersion,
		},
	}

	patch, err := json.Marshal(mergePatch)
	if err != nil {
		return err
	}

	_, err = r.rocektmqClientSet.MessagingV1alpha1().RocektmqChannels(channel.Namespace).Patch(channel.Name, types.MergePatchType, patch)
	return err
}

func removeFinalizer(channel *v1alpha1.RocektmqChannel) {
	finalizers := sets.NewString(channel.Finalizers...)
	finalizers.Delete(finalizerName)
	channel.Finalizers = finalizers.List()
}

func toChannel(rocektmqChannel *v1alpha1.RocektmqChannel) *messagingv1beta1.Channel {
	channel := &messagingv1beta1.Channel{
		ObjectMeta: v1.ObjectMeta{
			Name:      rocektmqChannel.Name,
			Namespace: rocektmqChannel.Namespace,
		},
		Spec: messagingv1beta1.ChannelSpec{
			ChannelTemplate: nil,
			ChannelableSpec: eventingduckv1beta1.ChannelableSpec{
				SubscribableSpec: eventingduckv1beta1.SubscribableSpec{},
			},
		},
	}

	if rocektmqChannel.Status.Address != nil {
		channel.Status = messagingv1beta1.ChannelStatus{
			ChannelableStatus: eventingduckv1beta1.ChannelableStatus{
				AddressStatus: duckv1.AddressStatus{
					Address: &duckv1.Addressable{
						URL: rocektmqChannel.Status.Address.URL,
					}},
				SubscribableStatus: eventingduckv1beta1.SubscribableStatus{},
				DeadLetterChannel:  nil,
			},
			Channel: nil,
		}
	}
	if rocektmqChannel.Spec.Subscribable != nil {
		for _, s := range rocektmqChannel.Spec.Subscribable.Subscribers {
			sbeta1 := eventingduckv1beta1.SubscriberSpec{
				UID:           s.UID,
				Generation:    s.Generation,
				SubscriberURI: s.SubscriberURI,
				ReplyURI:      s.ReplyURI,
				Delivery:      s.Delivery,
			}
			channel.Spec.Subscribers = append(channel.Spec.Subscribers, sbeta1)
		}
	}

	return channel
}
