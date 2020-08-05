/*
Copyright 2019 The Knative Authors.

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

package v1alpha1

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/apis/duck/v1alpha1"
)

var rc = apis.NewLivingConditionSet(
	RocketmqChannelConditionTopicReady,
	RocketmqChannelConditionDispatcherReady,
	RocketmqChannelConditionServiceReady,
	RocketmqChannelConditionEndpointsReady,
	RocketmqChannelConditionAddressable,
	RocketmqChannelConditionChannelServiceReady,
	RocketmqChannelConditionConfigReady)

const (
	// RocketmqChannelConditionReady has status True when all subconditions below have been set to True.
	RocketmqChannelConditionReady = apis.ConditionReady

	// RocketmqChannelConditionDispatcherReady has status True when a Dispatcher deployment is ready
	// Keyed off appsv1.DeploymentAvailable, which means minimum available replicas required are up
	// and running for at least minReadySeconds.
	RocketmqChannelConditionDispatcherReady apis.ConditionType = "DispatcherReady"

	// RocketmqChannelConditionServiceReady has status True when a k8s Service is ready. This
	// basically just means it exists because there's no meaningful status in Service. See Endpoints
	// below.
	RocketmqChannelConditionServiceReady apis.ConditionType = "ServiceReady"

	// RocketmqChannelConditionEndpointsReady has status True when a k8s Service Endpoints are backed
	// by at least one endpoint.
	RocketmqChannelConditionEndpointsReady apis.ConditionType = "EndpointsReady"

	// RocketmqChannelConditionAddressable has status true when this RocketmqChannel meets
	// the Addressable contract and has a non-empty hostname.
	RocketmqChannelConditionAddressable apis.ConditionType = "Addressable"

	// RocketmqChannelConditionChannelServiceReady has status True when a k8s Service representing the channel is ready.
	// Because this uses ExternalName, there are no endpoints to check.
	RocketmqChannelConditionChannelServiceReady apis.ConditionType = "ChannelServiceReady"

	// RocketmqChannelConditionTopicReady has status True when the Rocketmq topic to use by the channel exists.
	RocketmqChannelConditionTopicReady apis.ConditionType = "TopicReady"

	// RocketmqChannelConditionConfigReady has status True when the Rocketmq configuration to use by the channel exists and is valid
	// (ie. the connection has been established).
	RocketmqChannelConditionConfigReady apis.ConditionType = "ConfigurationReady"
)

// GetConditionSet retrieves the condition set for this resource. Implements the KRShaped interface.
func (*RocketmqChannel) GetConditionSet() apis.ConditionSet {
	return rc
}

// GetCondition returns the condition currently associated with the given type, or nil.
func (cs *RocketmqChannelStatus) GetCondition(t apis.ConditionType) *apis.Condition {
	return rc.Manage(cs).GetCondition(t)
}

// IsReady returns true if the resource is ready overall.
func (cs *RocketmqChannelStatus) IsReady() bool {
	return rc.Manage(cs).IsHappy()
}

// InitializeConditions sets relevant unset conditions to Unknown state.
func (cs *RocketmqChannelStatus) InitializeConditions() {
	rc.Manage(cs).InitializeConditions()
}

// SetAddress sets the address (as part of Addressable contract) and marks the correct condition.
func (cs *RocketmqChannelStatus) SetAddress(url *apis.URL) {
	if cs.Address == nil {
		cs.Address = &v1alpha1.Addressable{}
	}
	if url != nil {
		cs.Address.Hostname = url.Host
		cs.Address.URL = url
		rc.Manage(cs).MarkTrue(RocketmqChannelConditionAddressable)
	} else {
		cs.Address.Hostname = ""
		cs.Address.URL = nil
		rc.Manage(cs).MarkFalse(RocketmqChannelConditionAddressable, "EmptyHostname", "hostname is the empty string")
	}
}

func (cs *RocketmqChannelStatus) MarkDispatcherFailed(reason, messageFormat string, messageA ...interface{}) {
	rc.Manage(cs).MarkFalse(RocketmqChannelConditionDispatcherReady, reason, messageFormat, messageA...)
}

func (cs *RocketmqChannelStatus) MarkDispatcherUnknown(reason, messageFormat string, messageA ...interface{}) {
	rc.Manage(cs).MarkUnknown(RocketmqChannelConditionDispatcherReady, reason, messageFormat, messageA...)
}

// TODO: Unify this with the ones from Eventing. Say: Broker, Trigger.
func (cs *RocketmqChannelStatus) PropagateDispatcherStatus(ds *appsv1.DeploymentStatus) {
	for _, cond := range ds.Conditions {
		if cond.Type == appsv1.DeploymentAvailable {
			if cond.Status == corev1.ConditionTrue {
				rc.Manage(cs).MarkTrue(RocketmqChannelConditionDispatcherReady)
			} else if cond.Status == corev1.ConditionFalse {
				cs.MarkDispatcherFailed("DispatcherDeploymentFalse", "The status of Dispatcher Deployment is False: %s : %s", cond.Reason, cond.Message)
			} else if cond.Status == corev1.ConditionUnknown {
				cs.MarkDispatcherUnknown("DispatcherDeploymentUnknown", "The status of Dispatcher Deployment is Unknown: %s : %s", cond.Reason, cond.Message)
			}
		}
	}
}

func (cs *RocketmqChannelStatus) MarkServiceFailed(reason, messageFormat string, messageA ...interface{}) {
	rc.Manage(cs).MarkFalse(RocketmqChannelConditionServiceReady, reason, messageFormat, messageA...)
}

func (cs *RocketmqChannelStatus) MarkServiceUnknown(reason, messageFormat string, messageA ...interface{}) {
	rc.Manage(cs).MarkUnknown(RocketmqChannelConditionServiceReady, reason, messageFormat, messageA...)
}

func (cs *RocketmqChannelStatus) MarkServiceTrue() {
	rc.Manage(cs).MarkTrue(RocketmqChannelConditionServiceReady)
}

func (cs *RocketmqChannelStatus) MarrchannelServiceFailed(reason, messageFormat string, messageA ...interface{}) {
	rc.Manage(cs).MarkFalse(RocketmqChannelConditionChannelServiceReady, reason, messageFormat, messageA...)
}

func (cs *RocketmqChannelStatus) MarrchannelServiceTrue() {
	rc.Manage(cs).MarkTrue(RocketmqChannelConditionChannelServiceReady)
}

func (cs *RocketmqChannelStatus) MarkEndpointsFailed(reason, messageFormat string, messageA ...interface{}) {
	rc.Manage(cs).MarkFalse(RocketmqChannelConditionEndpointsReady, reason, messageFormat, messageA...)
}

func (cs *RocketmqChannelStatus) MarkEndpointsTrue() {
	rc.Manage(cs).MarkTrue(RocketmqChannelConditionEndpointsReady)
}

func (cs *RocketmqChannelStatus) MarkTopicTrue() {
	rc.Manage(cs).MarkTrue(RocketmqChannelConditionTopicReady)
}

func (cs *RocketmqChannelStatus) MarkTopicFailed(reason, messageFormat string, messageA ...interface{}) {
	rc.Manage(cs).MarkFalse(RocketmqChannelConditionTopicReady, reason, messageFormat, messageA...)
}

func (cs *RocketmqChannelStatus) MarrconfigTrue() {
	rc.Manage(cs).MarkTrue(RocketmqChannelConditionConfigReady)
}

func (cs *RocketmqChannelStatus) MarrconfigFailed(reason, messageFormat string, messageA ...interface{}) {
	rc.Manage(cs).MarkFalse(RocketmqChannelConditionConfigReady, reason, messageFormat, messageA...)
}
