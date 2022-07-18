/*
Copyright 2021.

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

package controllers

import (
	"context"
	"fmt"
	"os"
	"time"

	dbaasoperator "github.com/RHEcosystemAppEng/dbaas-operator/api/v1alpha1"

	v1 "k8s.io/api/apps/v1"
	rbac "k8s.io/api/rbac/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	label "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// DBaaSProviderReconciler reconciles a DBaaSProvider object
type DBaaSProviderReconciler struct {
	*runtime.Scheme
	Client    client.Client
	Clientset kubernetes.Interface //TODO

	operatorInstallNamespace string
	operatorNameVersion      string
}

const (
	providerResourceName = "mydb-cloud-registration"
	dbaasProviderKind    = "DBaaSProvider"

	provisionDocUrl      = "https://www.cockroachlabs.com/docs/cockroachcloud/quickstart.html"
	provisionDescription = "Follow the guide to start a free CockroachDB Serverless (beta) cluster"

	secretKeyDisplayName = "API Secret Key"
)

//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;create;update;delete;watch
//+kubebuilder:rbac:groups=dbaas.redhat.com,resources=dbaasproviders,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=dbaas.redhat.com,resources=dbaasproviders/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=dbaas.redhat.com,resources=dbaasproviders/finalizers,verbs=update

func (r *DBaaSProviderReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx, "DBaaSProvider", req.NamespacedName)

	//check if operator itself is deployed
	dep := &v1.Deployment{}
	if err := r.Client.Get(ctx, req.NamespacedName, dep); err != nil {
		if apiErrors.IsNotFound(err) {
			// CR deleted since request queued, child objects getting GC'd, no requeue
			logger.Info("deployment not found, could be deleted, no requeue")
			return ctrl.Result{}, nil
		}
		// error fetching deployment, requeue and try again
		logger.Error(err, "error fetching Deployment CR")
		return ctrl.Result{}, err
	}

	isCrdInstalled, err := r.checkCrdInstalled(dbaasoperator.GroupVersion.String(), dbaasProviderKind)
	if err != nil {
		logger.Error(err, "error while discovering DBaaS GVK")
		return ctrl.Result{}, err
	}
	if !isCrdInstalled {
		logger.Info("DBaaS CRD not found, requeuing with rate limiter")
		// returning with 'Requeue: true' will invoke our custom rate limiter seen in SetupWithManager below
		return ctrl.Result{Requeue: true}, nil
	}

	instance := &dbaasoperator.DBaaSProvider{
		ObjectMeta: metav1.ObjectMeta{
			Name: providerResourceName,
		},
	}

	if err := r.Client.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
		if apiErrors.IsNotFound(err) {
			// CR deleted since request queued, child objects getting GC'd, no requeue
			logger.Info("resource not found, creating now")

			// CockroachDB Cloud registration custom resource isn't present,so create now with ClusterRole owner for GC
			opts := &client.ListOptions{
				LabelSelector: label.SelectorFromSet(map[string]string{
					"olm.owner":      r.operatorNameVersion,
					"olm.owner.kind": "ClusterServiceVersion",
				}),
			}
			clusterRoleList := &rbac.ClusterRoleList{}
			if err := r.Client.List(context.Background(), clusterRoleList, opts); err != nil {
				logger.Error(err, "unable to list ClusterRoles to seek potential operand owners")
				return ctrl.Result{}, err
			}

			if len(clusterRoleList.Items) < 1 {
				err := apiErrors.NewNotFound(
					schema.GroupResource{Group: "rbac.authorization.k8s.io", Resource: "ClusterRole"}, "potentialOwner")
				logger.Error(err, "could not find ClusterRole owned by CSV to inherit operand")
				return ctrl.Result{}, err
			}
			instance = buildProviderCR(clusterRoleList)
			if err := r.Client.Create(ctx, instance); err != nil {
				logger.Error(err, "error while creating new cluster-scoped resource")
				return ctrl.Result{}, err
			} else {
				logger.Info("cluster-scoped resource created")
				return ctrl.Result{}, nil
			}
		}
		// error fetching the resource, requeue and try again
		logger.Error(err, "error fetching the resource")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func buildProviderCR(clusterRoleList *rbac.ClusterRoleList) *dbaasoperator.DBaaSProvider {
	instance := &dbaasoperator.DBaaSProvider{
		ObjectMeta: metav1.ObjectMeta{
			Name: providerResourceName,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         "rbac.authorization.k8s.io/v1",
					Kind:               "ClusterRole",
					UID:                clusterRoleList.Items[0].GetUID(),
					Name:               clusterRoleList.Items[0].Name,
					Controller:         pointer.BoolPtr(true),
					BlockOwnerDeletion: pointer.BoolPtr(false),
				},
			},
			Labels: map[string]string{"related-to": "dbaas-operator", "type": "dbaas-provider-registration"},
		},
		Spec: dbaasoperator.DBaaSProviderSpec{
			Provider: dbaasoperator.DatabaseProvider{
				Name:               "MyDB",
				DisplayName:        "MyDB Cloud",
				DisplayDescription: "A distributed SQL database as example",
				Icon: dbaasoperator.ProviderIcon{
					Data:      "iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAA/xJREFUeNq0V9uLE1cY/85k08wk2TWyf8AGH30oI8kigrLxgsULbqC1PrR0Z3rRQh928yD2ySUKgvhg9lFFZ1ZhZR+kkdaCgiUrBSm7YYc+9KmF2AfBBzF46V6SOcfvTM7kvu5kM/1gcgIz53y/7/fdzkegSQK77y4roKhhJkMEFAgzfPgKcjnCFAv/LyhMzt8pRizoIkezq2o1wtKVQTpWGWRqJcpiuML6IAW+8qcaZda/Q8O73D0DrUeQEv6o7QcTgBguKf4QINNnkrZFw2TmxhPJ5O9PnlnXUNlkhTIVNhMGpbazGxLcPR+v/HGq1L5n/+jfqsKQGZDHkZW0QmSwIwCotFDZRvmaciyMOk8eGbiPq/X7l9EOpkbKL+PPYsOlrgC8yNeJtVhQCk7ZYZh2aUWFgNRnqwrL/XxJLvdyntQrgFvFEFcw0uXVSK/KtwRgIrFioB81hz4KGSAs47hWAu3Y+VXjfwXwefLVFANWU26Dbv4ayM3NBXOoXBcO1T65vDLVy5meY+B48rkqQ2iZp6YMH2WuFwdyze/Hz65OVcNw1Uk1he367Yew5SsDFJhRyyJWaFfO5f4VmTNRcL4JMMNXFxxI/oO013KcEjqzYYpLbEbwqu67/VbzDQAlbLJRR1j5Aw61GmBg0hcAe0b/Ul3rPUi86WR1z703at8A0OJ0S79ggQ0tIzaZbHUJpPsGgME3VgMCZo1lkj6dqHb4d/zsmoZ1wVVoCgBjfjDAmxDYYGclIE70EyIZ3+2ldRCffb/OlRuiPuToAGRFTKQ2O3/Aa7o8WhrhDSSDlZB3Ro0FwNCO2LA2jH2AirRjYD48pziVce/c2/7T8OPRYkrQX+9es0VFR8tqFCMI9LshyrL54IKsN5ir7Uk8ep3qOw3RDS0t+tqipDenHCov/HQ1pLelZMmPNCyLwOtIJxpg2QYA0lGcCKtfbMpbBvDnYsJquhG1tuXHgbz7f/5WMN9lu7OneHjI6tcFjgWHk8+8FiMYu/nOk/WeAGDqFYQbUl4BYNCkRBEp9A0AFS8IIBOeezyFCREHC34AyLuBmE6+iG/2/aGZ/+JuACKQfN8Ani7uLKFys/axNL0pYBumxWo+/XSw1DcA5yMmzQoA2hfJNxvGwpGLKylMSU3QP+vbfeDx0o5CnQUmGd8mKh1peeLcaqxeFdH6J19FC74BENbzGl9GIHEJm1GXVmyg1XHMgDKykPH9UsrlVPKVGmKhZT4ZUQXM9W1Mc2a/IWrizMfHM+dC+vBHxfJuWA8yv7TdQiZ0dw5oKrvunKD3onxLg4lZlE1CiN5S5TjtOCf8clE2ez2v59nQlW8O2mo1zIwqDqm46tgPrK2c816AAQCBW4SEJD8W2QAAAABJRU5ErkJggg==",
					MediaType: "image/png",
				},
			},
			InventoryKind:  "MydbDBaaSInventory",
			ConnectionKind: "MydbDBaaSConnection",
			InstanceKind:   "MydbDBaaSInstance",
			CredentialFields: []dbaasoperator.CredentialField{
				{
					Key:         "apikey",
					DisplayName: secretKeyDisplayName,
					Type:        "maskedstring",
					Required:    true,
				},
			},
			AllowsFreeTrial:              true,
			ExternalProvisionURL:         provisionDocUrl,
			ExternalProvisionDescription: provisionDescription,
			InstanceParameterSpecs:       []dbaasoperator.InstanceParameterSpec{},
		},
	}

	return instance
}

// CheckCrdInstalled checks whether dbaas provider CRD, has been created yet
func (r *DBaaSProviderReconciler) checkCrdInstalled(groupVersion, kind string) (bool, error) {
	resources, err := r.Clientset.Discovery().ServerResourcesForGroupVersion(groupVersion)
	if err != nil {
		if apiErrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check DBaaSProvider CRD:%w", err)
	}
	for _, r := range resources.APIResources {
		if r.Kind == kind {
			return true, nil
		}
	}
	return false, nil
}

// ignoreOtherDeployments  only on a 'create' event is issued for the deployment
func (r *DBaaSProviderReconciler) ignoreOtherDeployments() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return r.evaluatePredicateObject(e.Object)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}
}

func (r *DBaaSProviderReconciler) evaluatePredicateObject(obj client.Object) bool {
	lbls := obj.GetLabels()
	if obj.GetNamespace() == r.operatorInstallNamespace {
		if val, keyFound := lbls["olm.owner.kind"]; keyFound {
			if val == "ClusterServiceVersion" {
				if val, keyFound := lbls["olm.owner"]; keyFound {
					return val == r.operatorNameVersion
				}
			}
		}
	}
	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *DBaaSProviderReconciler) SetupWithManager(mgr ctrl.Manager) error {
	logger := log.FromContext(context.Background())

	// envVar set in controller-manager's Deployment YAML
	if operatorInstallNamespace, found := os.LookupEnv("INSTALL_NAMESPACE"); !found {
		err := fmt.Errorf("INSTALL_NAMESPACE must be set")
		logger.Error(err, "error fetching envVar")
		return err
	} else {
		r.operatorInstallNamespace = operatorInstallNamespace
	}
	// envVar set for all operators
	if operatorNameEnvVar, found := os.LookupEnv("OPERATOR_CONDITION_NAME"); !found {
		err := fmt.Errorf("OPERATOR_CONDITION_NAME must be set")
		logger.Error(err, "error fetching envVar")
		return err
	} else {
		r.operatorNameVersion = operatorNameEnvVar
	}

	customRateLimiter := workqueue.NewItemExponentialFailureRateLimiter(30*time.Second, 30*time.Minute)

	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{RateLimiter: customRateLimiter}).
		For(&v1.Deployment{}).
		WithEventFilter(r.ignoreOtherDeployments()).
		Complete(r)
}
