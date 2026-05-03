package selfupdate

import (
	"context"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Updater patches the agent's own Deployment to point at the latest image
// tag the server reports back. RBAC is intentionally narrow: the chart grants
// us update on Deployments only with resourceNames=<our-deployment>, so even
// if the agent were compromised it cannot modify any other workload.
type Updater struct {
	cs           kubernetes.Interface
	namespace    string
	deployName   string
	currentImage string
}

func New(namespace, deployName string) (*Updater, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Updater{cs: cs, namespace: namespace, deployName: deployName}, nil
}

// Apply re-points the agent's Deployment to the requested version if it
// differs from what is currently running. The Deployment controller will
// roll out the new image. Returns true if a patch was applied.
func (u *Updater) Apply(ctx context.Context, latestVersion, currentVersion string) (bool, error) {
	if latestVersion == "" || latestVersion == currentVersion {
		return false, nil
	}
	dep, err := u.cs.AppsV1().Deployments(u.namespace).Get(ctx, u.deployName, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("read deployment: %w", err)
	}
	if len(dep.Spec.Template.Spec.Containers) == 0 {
		return false, fmt.Errorf("deployment has no containers")
	}
	img := dep.Spec.Template.Spec.Containers[0].Image
	repo, _, ok := strings.Cut(img, ":")
	if !ok {
		return false, fmt.Errorf("image %q has no tag", img)
	}
	newImg := fmt.Sprintf("%s:%s", repo, latestVersion)
	if newImg == img {
		return false, nil
	}
	dep.Spec.Template.Spec.Containers[0].Image = newImg
	if dep.Spec.Template.Annotations == nil {
		dep.Spec.Template.Annotations = make(map[string]string, 1)
	}
	dep.Spec.Template.Annotations["pingreports.io/last-self-update"] = time.Now().UTC().Format(time.RFC3339)
	_, err = u.cs.AppsV1().Deployments(u.namespace).Update(ctx, dep, metav1.UpdateOptions{})
	if err != nil {
		return false, fmt.Errorf("patch deployment: %w", err)
	}
	return true, nil
}
