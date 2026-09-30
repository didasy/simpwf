package service

import (
	"fmt"

	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

// authorizeInstance is the object-level half of the authorization check. The
// per-route RequirePermission gate answers "may this caller perform this
// action at all"; it never answers "may this caller touch *this* instance".
// Without the second half any holder of instances:read or
// instances:control reaches every instance in the deployment by id, and the
// id space is discoverable through the unscoped list endpoint.
//
// The binding is the instance's created_by against the caller's resolved
// users.id. The service principal (API token, or a broker/internal caller
// that carries no credential) bypasses the object check exactly as it
// bypasses the endpoint gate: it is the operator's own machine identity,
// not a row owner, so matching it against created_by would lock every
// human-created row away the moment authentication is switched on. An empty
// actor means authentication is disabled, where the deployment has no
// per-user identities to bind to and the pre-authentication behavior is
// preserved.
//
// An anonymous delivery reaches this check as the service principal with no
// instance ownership of its own, so it passes here the same way an
// unauthenticated deployment does. The public-node gate in authorizeDelivery
// is what actually decides: a private node still refuses the delivery, so
// the instance id is not an oracle for anything an anonymous caller cannot
// already deliver to.
//
// The refusal is deliberately indistinguishable from a miss (ErrNotFound):
// answering 403 would confirm the instance exists to a caller that is not
// entitled to know it, turning the endpoint into an id oracle. That is also
// why an input delivery is authorized at the endpoint before it reaches
// here: a caller who holds no input:deliver permission is refused by the
// route on its own terms, rather than being told the instance is gone.
func authorizeInstance(inst *model.WorkflowInstance, p auth.Principal) error {
	if inst == nil {
		return fmt.Errorf("%w: instance not found", model.ErrNotFound)
	}
	if p.Service {
		return nil
	}
	if p.UserID == "" || inst.CreatedBy == "" {
		// Authentication disabled, or a row predating per-request actors.
		// There is no ownership claim to enforce, so the endpoint gate stands
		// alone rather than locking every legacy row away.
		return nil
	}
	if inst.CreatedBy == p.UserID {
		return nil
	}
	return fmt.Errorf("%w: instance %s", model.ErrNotFound, inst.ID)
}

// ownedInstances constrains an instance listing to the caller's own rows.
// The service principal keeps the unscoped listing: it is the operator's
// own machine identity, not a row owner. A caller with no user id
// (authentication disabled) keeps the unscoped listing the deployment had
// before per-request principals existed.
func ownedInstances(q repository.InstanceListQuery, p auth.Principal) repository.InstanceListQuery {
	if p.Service || p.UserID == "" {
		return q
	}
	q.CreatedBy = p.UserID
	return q
}
