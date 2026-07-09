export class RunSpecBuilder {
  constructor(runId, name) {
    this.spec = {
      run_id: runId,
      name,
      namespace: {
        workspace_root: ".",
        visible_capabilities: [],
      },
      budget: {
        max_steps: 128,
      },
      capability_leases: [],
      steps: [],
      metadata: {},
    }
  }

  workspaceRoot(workspaceRoot) {
    this.spec.namespace.workspace_root = workspaceRoot
    return this
  }

  visibleCapability(capabilityId) {
    this.spec.namespace.visible_capabilities.push(capabilityId)
    return this
  }

  budgetMaxSteps(maxSteps) {
    this.spec.budget.max_steps = maxSteps
    return this
  }

  lease(capabilityId, permissions = ["invoke"]) {
    this.spec.capability_leases.push({
      capability_id: capabilityId,
      permissions,
    })
    return this
  }

  messageStep(id, title, role, content) {
    this.spec.steps.push({
      id,
      title,
      action: {
        kind: "message",
        role,
        content,
      },
    })
    return this
  }

  capabilityStep(id, title, capabilityId, input) {
    this.spec.steps.push({
      id,
      title,
      action: {
        kind: "capability_invoke",
        capability_id: capabilityId,
        input,
      },
    })
    return this
  }

  delegateStep(id, title, child, mergeMode) {
    this.spec.steps.push({
      id,
      title,
      action: {
        kind: "delegate",
        child,
        merge_mode: mergeMode,
      },
    })
    return this
  }

  metadata(key, value) {
    this.spec.metadata[key] = value
    return this
  }

  build() {
    return JSON.parse(JSON.stringify(this.spec))
  }
}
