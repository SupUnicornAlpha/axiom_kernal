export type CapabilityLease = {
  capability_id: string
  permissions: string[]
}

export type StepAction =
  | {
      kind: "message"
      role: string
      content: string
    }
  | {
      kind: "capability_invoke"
      capability_id: string
      input: string
    }
  | {
      kind: "delegate"
      child: ChildRunSpec
      merge_mode: "summary_only" | "append_messages"
    }

export type ChildRunSpec = {
  parent_run_id: string
  run: RunSpec
  memory_view: string[]
  sandbox_profile: string
  return_contract: string
}

export type Step = {
  id: string
  title: string
  action: StepAction
}

export type RunSpec = {
  schema_version: 1
  run_id: string
  name: string
  namespace: {
    workspace_root: string
    visible_capabilities: string[]
  }
  budget: {
    max_steps: number
  }
  capability_leases: CapabilityLease[]
  steps: Step[]
  metadata: Record<string, string>
}

export class RunSpecBuilder {
  private spec: RunSpec

  constructor(runId: string, name: string) {
    this.spec = {
      schema_version: 1,
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

  workspaceRoot(workspaceRoot: string): this {
    this.spec.namespace.workspace_root = workspaceRoot
    return this
  }

  visibleCapability(capabilityId: string): this {
    this.spec.namespace.visible_capabilities.push(capabilityId)
    return this
  }

  budgetMaxSteps(maxSteps: number): this {
    this.spec.budget.max_steps = maxSteps
    return this
  }

  lease(capabilityId: string, permissions: string[] = ["invoke"]): this {
    this.spec.capability_leases.push({
      capability_id: capabilityId,
      permissions,
    })
    return this
  }

  messageStep(id: string, title: string, role: string, content: string): this {
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

  capabilityStep(id: string, title: string, capabilityId: string, input: string): this {
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

  delegateStep(id: string, title: string, child: RunSpec, mergeMode: "summary_only" | "append_messages"): this {
    this.spec.steps.push({
      id,
      title,
      action: {
        kind: "delegate",
        child: {
          parent_run_id: this.spec.run_id,
          run: child,
          memory_view: [],
          sandbox_profile: "default-deny",
          return_contract: "effect-proposals-v1",
        },
        merge_mode: mergeMode,
      },
    })
    return this
  }

  metadata(key: string, value: string): this {
    this.spec.metadata[key] = value
    return this
  }

  build(): RunSpec {
    return structuredClone(this.spec)
  }
}
