from copy import deepcopy


class RunSpecBuilder:
    def __init__(self, run_id: str, name: str):
        self.spec = {
            "run_id": run_id,
            "name": name,
            "namespace": {
                "workspace_root": ".",
                "visible_capabilities": [],
            },
            "budget": {
                "max_steps": 128,
            },
            "capability_leases": [],
            "steps": [],
            "metadata": {},
        }

    def workspace_root(self, workspace_root: str):
        self.spec["namespace"]["workspace_root"] = workspace_root
        return self

    def visible_capability(self, capability_id: str):
        self.spec["namespace"]["visible_capabilities"].append(capability_id)
        return self

    def budget_max_steps(self, max_steps: int):
        self.spec["budget"]["max_steps"] = max_steps
        return self

    def lease(self, capability_id: str, permissions=None):
        self.spec["capability_leases"].append(
            {
                "capability_id": capability_id,
                "permissions": permissions or ["invoke"],
            }
        )
        return self

    def message_step(self, step_id: str, title: str, role: str, content: str):
        self.spec["steps"].append(
            {
                "id": step_id,
                "title": title,
                "action": {
                    "kind": "message",
                    "role": role,
                    "content": content,
                },
            }
        )
        return self

    def capability_step(self, step_id: str, title: str, capability_id: str, input_value: str):
        self.spec["steps"].append(
            {
                "id": step_id,
                "title": title,
                "action": {
                    "kind": "capability_invoke",
                    "capability_id": capability_id,
                    "input": input_value,
                },
            }
        )
        return self

    def delegate_step(self, step_id: str, title: str, child: dict, merge_mode: str):
        self.spec["steps"].append(
            {
                "id": step_id,
                "title": title,
                "action": {
                    "kind": "delegate",
                    "child": child,
                    "merge_mode": merge_mode,
                },
            }
        )
        return self

    def metadata(self, key: str, value: str):
        self.spec["metadata"][key] = value
        return self

    def build(self):
        return deepcopy(self.spec)
