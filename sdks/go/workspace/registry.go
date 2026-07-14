package workspace

import "fmt"

type Handler func(root, input string) (string, error)
type Registry struct{ handlers map[string]Handler }

func NewRegistry() *Registry                              { return &Registry{handlers: map[string]Handler{}} }
func (r *Registry) Register(name string, handler Handler) { r.handlers[name] = handler }
func (r *Registry) Invoke(name, root, permission, input string) (string, error) {
	if permission != "invoke" {
		return "", fmt.Errorf("permission denied")
	}
	handler, ok := r.handlers[name]
	if !ok {
		return "", fmt.Errorf("tool not found: %s", name)
	}
	return handler(root, input)
}
