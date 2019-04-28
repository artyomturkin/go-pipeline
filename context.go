package pipeline

// ContextKey type for working with context
type ContextKey string

// IDKey context key to get and set pipeline run ID
const IDKey ContextKey = "ID"

// NameKey context key to get and set pipeline name
const NameKey ContextKey = "NAME"
