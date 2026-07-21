package models

// ProjectVisibility mirrors dev2-company-config's Dev2Project visibility
// flags. Each flag is independent; chat flows require the matching flag to
// bind a session to the project.
type ProjectVisibility struct {
	Discord   bool `json:"discord"`
	Client    bool `json:"client"`
	Developer bool `json:"developer"`
}

// CompanyProject is the public-safe view of a Dev2Project as returned by the
// company.projects.get NATS request-reply (served by dev2-company-config).
// It contains no secrets.
type CompanyProject struct {
	ID                string            `json:"id"`
	CompanyID         string            `json:"companyId"`
	Name              string            `json:"name"`
	Description       string            `json:"description,omitempty"`
	Repos             []string          `json:"repos"`
	ProjectTrackerKey string            `json:"projectTrackerKey,omitempty"`
	Visibility        ProjectVisibility `json:"visibility"`
}
