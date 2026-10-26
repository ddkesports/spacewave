package storebench

// SeedConfig shapes the synthetic World Seed writes.
type SeedConfig struct {
	// Objects is the number of objects to create.
	Objects int
	// BodyBytes is the size of each object's body.
	BodyBytes int
	// Links is the number of graph links from each object to earlier ones.
	Links int
	// Batch is the number of objects created per World write.
	Batch int
	// Seed seeds the link targets.
	Seed uint64
}
