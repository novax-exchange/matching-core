//! Governance Control Boundary component.
//!
//! Architecture status: this component is identified in the Matching Service
//! architecture, but governed halt / resume, symbol configuration, market mode,
//! price-band, reduce-only, and fencing control events are not implemented yet.
//!
//! TODO: consume versioned control facts through the Journal path and expose
//! deterministic local control state to the symbol runtime.
