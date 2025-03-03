pub const STRING_ABOVE_MAX_LENGTH: &str = "string_above_max_length";

/// This indicates the model failed to correctly generate its expected structured output.
pub const FAILED_TO_PARSE_STRUCTURED_OUTPUT: &str = "failed_to_parse_structured_output";

pub const RETRY_CODES: &[&str] = &[STRING_ABOVE_MAX_LENGTH, FAILED_TO_PARSE_STRUCTURED_OUTPUT];
