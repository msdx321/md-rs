//! JSON overlays used by provider configuration endpoints.
use serde_json::Value;

/// Objects merge recursively; arrays and scalars replace. Explicit `null`
/// stays in the result so deserialization can clear optional fields.
pub fn merge_json(base: &mut Value, patch: Value) {
    match (base, patch) {
        (Value::Object(base), Value::Object(patch)) => {
            for (key, value) in patch {
                merge_json(base.entry(key).or_insert(Value::Null), value);
            }
        }
        (slot, value) => *slot = value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn merge_json_replaces_arrays_and_cross_type_values() {
        let mut base = json!({"links": [1, 2], "object": {"old": true}, "scalar": 4});
        merge_json(
            &mut base,
            json!({"links": [3], "object": false, "scalar": {"new": null}}),
        );
        assert_eq!(
            base,
            json!({"links": [3], "object": false, "scalar": {"new": null}})
        );
        merge_json(&mut base, Value::Null);
        assert_eq!(base, Value::Null);
        merge_json(&mut base, json!({"nested": {"new": 1}}));
        assert_eq!(base, json!({"nested": {"new": 1}}));
    }
}
