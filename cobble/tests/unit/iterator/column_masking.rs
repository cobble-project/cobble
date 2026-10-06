use super::selects_all_columns;

#[test]
fn test_full_projection_requires_ordered_complete_columns() {
    assert!(selects_all_columns(&[], 0));
    assert!(selects_all_columns(&[0], 1));
    assert!(selects_all_columns(&[0, 1, 2], 3));
    assert!(!selects_all_columns(&[1, 0], 2));
    assert!(!selects_all_columns(&[0, 0], 2));
    assert!(!selects_all_columns(&[0, 1], 3));
    assert!(!selects_all_columns(&[0, 1, 2], 2));
    assert!(!selects_all_columns(&[], 1));
}
