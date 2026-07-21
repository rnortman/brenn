use crate::db::init_db_memory;
use crate::messaging::db::*;
use crate::test_utils::ensure_user_and_conv;

#[test]
fn budget_decrement_eventually_exhausts() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);

    // Default budget = 3.
    for expected_remaining in [2, 1, 0] {
        match decrement_send_budget(&conn, 1, 3) {
            BudgetDecrement::Ok { remaining } => assert_eq!(remaining, expected_remaining),
            BudgetDecrement::Exhausted => panic!("decrement should succeed"),
        }
    }
    // 4th attempt fails.
    assert!(matches!(
        decrement_send_budget(&conn, 1, 3),
        BudgetDecrement::Exhausted
    ));
}

#[test]
fn budget_reset_overwrites_remaining() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);

    // Initial decrement creates row with default 5; brings remaining to 4.
    let _ = decrement_send_budget(&conn, 1, 5);
    // Reset to 100.
    reset_send_budget(&conn, 1, 100);
    let remaining = read_send_budget(&conn, 1).unwrap();
    assert_eq!(remaining, 100);
}
