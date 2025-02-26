use plan::LogicalPlan;

pub mod plan;

pub(crate) fn logical_plan_complete_summary(l: &LogicalPlan) -> String {
    let objectives = l
        .tasks
        .iter()
        .map(|t| format!("- {}\n", t.objective.clone()))
        .collect::<Vec<_>>();

    format!(
        "Logical Plan created. {} tasks identified, with the following objectives:\n{}",
        objectives.len(),
        objectives.join("\n")
    )
}
