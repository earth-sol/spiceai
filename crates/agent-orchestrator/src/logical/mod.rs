use plan::LogicalPlan;

pub mod plan;
pub mod planner;

pub(crate) fn logical_plan_complete_summary(l: &LogicalPlan) -> String {
    let objectives = l
        .tasks
        .iter()
        .map(|t| format!("- {}\n", t.objective.clone()))
        .collect::<Vec<_>>();

    format!(
        "Finished Logical Plan. {} tasks identified, with the following objectives:\n{}\nCreating physical plan",
        objectives.len(), objectives.join("")
    )
}
