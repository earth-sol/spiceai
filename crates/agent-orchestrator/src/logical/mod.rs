use plan::LogicalPlan;

pub mod plan;

pub(crate) fn logical_plan_complete_summary(l: &LogicalPlan) -> String {
    let objectives = l
        .tasks
        .iter()
        .map(|t| format!("- {}\n", t.objective.clone()))
        .collect::<Vec<_>>();

    format!(
        "\nFinished Logical Plan. {} tasks identified, with the following objectives:\n{}\nCreating physical plan",
        objectives.len(), objectives.join("")
    )
}
