use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.as_slice() {
        [] => {
            print_help();
            ExitCode::SUCCESS
        }
        [flag] if flag == "-h" || flag == "--help" => {
            print_help();
            ExitCode::SUCCESS
        }
        [cmd, target] if cmd == "schema" && target == "app" => {
            println!("{}", events::application_schema::APP_SCHEMA_SQL);
            ExitCode::SUCCESS
        }
        [cmd, target, option, table]
            if cmd == "schema" && target == "app" && option == "--table" =>
        {
            print_app_table(table)
        }
        _ => {
            eprintln!("unknown command\n");
            print_help();
            ExitCode::from(2)
        }
    }
}

fn print_app_table(table: &str) -> ExitCode {
    match table {
        "last-processed-event" | "last_processed_event" => {
            println!("{}", events::application_schema::LAST_PROCESSED_EVENT_SQL);
            ExitCode::SUCCESS
        }
        "workflow-failures" | "workflow_failures" => {
            println!("{}", events::application_schema::WORKFLOW_FAILURES_SQL);
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!("unknown app schema table: {table}\n");
            print_help();
            ExitCode::from(2)
        }
    }
}

fn print_help() {
    println!(
        "events_dev_cli\n\nUSAGE:\n    events_dev_cli schema app\n    events_dev_cli schema app --table last-processed-event\n    events_dev_cli schema app --table workflow-failures\n\nCOMMANDS:\n    schema app    Print example application-owned SQL for event handlers"
    );
}
