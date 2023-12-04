import libs.features as ft

if __name__ == '__main__':

    data = ft.scan_reports(
        directory='reports'
    )

    string_columns = [
        'Issue Key', 'Issue Status', 'Issue Summary',
        'Issue Priority', 'Issue Evaluation',
        'Issue Assignee', 'Issue Created',
        'Issue Updated', 'Issue Resolved'
    ]
    data[string_columns] = data[string_columns].astype(str)

    for index, row in data.iterrows():
        if ft.check_report(row=row):
            issue = ft.create_ticket(row=row, project_key='RA')

            row["Issue Key"] = str(issue.key)
            row["Issue Status"] = issue.fields.status.name
            row["Issue Created"] = issue.fields.created
            row["Issue Updated"] = str(issue.fields.updated)
            row["Issue Summary"] = issue.fields.summary
            row["Issue Priority"] = issue.fields.priority.name
            row["Issue Resolved"] = str(issue.fields.resolutiondate)
            row["Issue Due Date"] = issue.fields.duedate
            row["Issue Assignee"] = getattr(
                issue.fields.assignee, "displayName", None)
            if issue.fields.status.name == 'Done':
                row["Issue Evaluation"] = getattr(
                    issue.fields, "customfield_10200", None
                ).value
                row["Issue Review Note"] = getattr(
                    issue.fields, "customfield_10201", None
                ).value

            ft.save_report(
                table_name='reports',
                row=row
            )

    tickets = ft.scan_ticket()

    # ft.update_report()
